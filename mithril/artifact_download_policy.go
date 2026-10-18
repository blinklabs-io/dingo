// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mithril

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const (
	maxArtifactLocations         = 4
	maxCardanoDatabaseArchives   = 100_000
	artifactRequestsPerObject    = 4
	artifactDownloadTotalTimeout = 24 * time.Hour
	officialMithrilStorageOrigin = "https://storage.googleapis.com"
)

var errArtifactDownloadBudget = errors.New("mithril artifact download budget exceeded")

type artifactDownloadBudget struct {
	maxRequests int64
	maxBytes    int64
	requests    atomic.Int64
	bytes       atomic.Int64
}

func newArtifactDownloadBudget(
	objects uint64,
	immutableBytes int64,
	digestBytes int64,
	ancillaryBytes int64,
) (*artifactDownloadBudget, error) {
	if objects > math.MaxInt64/artifactRequestsPerObject {
		return nil, errArtifactDownloadBudget
	}
	maxBytes, ok := addWithinInt64(immutableBytes, digestBytes)
	if !ok {
		return nil, errArtifactDownloadBudget
	}
	maxBytes, ok = addWithinInt64(maxBytes, ancillaryBytes)
	if !ok || maxBytes <= 0 {
		return nil, errArtifactDownloadBudget
	}
	return &artifactDownloadBudget{
		maxRequests: int64(objects) * artifactRequestsPerObject,
		maxBytes:    maxBytes,
	}, nil
}

func addWithinInt64(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

func multiplyWithinInt64(a int64, b uint64) (int64, bool) {
	if a < 0 || b > math.MaxInt64 || (a > 0 && int64(b) > math.MaxInt64/a) {
		return 0, false
	}
	return a * int64(b), true
}

func (b *artifactDownloadBudget) takeRequest() error {
	if b == nil {
		return nil
	}
	if b.requests.Add(1) > b.maxRequests {
		return fmt.Errorf("%w: request limit reached", errArtifactDownloadBudget)
	}
	return nil
}

func (b *artifactDownloadBudget) wrap(body io.ReadCloser) io.ReadCloser {
	if b == nil {
		return body
	}
	return &budgetedReadCloser{ReadCloser: body, budget: b}
}

type budgetedReadCloser struct {
	io.ReadCloser
	budget *artifactDownloadBudget
}

func (r *budgetedReadCloser) Read(p []byte) (int, error) {
	var reserved int64
	for {
		used := r.budget.bytes.Load()
		remaining := r.budget.maxBytes - used
		if remaining <= 0 {
			return 0, fmt.Errorf("%w: byte limit reached", errArtifactDownloadBudget)
		}
		reserved = min(int64(len(p)), remaining)
		if r.budget.bytes.CompareAndSwap(used, used+reserved) {
			break
		}
	}
	n, err := r.ReadCloser.Read(p[:reserved])
	r.budget.bytes.Add(int64(n) - reserved)
	return n, err
}

type artifactLocationPolicy struct {
	aggregatorOrigin string
	allowedOrigins   map[string]struct{}
	allowInsecure    bool
}

func newArtifactLocationPolicy(
	aggregatorURL string,
	allowInsecure bool,
) (*artifactLocationPolicy, error) {
	origin, err := normalizedOrigin(aggregatorURL)
	if err != nil {
		return nil, fmt.Errorf("parsing Mithril aggregator origin: %w", err)
	}
	return &artifactLocationPolicy{
		aggregatorOrigin: origin,
		allowedOrigins: map[string]struct{}{
			origin:                       {},
			officialMithrilStorageOrigin: {},
		},
		allowInsecure: allowInsecure,
	}, nil
}

func normalizedOrigin(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Hostname() == "" {
		return "", errors.New("URL must include a host")
	}
	host := strings.ToLower(parsed.Hostname())
	port := parsed.Port()
	if port != "" &&
		(parsed.Scheme != "https" || port != "443") &&
		(parsed.Scheme != "http" || port != "80") {
		host += ":" + port
	}
	return strings.ToLower(parsed.Scheme) + "://" + host, nil
}

func canonicalLocationKey(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	origin, err := normalizedOrigin(rawURL)
	if err != nil {
		return rawURL
	}
	key := origin + parsed.EscapedPath()
	if parsed.ForceQuery || parsed.RawQuery != "" {
		key += "?" + parsed.RawQuery
	}
	return key
}

func (p *artifactLocationPolicy) admit(rawURL, locationType string) bool {
	if requireSecureURL(rawURL, "Mithril artifact location", p.allowInsecure) != nil {
		return false
	}
	origin, err := normalizedOrigin(rawURL)
	if err != nil {
		return false
	}
	if locationType == locationTypeAggregator {
		return origin == p.aggregatorOrigin
	}
	if locationType != locationTypeCloudStorage {
		return false
	}
	_, ok := p.allowedOrigins[origin]
	return ok
}

func (p *artifactLocationPolicy) checkRedirect(
	req *http.Request,
	_ []*http.Request,
) error {
	if !p.admit(req.URL.String(), locationTypeCloudStorage) {
		return errors.New("mithril artifact redirect origin is not allowed")
	}
	return nil
}

func (p *artifactLocationPolicy) filterLocations(
	locations []CardanoDatabaseLocation,
	templated bool,
) []CardanoDatabaseLocation {
	ret := make([]CardanoDatabaseLocation, 0, min(len(locations), maxArtifactLocations))
	seen := make(map[string]struct{}, len(locations))
	for _, location := range locations {
		rawURL := location.URI
		if templated {
			if strings.Count(location.URITemplate, immutableFileNumberTemplate) != 1 {
				continue
			}
			rawURL = location.ImmutableArchiveURI(0)
		}
		if rawURL == "" || !p.admit(rawURL, location.Type) {
			continue
		}
		key := location.Type + "\x00" + canonicalLocationKey(rawURL)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ret = append(ret, location)
		if len(ret) == maxArtifactLocations {
			break
		}
	}
	return ret
}

func (p *artifactLocationPolicy) apply(artifact *CardanoDatabaseSnapshot) error {
	artifact.Digests.Locations = p.filterLocations(artifact.Digests.Locations, false)
	artifact.Immutables.Locations = p.filterLocations(artifact.Immutables.Locations, true)
	artifact.Ancillary.Locations = p.filterLocations(artifact.Ancillary.Locations, false)
	if len(artifact.Digests.Locations) == 0 {
		return errors.New("no approved digest locations in Cardano database snapshot")
	}
	if len(artifact.Immutables.Locations) == 0 {
		return errors.New("no approved immutable locations in Cardano database snapshot")
	}
	return nil
}
