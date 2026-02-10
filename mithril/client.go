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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Default aggregator URLs for each supported Cardano network.
var DefaultAggregatorURLs = map[string]string{
	"mainnet": "https://aggregator.release-mainnet.api.mithril.network/aggregator",
	"preprod": "https://aggregator.release-preprod.api.mithril.network/aggregator",
	"preview": "https://aggregator.pre-release-preview.api.mithril.network/aggregator",
}

// AggregatorURLForNetwork returns the default aggregator URL for the
// given network name, or an error if the network is not recognized.
func AggregatorURLForNetwork(network string) (string, error) {
	url, ok := DefaultAggregatorURLs[network]
	if !ok {
		return "", fmt.Errorf(
			"no default Mithril aggregator URL for network %q",
			network,
		)
	}
	return url, nil
}

// Client is an HTTP client for the Mithril aggregator REST API.
type Client struct {
	aggregatorURL string
	httpClient    *http.Client
}

// ClientOption is a functional option for configuring a Client.
type ClientOption func(*Client)

// WithHTTPClient sets a custom *http.Client for the Mithril client.
// Note: the default client enforces HTTPS-only redirects via
// httpsOnlyRedirect. A custom client bypasses this protection,
// so callers should configure their own redirect policy if needed.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = hc
	}
}

// NewClient creates a new Mithril aggregator API client.
// The aggregatorURL should be the base URL of the aggregator
// (e.g., "https://aggregator.release-preprod.api.mithril.network/aggregator").
func NewClient(
	aggregatorURL string,
	opts ...ClientOption,
) *Client {
	c := &Client{
		aggregatorURL: strings.TrimRight(aggregatorURL, "/"),
		httpClient: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: httpsOnlyRedirect,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// httpsOnlyRedirect rejects redirects to non-HTTPS URLs to prevent
// downgrade attacks and SSRF.
func httpsOnlyRedirect(
	req *http.Request,
	via []*http.Request,
) error {
	if len(via) >= 10 {
		return errors.New("too many redirects")
	}
	if req.URL.Scheme != "https" {
		return fmt.Errorf(
			"redirect to non-HTTPS URL blocked: %s",
			req.URL,
		)
	}
	return nil
}

// ListSnapshots retrieves the list of available snapshots from the
// aggregator. Corresponds to GET /artifact/snapshots.
func (c *Client) ListSnapshots(
	ctx context.Context,
) ([]SnapshotListItem, error) {
	reqURL := c.aggregatorURL + "/artifact/snapshots"
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"listing snapshots: %w",
			err,
		)
	}
	defer body.Close()

	var snapshots []SnapshotListItem
	if err := json.NewDecoder(body).Decode(&snapshots); err != nil {
		return nil, fmt.Errorf(
			"decoding snapshot list: %w",
			err,
		)
	}
	return snapshots, nil
}

// GetSnapshot retrieves the details of a specific snapshot by its
// digest. Corresponds to GET /artifact/snapshot/{digest}.
func (c *Client) GetSnapshot(
	ctx context.Context,
	digest string,
) (*Snapshot, error) {
	reqURL := c.aggregatorURL + "/artifact/snapshot/" + url.PathEscape(digest)
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"getting snapshot %s: %w",
			digest,
			err,
		)
	}
	defer body.Close()

	var snapshot Snapshot
	if err := json.NewDecoder(body).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf(
			"decoding snapshot %s: %w",
			digest,
			err,
		)
	}
	return &snapshot, nil
}

// GetCertificate retrieves a certificate by its hash.
// Corresponds to GET /certificate/{hash}.
func (c *Client) GetCertificate(
	ctx context.Context,
	hash string,
) (*Certificate, error) {
	reqURL := c.aggregatorURL + "/certificate/" + url.PathEscape(hash)
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"getting certificate %s: %w",
			hash,
			err,
		)
	}
	defer body.Close()

	var cert Certificate
	if err := json.NewDecoder(body).Decode(&cert); err != nil {
		return nil, fmt.Errorf(
			"decoding certificate %s: %w",
			hash,
			err,
		)
	}
	return &cert, nil
}

// GetLatestSnapshot returns the most recent snapshot from the
// aggregator (the first item in the list).
func (c *Client) GetLatestSnapshot(
	ctx context.Context,
) (*SnapshotListItem, error) {
	snapshots, err := c.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, errors.New("no snapshots available from aggregator")
	}
	return &snapshots[0], nil
}

// doGet performs an HTTP GET request and returns the response body.
// The caller is responsible for closing the returned ReadCloser.
func (c *Client) doGet(
	ctx context.Context,
	reqURL string,
) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		reqURL,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	if resp == nil || resp.Body == nil {
		return nil, errors.New("nil response from server")
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		bodyBytes, _ := io.ReadAll(
			io.LimitReader(resp.Body, 1024),
		)
		return nil, fmt.Errorf(
			"unexpected status %d: %s",
			resp.StatusCode,
			string(bodyBytes),
		)
	}

	return &limitedReadCloser{
		Reader: io.LimitReader(resp.Body, maxResponseBytes),
		Closer: resp.Body,
	}, nil
}

// maxResponseBytes limits JSON API responses to 10 MiB to prevent
// OOM from a malicious or misconfigured aggregator.
const maxResponseBytes = 10 << 20

// limitedReadCloser wraps a size-limited Reader with the
// underlying connection's Closer.
type limitedReadCloser struct {
	io.Reader
	io.Closer
}
