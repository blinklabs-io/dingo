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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// Beacon represents a Cardano chain position at a specific epoch
// and immutable file number.
type Beacon struct {
	Epoch               uint64 `json:"epoch"`
	ImmutableFileNumber uint64 `json:"immutable_file_number"`
}

// SnapshotBase contains the fields shared by both the list and
// detail snapshot responses from the Mithril aggregator.
type SnapshotBase struct {
	Digest               string   `json:"digest"`
	Network              string   `json:"network"`
	Beacon               Beacon   `json:"beacon"`
	CertificateHash      string   `json:"certificate_hash"`
	Size                 int64    `json:"size"`
	AncillarySize        int64    `json:"ancillary_size"`
	CreatedAt            string   `json:"created_at"`
	Locations            []string `json:"locations"`
	AncillaryLocations   []string `json:"ancillary_locations"`
	CompressionAlgorithm string   `json:"compression_algorithm"`
	CardanoNodeVersion   string   `json:"cardano_node_version"`
}

// CreatedAtTime parses the CreatedAt string into a time.Time
// value.
func (s *SnapshotBase) CreatedAtTime() (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s.CreatedAt)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"parsing SnapshotBase.CreatedAt: %w",
			err,
		)
	}
	return t, nil
}

// SnapshotListItem represents a snapshot entry returned by the
// aggregator's list endpoint (GET /artifact/snapshots).
type SnapshotListItem struct {
	SnapshotBase
}

// ProtocolParameters represents the Mithril protocol parameters
// used during signing.
type ProtocolParameters struct {
	K    uint64  `json:"k"`
	M    uint64  `json:"m"`
	PhiF float64 `json:"phi_f"`
}

// StakeDistributionParty represents a signer in the certificate
// metadata with their party ID and stake.
type StakeDistributionParty struct {
	PartyID string `json:"party_id"`
	Stake   uint64 `json:"stake"`
}

// CertificateMetadata holds the metadata section of a certificate.
type CertificateMetadata struct {
	Network     string                   `json:"network"`
	Version     string                   `json:"version"`
	Parameters  ProtocolParameters       `json:"parameters"`
	InitiatedAt string                   `json:"initiated_at"`
	SealedAt    string                   `json:"sealed_at"`
	Signers     []StakeDistributionParty `json:"signers"`
}

// ProtocolMessage represents the protocol message included in a
// certificate.
type ProtocolMessage struct {
	MessageParts map[string]string `json:"message_parts"`
}

// SignedEntityType represents the type and parameters of the signed
// entity in a certificate. The JSON representation uses a tagged
// union where the key is the entity type name.
type SignedEntityType struct {
	raw json.RawMessage
}

// UnmarshalJSON implements json.Unmarshaler for SignedEntityType.
func (s *SignedEntityType) UnmarshalJSON(data []byte) error {
	s.raw = make(json.RawMessage, len(data))
	copy(s.raw, data)
	return nil
}

// MarshalJSON implements json.Marshaler for SignedEntityType.
func (s SignedEntityType) MarshalJSON() ([]byte, error) {
	if s.raw == nil {
		return []byte("null"), nil
	}
	return s.raw, nil
}

// Raw returns the raw JSON of the signed entity type.
func (s *SignedEntityType) Raw() json.RawMessage {
	return s.raw
}

// CardanoImmutableFilesFull attempts to parse the signed entity
// as a CardanoImmutableFilesFull beacon. Returns nil if the entity
// type does not match.
func (s *SignedEntityType) CardanoImmutableFilesFull() *Beacon {
	if s.raw == nil {
		return nil
	}
	var parsed map[string]Beacon
	if err := json.Unmarshal(s.raw, &parsed); err != nil {
		return nil
	}
	if b, ok := parsed["CardanoImmutableFilesFull"]; ok {
		return &b
	}
	return nil
}

// Certificate represents a Mithril certificate as returned by the
// aggregator's certificate endpoint (GET /certificate/{hash}).
type Certificate struct {
	Hash                     string              `json:"hash"`
	PreviousHash             string              `json:"previous_hash"`
	Epoch                    uint64              `json:"epoch"`
	SignedEntityType         SignedEntityType    `json:"signed_entity_type"`
	Metadata                 CertificateMetadata `json:"metadata"`
	ProtocolMessage          ProtocolMessage     `json:"protocol_message"`
	SignedMessage            string              `json:"signed_message"`
	AggregateVerificationKey string              `json:"aggregate_verification_key"`
	MultiSignature           string              `json:"multi_signature"`
	GenesisSignature         string              `json:"genesis_signature"`
}

// IsGenesis returns true if the certificate was signed with a
// genesis signature rather than a multi-signature.
func (c *Certificate) IsGenesis() bool {
	return c.GenesisSignature != ""
}

// IsChainingToItself returns true if this certificate's hash
// equals its previous hash (i.e., it is the root of the chain).
// Returns false if either hash is empty to avoid treating
// malformed certificates as root.
func (c *Certificate) IsChainingToItself() bool {
	return c.Hash != "" && c.Hash == c.PreviousHash
}

// Default aggregator URLs for each supported Cardano network.
var DefaultAggregatorURLs = map[string]string{
	"mainnet": "https://aggregator.release-mainnet.api.mithril.network/aggregator",
	"preprod": "https://aggregator.release-preprod.api.mithril.network/aggregator",
	"preview": "https://aggregator.pre-release-preview.api.mithril.network/aggregator",
}

// AggregatorURLForNetwork returns the default aggregator URL for the
// given network name, or an error if the network is not recognized.
func AggregatorURLForNetwork(network string) (string, error) {
	aggregatorURL, ok := DefaultAggregatorURLs[network]
	if !ok {
		return "", fmt.Errorf(
			"no default Mithril aggregator URL for network %q",
			network,
		)
	}
	return aggregatorURL, nil
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
		if hc != nil {
			c.httpClient = hc
		}
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
) (*SnapshotListItem, error) {
	reqURL := c.aggregatorURL + "/artifact/snapshot/" +
		url.PathEscape(digest)
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"getting snapshot %s: %w",
			digest,
			err,
		)
	}
	defer body.Close()

	var snapshot SnapshotListItem
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
// aggregator, sorted by epoch (descending) with immutable file
// number as tie-breaker.
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
	slices.SortFunc(snapshots, func(a, b SnapshotListItem) int {
		if a.Beacon.Epoch != b.Beacon.Epoch {
			return cmp.Compare(b.Beacon.Epoch, a.Beacon.Epoch)
		}
		return cmp.Compare(
			b.Beacon.ImmutableFileNumber,
			a.Beacon.ImmutableFileNumber,
		)
	})
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

	resp, err := c.httpClient.Do( //nolint:gosec // URL is built from trusted aggregatorURL base; HTTPS-only redirect policy prevents downgrade
		req,
	)
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
