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
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
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

// CardanoTransactionsBeacon represents a Cardano chain position at a
// specific epoch and block number, used by the CardanoTransactions
// signed entity type.
type CardanoTransactionsBeacon struct {
	Epoch       uint64 `json:"epoch"`
	BlockNumber uint64 `json:"block_number"`
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

// MithrilStakeDistributionListItem represents a Mithril stake distribution
// artifact entry returned by the aggregator.
type MithrilStakeDistributionListItem struct {
	Hash                 string   `json:"hash"`
	CertificateHash      string   `json:"certificate_hash"`
	CreatedAt            string   `json:"created_at"`
	Locations            []string `json:"locations"`
	CompressionAlgorithm string   `json:"compression_algorithm"`
	Epoch                uint64   `json:"epoch"`
}

// MithrilStakeDistributionParty represents a Mithril signer and its associated
// stake and verification key in the certified stake distribution.
type MithrilStakeDistributionParty struct {
	PartyID         string `json:"party_id"`
	Stake           uint64 `json:"stake"`
	VerificationKey string `json:"verification_key"`
}

// VerificationKeyBytes decodes the signer's verification key from its encoded
// string representation.
func (p *MithrilStakeDistributionParty) VerificationKeyBytes() ([]byte, error) {
	if p == nil || p.VerificationKey == "" {
		return nil, errors.New("verification key is empty")
	}
	ret, ok := decodePrimaryEncodedBytes(p.VerificationKey)
	if !ok {
		return nil, errors.New("could not decode signer verification key")
	}
	return ret, nil
}

// MithrilStakeDistribution represents a downloaded Mithril stake distribution
// artifact.
type MithrilStakeDistribution struct {
	Hash            string                          `json:"hash"`
	CertificateHash string                          `json:"certificate_hash"`
	Epoch           uint64                          `json:"epoch"`
	Signers         []MithrilStakeDistributionParty `json:"signers"`
}

// CardanoStakeDistributionListItem represents a Cardano stake distribution
// artifact entry returned by the aggregator.
type CardanoStakeDistributionListItem struct {
	Hash                 string   `json:"hash"`
	CertificateHash      string   `json:"certificate_hash"`
	CreatedAt            string   `json:"created_at"`
	Locations            []string `json:"locations"`
	CompressionAlgorithm string   `json:"compression_algorithm"`
	Epoch                uint64   `json:"epoch"`
}

// CardanoStakeDistributionParty represents a stake pool and stake value in a
// certified Cardano stake distribution artifact.
type CardanoStakeDistributionParty struct {
	PoolID string `json:"pool_id"`
	Stake  uint64 `json:"stake"`
}

// CardanoStakeDistribution represents a downloaded Cardano stake distribution
// artifact.
type CardanoStakeDistribution struct {
	Hash            string                          `json:"hash"`
	CertificateHash string                          `json:"certificate_hash"`
	Epoch           uint64                          `json:"epoch"`
	Pools           []CardanoStakeDistributionParty `json:"pools"`
}

// ProtocolParameters represents the Mithril protocol parameters
// used during signing.
type ProtocolParameters struct {
	K    uint64  `json:"k"`
	M    uint64  `json:"m"`
	PhiF float64 `json:"phi_f"`
}

// ComputeHash matches the upstream Mithril protocol-parameters hash.
func (p ProtocolParameters) ComputeHash() string {
	hasher := sha256.New()
	hasher.Write(uint64ToBigEndianBytes(p.K))
	hasher.Write(uint64ToBigEndianBytes(p.M))
	phiFixed := uint32(math.Round(p.PhiF * float64(uint64(1)<<24)))
	hasher.Write(uint32ToBigEndianBytes(phiFixed))
	return hex.EncodeToString(hasher.Sum(nil))
}

// StakeDistributionParty represents a signer in the certificate
// metadata with their party ID and stake.
type StakeDistributionParty struct {
	PartyID string `json:"party_id"`
	Stake   uint64 `json:"stake"`
}

// ComputeHash matches the upstream Mithril signer hash used in certificate
// metadata hashing.
func (p StakeDistributionParty) ComputeHash() string {
	hasher := sha256.New()
	hasher.Write([]byte(p.PartyID))
	hasher.Write(uint64ToBigEndianBytes(p.Stake))
	return hex.EncodeToString(hasher.Sum(nil))
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

// ComputeHash matches the upstream Mithril certificate-metadata hash.
func (m CertificateMetadata) ComputeHash() (string, error) {
	hasher := sha256.New()
	hasher.Write([]byte(m.Network))
	hasher.Write([]byte(m.Version))
	hasher.Write([]byte(m.Parameters.ComputeHash()))
	initiatedAt, err := time.Parse(time.RFC3339Nano, m.InitiatedAt)
	if err != nil {
		return "", fmt.Errorf("parsing initiated_at: %w", err)
	}
	sealedAt, err := time.Parse(time.RFC3339Nano, m.SealedAt)
	if err != nil {
		return "", fmt.Errorf("parsing sealed_at: %w", err)
	}
	hasher.Write(int64ToBigEndianBytes(initiatedAt.UnixNano()))
	hasher.Write(int64ToBigEndianBytes(sealedAt.UnixNano()))
	for _, signer := range m.Signers {
		hasher.Write([]byte(signer.ComputeHash()))
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// ProtocolMessage represents the protocol message included in a
// certificate.
type ProtocolMessage struct {
	MessageParts map[string]string `json:"message_parts"`
}

// ComputeHash matches the upstream Mithril protocol-message hash.
func (p ProtocolMessage) ComputeHash() string {
	hasher := sha256.New()
	seenKeys := make(map[string]struct{}, len(p.MessageParts))
	for _, key := range protocolMessageHashOrder {
		value, ok := p.MessageParts[key]
		if !ok {
			continue
		}
		hasher.Write([]byte(key))
		hasher.Write([]byte(value))
		seenKeys[key] = struct{}{}
	}
	remainingKeys := make([]string, 0, len(p.MessageParts)-len(seenKeys))
	for key := range p.MessageParts {
		if _, ok := seenKeys[key]; ok {
			continue
		}
		remainingKeys = append(remainingKeys, key)
	}
	slices.Sort(remainingKeys)
	for _, key := range remainingKeys {
		hasher.Write([]byte(key))
		hasher.Write([]byte(p.MessageParts[key]))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

var protocolMessageHashOrder = []string{
	"snapshot_digest",
	"cardano_transactions_merkle_root",
	"cardano_blocks_transactions_merkle_root",
	"next_aggregate_verification_key",
	"next_protocol_parameters",
	"current_epoch",
	"latest_block_number",
	"cardano_stake_distribution_epoch",
	"cardano_stake_distribution_merkle_root",
	"cardano_database_merkle_root",
}

// SignedEntityType represents the type and parameters of the signed
// entity in a certificate. The JSON representation uses a tagged
// union where the key is the entity type name.
type SignedEntityType struct {
	raw json.RawMessage
}

const (
	signedEntityTypeMithrilStakeDistribution  = "MithrilStakeDistribution"
	signedEntityTypeCardanoStakeDistribution  = "CardanoStakeDistribution"
	signedEntityTypeCardanoImmutableFilesFull = "CardanoImmutableFilesFull"
	signedEntityTypeCardanoTransactions       = "CardanoTransactions"
)

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

// Kind returns the tagged union key for the signed entity type.
func (s *SignedEntityType) Kind() (string, error) {
	if s == nil || s.raw == nil {
		return "", errors.New("signed entity type is nil")
	}
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(s.raw, &parsed); err != nil {
		return "", fmt.Errorf("parsing signed entity type: %w", err)
	}
	if len(parsed) != 1 {
		return "", fmt.Errorf(
			"signed entity type must contain exactly one key, got %d",
			len(parsed),
		)
	}
	var key string
	for k := range parsed {
		key = k
	}
	return key, nil
}

func (s *SignedEntityType) beaconForType(typeName string) *Beacon {
	if s == nil || s.raw == nil {
		return nil
	}
	var parsed map[string]Beacon
	if err := json.Unmarshal(s.raw, &parsed); err != nil {
		return nil
	}
	beacon, ok := parsed[typeName]
	if !ok {
		return nil
	}
	return &beacon
}

func (s *SignedEntityType) epochBeaconForType(typeName string) *Beacon {
	if s == nil || s.raw == nil {
		return nil
	}
	var parsed map[string]uint64
	if err := json.Unmarshal(s.raw, &parsed); err != nil {
		return nil
	}
	epoch, ok := parsed[typeName]
	if !ok {
		return nil
	}
	return &Beacon{Epoch: epoch}
}

func (s *SignedEntityType) feedHash(hasher hash.Hash) error {
	if s == nil {
		return errors.New("signed entity type is nil")
	}
	kind, err := s.Kind()
	if err != nil {
		return err
	}
	switch kind {
	case signedEntityTypeMithrilStakeDistribution:
		beacon := s.MithrilStakeDistribution()
		if beacon == nil {
			return fmt.Errorf("cannot parse beacon for %s", kind)
		}
		hasher.Write(uint64ToBigEndianBytes(beacon.Epoch))
	case signedEntityTypeCardanoStakeDistribution:
		beacon := s.CardanoStakeDistribution()
		if beacon == nil {
			return fmt.Errorf("cannot parse beacon for %s", kind)
		}
		hasher.Write(uint64ToBigEndianBytes(beacon.Epoch))
	case signedEntityTypeCardanoImmutableFilesFull:
		beacon := s.CardanoImmutableFilesFull()
		if beacon == nil {
			return fmt.Errorf("cannot parse beacon for %s", kind)
		}
		hasher.Write(uint64ToBigEndianBytes(beacon.Epoch))
		hasher.Write(uint64ToBigEndianBytes(beacon.ImmutableFileNumber))
	case signedEntityTypeCardanoTransactions:
		beacon := s.CardanoTransactions()
		if beacon == nil {
			return fmt.Errorf("cannot parse beacon for %s", kind)
		}
		hasher.Write(uint64ToBigEndianBytes(beacon.Epoch))
		hasher.Write(uint64ToBigEndianBytes(beacon.BlockNumber))
	default:
		return fmt.Errorf("unsupported signed entity type: %s", kind)
	}
	return nil
}

func uint64ToBigEndianBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func uint32ToBigEndianBytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func int64ToBigEndianBytes(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v)) //nolint:gosec // intentional bit-pattern reinterpretation for serialization
	return b
}

// CardanoImmutableFilesFull attempts to parse the signed entity
// as a CardanoImmutableFilesFull beacon. Returns nil if the entity
// type does not match.
func (s *SignedEntityType) CardanoImmutableFilesFull() *Beacon {
	return s.beaconForType(signedEntityTypeCardanoImmutableFilesFull)
}

// MithrilStakeDistribution attempts to parse the signed entity as a
// MithrilStakeDistribution beacon. Returns nil if the entity type does not
// match.
func (s *SignedEntityType) MithrilStakeDistribution() *Beacon {
	if beacon := s.epochBeaconForType(signedEntityTypeMithrilStakeDistribution); beacon != nil {
		return beacon
	}
	return s.beaconForType(signedEntityTypeMithrilStakeDistribution)
}

// CardanoStakeDistribution attempts to parse the signed entity as a
// CardanoStakeDistribution beacon. Returns nil if the entity type does not
// match.
func (s *SignedEntityType) CardanoStakeDistribution() *Beacon {
	if beacon := s.epochBeaconForType(signedEntityTypeCardanoStakeDistribution); beacon != nil {
		return beacon
	}
	return s.beaconForType(signedEntityTypeCardanoStakeDistribution)
}

// CardanoTransactions attempts to parse the signed entity as a
// CardanoTransactions beacon containing epoch and block_number.
// Returns nil if the entity type does not match.
func (s *SignedEntityType) CardanoTransactions() *CardanoTransactionsBeacon {
	if s == nil || s.raw == nil {
		return nil
	}
	var parsed map[string]CardanoTransactionsBeacon
	if err := json.Unmarshal(s.raw, &parsed); err != nil {
		return nil
	}
	beacon, ok := parsed[signedEntityTypeCardanoTransactions]
	if !ok {
		return nil
	}
	return &beacon
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

// AggregateVerificationKeyBytes decodes the aggregate verification key from
// its encoded string representation.
func (c *Certificate) AggregateVerificationKeyBytes() ([]byte, error) {
	if c == nil || c.AggregateVerificationKey == "" {
		return nil, errors.New("aggregate verification key is empty")
	}
	ret, ok := decodePrimaryEncodedBytes(c.AggregateVerificationKey)
	if !ok {
		return nil, errors.New("could not decode aggregate verification key")
	}
	return ret, nil
}

// MultiSignatureBytes decodes the multi-signature from its encoded string
// representation.
func (c *Certificate) MultiSignatureBytes() ([]byte, error) {
	if c == nil || c.MultiSignature == "" {
		return nil, errors.New("multi-signature is empty")
	}
	ret, ok := decodePrimaryEncodedBytes(c.MultiSignature)
	if !ok {
		return nil, errors.New("could not decode multi-signature")
	}
	return ret, nil
}

// ComputeHash matches the upstream Mithril certificate hash.
func (c *Certificate) ComputeHash() (string, error) {
	if c == nil {
		return "", errors.New("certificate is nil")
	}
	metadataHash, err := c.Metadata.ComputeHash()
	if err != nil {
		return "", fmt.Errorf("computing certificate metadata hash: %w", err)
	}
	hasher := sha256.New()
	hasher.Write([]byte(c.PreviousHash))
	hasher.Write(uint64ToBigEndianBytes(c.Epoch))
	hasher.Write([]byte(metadataHash))
	hasher.Write([]byte(c.ProtocolMessage.ComputeHash()))
	hasher.Write([]byte(c.SignedMessage))
	hasher.Write([]byte(c.AggregateVerificationKey))
	if c.IsGenesis() {
		hasher.Write([]byte(c.GenesisSignature))
	} else {
		if err := c.SignedEntityType.feedHash(hasher); err != nil {
			return "", fmt.Errorf("hashing signed entity type: %w", err)
		}
		hasher.Write([]byte(c.MultiSignature))
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// NetworkConfig describes the Mithril trust endpoints for a specific Cardano
// network.
type NetworkConfig struct {
	AggregatorURL               string
	GenesisVerificationKeyURL   string
	AncillaryVerificationKeyURL string
}

// Default network configuration for each supported Cardano network. The
// verification key URLs follow the official Mithril network configurations.
var defaultNetworkConfigs = map[string]NetworkConfig{
	"mainnet": {
		AggregatorURL:               "https://aggregator.release-mainnet.api.mithril.network/aggregator",
		GenesisVerificationKeyURL:   "https://raw.githubusercontent.com/input-output-hk/mithril/main/mithril-infra/configuration/data/network-config/mainnet/genesis.vkey",
		AncillaryVerificationKeyURL: "https://raw.githubusercontent.com/input-output-hk/mithril/main/mithril-infra/configuration/data/network-config/mainnet/genesis-ancillary.vkey",
	},
	"preprod": {
		AggregatorURL:               "https://aggregator.release-preprod.api.mithril.network/aggregator",
		GenesisVerificationKeyURL:   "https://raw.githubusercontent.com/input-output-hk/mithril/main/mithril-infra/configuration/data/network-config/preprod/genesis.vkey",
		AncillaryVerificationKeyURL: "https://raw.githubusercontent.com/input-output-hk/mithril/main/mithril-infra/configuration/data/network-config/preprod/genesis-ancillary.vkey",
	},
	"preview": {
		AggregatorURL:               "https://aggregator.pre-release-preview.api.mithril.network/aggregator",
		GenesisVerificationKeyURL:   "https://raw.githubusercontent.com/input-output-hk/mithril/main/mithril-infra/configuration/data/network-config/preview/genesis.vkey",
		AncillaryVerificationKeyURL: "https://raw.githubusercontent.com/input-output-hk/mithril/main/mithril-infra/configuration/data/network-config/preview/genesis-ancillary.vkey",
	},
}

// NetworkConfigForNetwork returns the default Mithril network configuration
// for the given network name, or an error if the network is not recognized.
func NetworkConfigForNetwork(network string) (NetworkConfig, error) {
	cfg, ok := defaultNetworkConfigs[network]
	if !ok {
		return NetworkConfig{}, fmt.Errorf(
			"no default Mithril network config for network %q",
			network,
		)
	}
	return cfg, nil
}

// AggregatorURLForNetwork returns the default aggregator URL for the
// given network name, or an error if the network is not recognized.
func AggregatorURLForNetwork(network string) (string, error) {
	cfg, err := NetworkConfigForNetwork(network)
	if err != nil {
		return "", err
	}
	return cfg.AggregatorURL, nil
}

// GenesisVerificationKeyURLForNetwork returns the default Mithril genesis
// verification key URL for the given network.
func GenesisVerificationKeyURLForNetwork(network string) (string, error) {
	cfg, err := NetworkConfigForNetwork(network)
	if err != nil {
		return "", err
	}
	return cfg.GenesisVerificationKeyURL, nil
}

// AncillaryVerificationKeyURLForNetwork returns the default Mithril ancillary
// verification key URL for the given network.
func AncillaryVerificationKeyURLForNetwork(network string) (string, error) {
	cfg, err := NetworkConfigForNetwork(network)
	if err != nil {
		return "", err
	}
	return cfg.AncillaryVerificationKeyURL, nil
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

// ListMithrilStakeDistributions retrieves available Mithril stake
// distributions from the aggregator.
func (c *Client) ListMithrilStakeDistributions(
	ctx context.Context,
) ([]MithrilStakeDistributionListItem, error) {
	reqURL := c.aggregatorURL + "/artifact/mithril-stake-distributions"
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"listing Mithril stake distributions: %w",
			err,
		)
	}
	defer body.Close()

	var ret []MithrilStakeDistributionListItem
	if err := json.NewDecoder(body).Decode(&ret); err != nil {
		return nil, fmt.Errorf(
			"decoding Mithril stake distribution list: %w",
			err,
		)
	}
	return ret, nil
}

// GetMithrilStakeDistribution retrieves a Mithril stake distribution by hash.
func (c *Client) GetMithrilStakeDistribution(
	ctx context.Context,
	hash string,
) (*MithrilStakeDistribution, error) {
	reqURL := c.aggregatorURL + "/artifact/mithril-stake-distribution/" +
		url.PathEscape(hash)
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"getting Mithril stake distribution %s: %w",
			hash,
			err,
		)
	}
	defer body.Close()

	var ret MithrilStakeDistribution
	if err := json.NewDecoder(body).Decode(&ret); err != nil {
		return nil, fmt.Errorf(
			"decoding Mithril stake distribution %s: %w",
			hash,
			err,
		)
	}
	return &ret, nil
}

// ListCardanoStakeDistributions retrieves available Cardano stake
// distributions from the aggregator.
func (c *Client) ListCardanoStakeDistributions(
	ctx context.Context,
) ([]CardanoStakeDistributionListItem, error) {
	reqURL := c.aggregatorURL + "/artifact/cardano-stake-distributions"
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"listing Cardano stake distributions: %w",
			err,
		)
	}
	defer body.Close()

	var ret []CardanoStakeDistributionListItem
	if err := json.NewDecoder(body).Decode(&ret); err != nil {
		return nil, fmt.Errorf(
			"decoding Cardano stake distribution list: %w",
			err,
		)
	}
	return ret, nil
}

// GetCardanoStakeDistribution retrieves a Cardano stake distribution by hash
// or unique identifier.
func (c *Client) GetCardanoStakeDistribution(
	ctx context.Context,
	identifier string,
) (*CardanoStakeDistribution, error) {
	reqURL := c.aggregatorURL + "/artifact/cardano-stake-distribution/" +
		url.PathEscape(identifier)
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"getting Cardano stake distribution %s: %w",
			identifier,
			err,
		)
	}
	defer body.Close()

	var ret CardanoStakeDistribution
	if err := json.NewDecoder(body).Decode(&ret); err != nil {
		return nil, fmt.Errorf(
			"decoding Cardano stake distribution %s: %w",
			identifier,
			err,
		)
	}
	return &ret, nil
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

func decodePrimaryEncodedBytes(data string) ([]byte, bool) {
	if decoded, err := decodeHexString(data); err == nil {
		return decoded, true
	}
	if decoded, err := base64.StdEncoding.DecodeString(data); err == nil {
		return decoded, true
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(data); err == nil {
		return decoded, true
	}
	if decoded, err := base64.URLEncoding.DecodeString(data); err == nil {
		return decoded, true
	}
	if decoded, err := base64.RawURLEncoding.DecodeString(data); err == nil {
		return decoded, true
	}
	return nil, false
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
