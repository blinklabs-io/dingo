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
	"encoding/json"
	"time"
)

// Beacon represents a Cardano chain position at a specific epoch
// and immutable file number.
type Beacon struct {
	Epoch               uint64 `json:"epoch"`
	ImmutableFileNumber uint64 `json:"immutable_file_number"`
}

// SnapshotListItem represents a snapshot entry returned by the
// aggregator's list endpoint (GET /artifact/snapshots).
type SnapshotListItem struct {
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

// CreatedAtTime parses the CreatedAt string into a time.Time value.
func (s *SnapshotListItem) CreatedAtTime() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s.CreatedAt)
}

// Snapshot represents the full details of a snapshot returned by the
// aggregator's single snapshot endpoint
// (GET /artifact/snapshot/{digest}).
type Snapshot struct {
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

// CreatedAtTime parses the CreatedAt string into a time.Time value.
func (s *Snapshot) CreatedAtTime() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s.CreatedAt)
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
