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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// Location type discriminants used by v2 artifact location lists.
const (
	locationTypeCloudStorage = "cloud_storage"
	locationTypeAggregator   = "aggregator"
)

// immutableFileNumberTemplate is the placeholder substituted with the
// zero-padded immutable file number in templated archive URIs.
const immutableFileNumberTemplate = "{immutable_file_number}"

const immutableFileNumberDigits = 5

// CardanoDatabaseLocation is one download location for a component of
// a Cardano database (v2) artifact. The JSON representation is
// internally tagged by "type"; the uri field is either a plain string
// or, for immutable archives, a {"Template": "..."} object whose
// template contains the {immutable_file_number} placeholder. Unknown
// location types and uri shapes are tolerated so new aggregator
// location kinds do not break parsing; such locations are simply
// skipped during download.
type CardanoDatabaseLocation struct {
	Type                 string
	URI                  string
	URITemplate          string
	CompressionAlgorithm string
}

// UnmarshalJSON implements json.Unmarshaler for
// CardanoDatabaseLocation.
func (l *CardanoDatabaseLocation) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type                 string          `json:"type"`
		URI                  json.RawMessage `json:"uri"`
		CompressionAlgorithm string          `json:"compression_algorithm"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("parsing Cardano database location: %w", err)
	}
	l.Type = raw.Type
	l.URI = ""
	l.URITemplate = ""
	l.CompressionAlgorithm = raw.CompressionAlgorithm
	if len(raw.URI) > 0 {
		var plain string
		if err := json.Unmarshal(raw.URI, &plain); err == nil {
			l.URI = plain
		} else {
			var templated struct {
				Template string `json:"Template"`
			}
			if err := json.Unmarshal(raw.URI, &templated); err == nil {
				l.URITemplate = templated.Template
			}
			// Unknown uri shapes leave both fields empty
		}
	}
	return nil
}

// MarshalJSON implements json.Marshaler for CardanoDatabaseLocation,
// producing the aggregator wire format (uri as a plain string, or as
// a {"Template": ...} object for templated locations).
func (l CardanoDatabaseLocation) MarshalJSON() ([]byte, error) {
	out := map[string]any{"type": l.Type}
	switch {
	case l.URITemplate != "":
		out["uri"] = map[string]string{"Template": l.URITemplate}
	case l.URI != "":
		out["uri"] = l.URI
	}
	if l.CompressionAlgorithm != "" {
		out["compression_algorithm"] = l.CompressionAlgorithm
	}
	return json.Marshal(out)
}

// ImmutableArchiveURI resolves the location's URI template for the
// given immutable file number (zero-padded to 5 digits). Returns an
// empty string if the location has no URI template.
func (l *CardanoDatabaseLocation) ImmutableArchiveURI(num uint64) string {
	if l.URITemplate == "" {
		return ""
	}
	return strings.ReplaceAll(
		l.URITemplate,
		immutableFileNumberTemplate,
		fmt.Sprintf("%05d", num),
	)
}

// CardanoDatabaseDigests describes the digest-list component of a v2
// artifact.
type CardanoDatabaseDigests struct {
	SizeUncompressed int64                     `json:"size_uncompressed"`
	Locations        []CardanoDatabaseLocation `json:"locations"`
}

// CardanoDatabaseImmutables describes the immutable-archives component
// of a v2 artifact.
type CardanoDatabaseImmutables struct {
	AverageSizeUncompressed int64                     `json:"average_size_uncompressed"`
	Locations               []CardanoDatabaseLocation `json:"locations"`
}

// CardanoDatabaseAncillary describes the ancillary component (ledger
// state plus the next in-progress immutable trio) of a v2 artifact.
type CardanoDatabaseAncillary struct {
	SizeUncompressed int64                     `json:"size_uncompressed"`
	Locations        []CardanoDatabaseLocation `json:"locations"`
}

// CardanoDatabaseSnapshotListItem represents one entry returned by the
// aggregator's v2 artifact list endpoint
// (GET /artifact/cardano-database).
type CardanoDatabaseSnapshotListItem struct {
	Hash                    string `json:"hash"`
	MerkleRoot              string `json:"merkle_root"`
	Beacon                  Beacon `json:"beacon"`
	CertificateHash         string `json:"certificate_hash"`
	TotalDbSizeUncompressed int64  `json:"total_db_size_uncompressed"`
	CardanoNodeVersion      string `json:"cardano_node_version"`
	CreatedAt               string `json:"created_at"`
}

// CardanoDatabaseSnapshot represents the detail response for a v2
// artifact (GET /artifact/cardano-database/{hash}).
type CardanoDatabaseSnapshot struct {
	Hash                    string                    `json:"hash"`
	MerkleRoot              string                    `json:"merkle_root"`
	Network                 string                    `json:"network"`
	Beacon                  Beacon                    `json:"beacon"`
	CertificateHash         string                    `json:"certificate_hash"`
	TotalDbSizeUncompressed int64                     `json:"total_db_size_uncompressed"`
	Digests                 CardanoDatabaseDigests    `json:"digests"`
	Immutables              CardanoDatabaseImmutables `json:"immutables"`
	Ancillary               CardanoDatabaseAncillary  `json:"ancillary"`
	CardanoNodeVersion      string                    `json:"cardano_node_version"`
	CreatedAt               string                    `json:"created_at"`
}

// ComputeHash matches the upstream Mithril Cardano database artifact
// hash: hex(sha256(beacon.epoch as 8-byte big-endian || merkle_root
// ASCII bytes)).
func (s *CardanoDatabaseSnapshot) ComputeHash() string {
	hasher := sha256.New()
	hasher.Write(uint64ToBigEndianBytes(s.Beacon.Epoch))
	hasher.Write([]byte(s.MerkleRoot))
	return hex.EncodeToString(hasher.Sum(nil))
}

// CardanoDatabaseDigestEntry is one immutable-file digest from the v2
// digest list.
type CardanoDatabaseDigestEntry struct {
	ImmutableFileName string `json:"immutable_file_name"`
	Digest            string `json:"digest"`
}

// immutableFileNumberFromName parses the leading zero-padded number
// from immutable file names like "05810.chunk". Returns false for
// names that do not follow the canonical width used by Mithril:
// numbers below 100000 must be padded to exactly five digits, while
// larger numbers use their natural decimal width.
func immutableFileNumberFromName(name string) (uint64, bool) {
	base, ext, found := strings.Cut(name, ".")
	if !found || ext == "" || len(base) < immutableFileNumberDigits {
		return 0, false
	}
	if len(base) > immutableFileNumberDigits && base[0] == '0' {
		return 0, false
	}
	for _, c := range base {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	num, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return num, true
}

// digestMerkleLeaves filters the digest list to entries at or below
// maxImmutable, sorts by immutable file name (the order Mithril
// builds its merkle tree in), and returns the digest strings as leaf
// byte slices. Entries with unparseable names are rejected.
func digestMerkleLeaves(
	entries []CardanoDatabaseDigestEntry,
	maxImmutable uint64,
) ([][]byte, error) {
	filtered := make([]CardanoDatabaseDigestEntry, 0, len(entries))
	for _, entry := range entries {
		num, ok := immutableFileNumberFromName(entry.ImmutableFileName)
		if !ok {
			return nil, fmt.Errorf(
				"invalid immutable file name in digest list: %q",
				entry.ImmutableFileName,
			)
		}
		if num > maxImmutable {
			continue
		}
		filtered = append(filtered, entry)
	}
	slices.SortFunc(
		filtered,
		func(a, b CardanoDatabaseDigestEntry) int {
			return cmp.Compare(a.ImmutableFileName, b.ImmutableFileName)
		},
	)
	leaves := make([][]byte, 0, len(filtered))
	for _, entry := range filtered {
		leaves = append(leaves, []byte(entry.Digest))
	}
	return leaves, nil
}

// ListCardanoDatabaseSnapshots retrieves the list of available v2
// Cardano database snapshots from the aggregator. Corresponds to
// GET /artifact/cardano-database.
func (c *Client) ListCardanoDatabaseSnapshots(
	ctx context.Context,
) ([]CardanoDatabaseSnapshotListItem, error) {
	reqURL := c.aggregatorURL + "/artifact/cardano-database"
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"listing Cardano database snapshots: %w",
			err,
		)
	}
	defer body.Close()

	var items []CardanoDatabaseSnapshotListItem
	if err := json.NewDecoder(body).Decode(&items); err != nil {
		return nil, fmt.Errorf(
			"decoding Cardano database snapshot list: %w",
			err,
		)
	}
	return items, nil
}

// GetCardanoDatabaseSnapshot retrieves the details of a specific v2
// Cardano database snapshot by its hash. Corresponds to
// GET /artifact/cardano-database/{hash}.
func (c *Client) GetCardanoDatabaseSnapshot(
	ctx context.Context,
	hash string,
) (*CardanoDatabaseSnapshot, error) {
	reqURL := c.aggregatorURL + "/artifact/cardano-database/" +
		url.PathEscape(hash)
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"getting Cardano database snapshot %s: %w",
			hash,
			err,
		)
	}
	defer body.Close()

	var snapshot CardanoDatabaseSnapshot
	if err := json.NewDecoder(body).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf(
			"decoding Cardano database snapshot %s: %w",
			hash,
			err,
		)
	}
	return &snapshot, nil
}

// GetCardanoDatabaseDigests retrieves the immutable-file digest list
// for the latest v2 snapshot directly from the aggregator.
// Corresponds to GET /artifact/cardano-database/digests.
func (c *Client) GetCardanoDatabaseDigests(
	ctx context.Context,
) ([]CardanoDatabaseDigestEntry, error) {
	reqURL := c.aggregatorURL + "/artifact/cardano-database/digests"
	body, err := c.doGet(ctx, reqURL)
	if err != nil {
		return nil, fmt.Errorf(
			"getting Cardano database digests: %w",
			err,
		)
	}
	defer body.Close()

	var entries []CardanoDatabaseDigestEntry
	if err := json.NewDecoder(body).Decode(&entries); err != nil {
		return nil, fmt.Errorf(
			"decoding Cardano database digests: %w",
			err,
		)
	}
	return entries, nil
}

// GetLatestCardanoDatabaseSnapshot returns the most recent v2 Cardano
// database snapshot from the aggregator, sorted by epoch (descending)
// with immutable file number as tie-breaker.
func (c *Client) GetLatestCardanoDatabaseSnapshot(
	ctx context.Context,
) (*CardanoDatabaseSnapshot, error) {
	items, err := c.ListCardanoDatabaseSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, errors.New(
			"no Cardano database snapshots available from aggregator",
		)
	}
	slices.SortFunc(
		items,
		func(a, b CardanoDatabaseSnapshotListItem) int {
			if a.Beacon.Epoch != b.Beacon.Epoch {
				return cmp.Compare(b.Beacon.Epoch, a.Beacon.Epoch)
			}
			return cmp.Compare(
				b.Beacon.ImmutableFileNumber,
				a.Beacon.ImmutableFileNumber,
			)
		},
	)
	return c.GetCardanoDatabaseSnapshot(ctx, items[0].Hash)
}
