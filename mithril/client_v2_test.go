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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCardanoDatabaseListFixtureParsing(t *testing.T) {
	data, err := os.ReadFile(
		filepath.Join("testdata", "v2", "cardano_database_list.json"),
	)
	require.NoError(t, err)
	var items []CardanoDatabaseSnapshotListItem
	require.NoError(t, json.Unmarshal(data, &items))
	require.NotEmpty(t, items)
	first := items[0]
	assert.Equal(
		t,
		"408eeb46aa7fd228c99172d9c1c822cc8de75dfe6e841e2c77e59596edcac712",
		first.Hash,
	)
	assert.Equal(
		t,
		"260f03a98322e0e1f84ca6f831e82096ba25be3d4398e16b81ca2a7e95d3dbbb",
		first.MerkleRoot,
	)
	assert.Equal(t, uint64(294), first.Beacon.Epoch)
	assert.Equal(t, uint64(5810), first.Beacon.ImmutableFileNumber)
	assert.NotEmpty(t, first.CertificateHash)
	assert.Positive(t, first.TotalDbSizeUncompressed)
}

func TestCardanoDatabaseDetailFixtureParsing(t *testing.T) {
	snapshot := loadV2DetailFixture(t)
	assert.Equal(t, "preprod", snapshot.Network)
	assert.Equal(t, uint64(294), snapshot.Beacon.Epoch)

	// Digests: cloud_storage + aggregator locations
	require.Len(t, snapshot.Digests.Locations, 2)
	cloud := snapshot.Digests.Locations[0]
	assert.Equal(t, locationTypeCloudStorage, cloud.Type)
	assert.Contains(t, cloud.URI, "digests.tar.zst")
	assert.Equal(t, "zstandard", cloud.CompressionAlgorithm)
	agg := snapshot.Digests.Locations[1]
	assert.Equal(t, locationTypeAggregator, agg.Type)
	assert.Contains(t, agg.URI, "/artifact/cardano-database/digests")

	// Immutables: templated URI
	require.NotEmpty(t, snapshot.Immutables.Locations)
	imm := snapshot.Immutables.Locations[0]
	assert.Equal(t, locationTypeCloudStorage, imm.Type)
	assert.Empty(t, imm.URI)
	assert.Contains(t, imm.URITemplate, "{immutable_file_number}")
	resolved := imm.ImmutableArchiveURI(5810)
	assert.True(t, strings.HasSuffix(resolved, "/05810.tar.zst"), resolved)
	resolved = imm.ImmutableArchiveURI(100000)
	assert.True(t, strings.HasSuffix(resolved, "/100000.tar.zst"), resolved)

	// Ancillary: plain URI
	require.NotEmpty(t, snapshot.Ancillary.Locations)
	assert.Contains(t, snapshot.Ancillary.Locations[0].URI, "ancillary.tar.zst")
	assert.Positive(t, snapshot.Ancillary.SizeUncompressed)
}

func TestCardanoDatabaseLocationUnknownType(t *testing.T) {
	var loc CardanoDatabaseLocation
	err := json.Unmarshal(
		[]byte(`{"type":"weird_new_thing","details":{"a":1}}`),
		&loc,
	)
	require.NoError(t, err)
	assert.Equal(t, "weird_new_thing", loc.Type)
	assert.Empty(t, loc.URI)
	assert.Empty(t, loc.URITemplate)

	// Unknown uri shapes are tolerated too
	err = json.Unmarshal(
		[]byte(`{"type":"cloud_storage","uri":{"Strange":"x"}}`),
		&loc,
	)
	require.NoError(t, err)
	assert.Empty(t, loc.URI)
	assert.Empty(t, loc.URITemplate)
}

func TestCardanoDatabaseSnapshotComputeHash(t *testing.T) {
	snapshot := loadV2DetailFixture(t)
	assert.Equal(t, snapshot.Hash, snapshot.ComputeHash())
}

// TestCertificateCardanoDatabaseFixture verifies dingo's certificate
// hashing against the real preprod certificate that signs the v2
// fixture artifact.
func TestCertificateCardanoDatabaseFixture(t *testing.T) {
	data, err := os.ReadFile(
		filepath.Join("testdata", "v2", "cardano_database_certificate.json"),
	)
	require.NoError(t, err)
	var cert Certificate
	require.NoError(t, json.Unmarshal(data, &cert))

	assert.Equal(t, cert.SignedMessage, cert.ProtocolMessage.ComputeHash())

	computed, err := cert.ComputeHash()
	require.NoError(t, err)
	assert.Equal(t, cert.Hash, computed)

	kind, err := cert.SignedEntityType.Kind()
	require.NoError(t, err)
	assert.Equal(t, "CardanoDatabase", kind)

	beacon := cert.SignedEntityType.CardanoDatabase()
	require.NotNil(t, beacon)
	assert.Equal(t, uint64(294), beacon.Epoch)
	assert.Equal(t, uint64(5810), beacon.ImmutableFileNumber)

	snapshot := loadV2DetailFixture(t)
	assert.Equal(
		t,
		snapshot.MerkleRoot,
		cert.ProtocolMessage.MessageParts["cardano_database_merkle_root"],
	)
}

func TestGetLatestCardanoDatabaseSnapshot(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc(
		"/artifact/cardano-database",
		func(w http.ResponseWriter, _ *http.Request) {
			items := []CardanoDatabaseSnapshotListItem{
				{
					Hash:   "older",
					Beacon: Beacon{Epoch: 293, ImmutableFileNumber: 5790},
				},
				{
					Hash:   "newest",
					Beacon: Beacon{Epoch: 294, ImmutableFileNumber: 5810},
				},
				{
					Hash:   "same-epoch-older",
					Beacon: Beacon{Epoch: 294, ImmutableFileNumber: 5800},
				},
			}
			require.NoError(t, json.NewEncoder(w).Encode(items))
		},
	)
	mux.HandleFunc(
		"/artifact/cardano-database/newest",
		func(w http.ResponseWriter, _ *http.Request) {
			detail := CardanoDatabaseSnapshot{
				Hash:    "newest",
				Network: "preprod",
				Beacon:  Beacon{Epoch: 294, ImmutableFileNumber: 5810},
			}
			require.NoError(t, json.NewEncoder(w).Encode(detail))
		},
	)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewClient(srv.URL)
	snapshot, err := client.GetLatestCardanoDatabaseSnapshot(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "newest", snapshot.Hash)
	assert.Equal(t, "preprod", snapshot.Network)
}

func TestGetLatestCardanoDatabaseSnapshotEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc(
		"/artifact/cardano-database",
		func(w http.ResponseWriter, _ *http.Request) {
			_, err := w.Write([]byte("[]"))
			require.NoError(t, err)
		},
	)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewClient(srv.URL)
	_, err := client.GetLatestCardanoDatabaseSnapshot(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no Cardano database snapshots")
}

func TestGetCardanoDatabaseDigests(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc(
		"/artifact/cardano-database/digests",
		func(w http.ResponseWriter, _ *http.Request) {
			entries := []CardanoDatabaseDigestEntry{
				{ImmutableFileName: "00000.chunk", Digest: "aa"},
				{ImmutableFileName: "00000.primary", Digest: "bb"},
			}
			require.NoError(t, json.NewEncoder(w).Encode(entries))
		},
	)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewClient(srv.URL)
	entries, err := client.GetCardanoDatabaseDigests(context.Background())
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "00000.chunk", entries[0].ImmutableFileName)
	assert.Equal(t, "bb", entries[1].Digest)
}

func TestSignedEntityTypeCardanoDatabase(t *testing.T) {
	var entity SignedEntityType
	require.NoError(t, json.Unmarshal(
		[]byte(`{"CardanoDatabase":{"epoch":294,"immutable_file_number":5810}}`),
		&entity,
	))
	beacon := entity.CardanoDatabase()
	require.NotNil(t, beacon)
	assert.Equal(t, uint64(294), beacon.Epoch)
	assert.Equal(t, uint64(5810), beacon.ImmutableFileNumber)
	// Mismatched accessor returns nil
	assert.Nil(t, entity.CardanoImmutableFilesFull())
}
