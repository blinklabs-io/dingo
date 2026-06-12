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
	"archive/tar"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2s"
)

func TestComputeMMRRootEmpty(t *testing.T) {
	_, err := computeMMRRoot(nil)
	require.Error(t, err)
}

func TestComputeMMRRootSingleLeaf(t *testing.T) {
	leaf := []byte("aabbcc")
	root, err := computeMMRRoot([][]byte{leaf})
	require.NoError(t, err)
	// A single-node MMR's root is the node itself, un-hashed.
	assert.Equal(t, leaf, root)
}

func TestComputeMMRRootTwoLeaves(t *testing.T) {
	l0 := []byte("leaf-zero")
	l1 := []byte("leaf-one")
	root, err := computeMMRRoot([][]byte{l0, l1})
	require.NoError(t, err)
	expected := blake2s.Sum256(append(append([]byte{}, l0...), l1...))
	assert.Equal(t, expected[:], root)
}

func TestComputeMMRRootThreeLeaves(t *testing.T) {
	l0 := []byte("leaf-zero")
	l1 := []byte("leaf-one")
	l2 := []byte("leaf-two")
	root, err := computeMMRRoot([][]byte{l0, l1, l2})
	require.NoError(t, err)
	// Peaks: [h(l0||l1) at height 1, l2 at height 0]. Bagging merges
	// right-to-left with reversed argument order: h(rightPeak || leftPeak).
	inner := blake2s.Sum256(append(append([]byte{}, l0...), l1...))
	expected := blake2s.Sum256(append(append([]byte{}, l2...), inner[:]...))
	assert.Equal(t, expected[:], root)
}

// loadV2DigestEntries extracts and parses the digest list from the
// real preprod digests archive fixture.
func loadV2DigestEntries(t *testing.T) []CardanoDatabaseDigestEntry {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "v2", "digests.tar.zst"))
	require.NoError(t, err)
	defer f.Close()
	zr, err := zstd.NewReader(f)
	require.NoError(t, err)
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if !strings.HasSuffix(hdr.Name, ".digests.json") {
			continue
		}
		var entries []CardanoDatabaseDigestEntry
		require.NoError(t, json.NewDecoder(tr).Decode(&entries))
		return entries
	}
	t.Fatal("no .digests.json member found in fixture archive")
	return nil
}

func loadV2DetailFixture(t *testing.T) *CardanoDatabaseSnapshot {
	t.Helper()
	data, err := os.ReadFile(
		filepath.Join("testdata", "v2", "cardano_database_detail.json"),
	)
	require.NoError(t, err)
	var snapshot CardanoDatabaseSnapshot
	require.NoError(t, json.Unmarshal(data, &snapshot))
	return &snapshot
}

// TestComputeMMRRootKnownVector rebuilds the merkle root certified by
// the real preprod aggregator for artifact 408eeb46... (epoch 294,
// immutable 5810) from its published digest list.
func TestComputeMMRRootKnownVector(t *testing.T) {
	snapshot := loadV2DetailFixture(t)
	entries := loadV2DigestEntries(t)
	require.NotEmpty(t, entries)

	leaves, err := digestMerkleLeaves(
		entries, snapshot.Beacon.ImmutableFileNumber,
	)
	require.NoError(t, err)
	require.Len(t, leaves, len(entries))

	root, err := computeMMRRoot(leaves)
	require.NoError(t, err)
	assert.Equal(t, snapshot.MerkleRoot, hex.EncodeToString(root))
}

func TestImmutableFileNumberFromName(t *testing.T) {
	for _, tt := range []struct {
		name string
		num  uint64
		ok   bool
	}{
		{name: "05810.chunk", num: 5810, ok: true},
		{name: "00000.secondary", num: 0, ok: true},
		{name: "99999.chunk", num: 99999, ok: true},
		{name: "100000.chunk", num: 100000, ok: true},
		{name: "18446744073709551615.chunk", num: 18446744073709551615, ok: true},
		{name: "5810.chunk"},
		{name: "005810.chunk"},
		{name: "099999.chunk"},
		{name: "18446744073709551616.chunk"},
		{name: "05a10.chunk"},
		{name: "00000."},
		{name: "clean"},
		{name: ""},
	} {
		num, ok := immutableFileNumberFromName(tt.name)
		assert.Equal(t, tt.ok, ok, tt.name)
		if tt.ok {
			assert.Equal(t, tt.num, num, tt.name)
		}
	}
}
