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
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTarZst builds a zstd-compressed tar archive from the given
// file map (paths use forward slashes).
func buildTarZst(t *testing.T, files map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	tw := tar.NewWriter(zw)
	for name, content := range files {
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0o640,
			Size: int64(len(content)),
		}))
		_, err = tw.Write(content)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

// mithrilJSONHexKey encodes a raw key in Mithril's JSON-hex key
// format: hex of the ASCII JSON array of byte values.
func mithrilJSONHexKey(t *testing.T, key []byte) string {
	t.Helper()
	ints := make([]int, len(key))
	for i, b := range key {
		ints[i] = int(b)
	}
	encoded, err := json.Marshal(ints)
	require.NoError(t, err)
	return hex.EncodeToString(encoded)
}

type v2FixtureOptions struct {
	immutableFileNumber  uint64
	tamperArtifactHash   bool
	tamperCertMerkleRoot bool
	tamperDigestList     bool
	tamperImmutable      *uint64
	badAncillarySig      bool
	digestsCloud404      bool
	digestsCloudBadRoot  bool
	immutableBadMirror   bool
}

type v2Fixture struct {
	server               *httptest.Server
	artifact             *CardanoDatabaseSnapshot
	listItems            []CardanoDatabaseSnapshotListItem
	certs                map[string]Certificate
	leafHash             string
	digestEntries        []CardanoDatabaseDigestEntry
	digestArchive        []byte
	immutableArchives    map[uint64][]byte
	badImmutableArchives map[uint64][]byte
	immutableContent     map[string][]byte
	ancillaryArchive     []byte
	ancillaryVKey        string
	immutableHits        atomic.Int32
}

// newV2Fixture fabricates a complete, internally-consistent v2 mock
// aggregator: immutable archives, certified digest list, ancillary
// archive with a signed manifest, and a leaf+genesis certificate
// chain (structural verification).
func newV2Fixture(t *testing.T, opts v2FixtureOptions) *v2Fixture {
	t.Helper()
	fixture := &v2Fixture{
		certs:                map[string]Certificate{},
		immutableArchives:    map[uint64][]byte{},
		badImmutableArchives: map[uint64][]byte{},
		immutableContent:     map[string][]byte{},
	}

	mux := http.NewServeMux()
	fixture.server = httptest.NewServer(mux)
	t.Cleanup(fixture.server.Close)
	baseURL := fixture.server.URL

	// Immutable trios and their digest entries
	for num := uint64(0); num <= opts.immutableFileNumber; num++ {
		files := map[string][]byte{}
		for _, ext := range []string{"chunk", "primary", "secondary"} {
			name := fmt.Sprintf("%05d.%s", num, ext)
			content := fmt.Appendf(nil, "immutable-%s-data", name)
			fixture.immutableContent[name] = content
			files["immutable/"+name] = content
			digest := sha256.Sum256(content)
			fixture.digestEntries = append(
				fixture.digestEntries,
				CardanoDatabaseDigestEntry{
					ImmutableFileName: name,
					Digest:            hex.EncodeToString(digest[:]),
				},
			)
		}
		badFiles := map[string][]byte{}
		for name, content := range files {
			badFiles[name] = content
		}
		badFiles[fmt.Sprintf("immutable/%05d.chunk", num)] = []byte(
			"corrupted content",
		)
		archiveFiles := files
		if opts.tamperImmutable != nil && *opts.tamperImmutable == num {
			archiveFiles = badFiles
		}
		fixture.immutableArchives[num] = buildTarZst(t, archiveFiles)
		if opts.immutableBadMirror {
			fixture.badImmutableArchives[num] = buildTarZst(t, badFiles)
		}
	}

	leaves, err := digestMerkleLeaves(
		fixture.digestEntries, opts.immutableFileNumber,
	)
	require.NoError(t, err)
	root, err := computeMMRRoot(leaves)
	require.NoError(t, err)
	merkleRoot := hex.EncodeToString(root)

	digestArchiveEntries := fixture.digestEntries
	if opts.tamperDigestList || opts.digestsCloudBadRoot {
		digestArchiveEntries = append(
			[]CardanoDatabaseDigestEntry(nil),
			fixture.digestEntries...,
		)
		digestArchiveEntries[0].Digest = strings.Repeat("0", 64)
		if opts.tamperDigestList {
			fixture.digestEntries = digestArchiveEntries
		}
	}
	digestJSON, err := json.Marshal(digestArchiveEntries)
	require.NoError(t, err)
	fixture.digestArchive = buildTarZst(t, map[string][]byte{
		"preprod-e294-i00002.digests.json": digestJSON,
	})

	// Ancillary archive with signed manifest
	ancillaryPub, ancillaryPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	fixture.ancillaryVKey = mithrilJSONHexKey(t, ancillaryPub)
	nextTrio := opts.immutableFileNumber + 1
	ancillaryFiles := map[string][]byte{
		"ledger/100/state": []byte("ledger state data"),
	}
	for _, ext := range []string{"chunk", "primary", "secondary"} {
		name := fmt.Sprintf("immutable/%05d.%s", nextTrio, ext)
		ancillaryFiles[name] = fmt.Appendf(nil, "ancillary-%s", name)
	}
	manifest := ancillaryManifest{Data: map[string]string{}}
	for name, content := range ancillaryFiles {
		digest := sha256.Sum256(content)
		manifest.Data[name] = hex.EncodeToString(digest[:])
	}
	signingKey := ancillaryPriv
	if opts.badAncillarySig {
		_, wrongKey, err := ed25519.GenerateKey(nil)
		require.NoError(t, err)
		signingKey = wrongKey
	}
	manifest.Signature = hex.EncodeToString(
		ed25519.Sign(signingKey, manifest.computeHash()),
	)
	manifestJSON, err := json.Marshal(manifest)
	require.NoError(t, err)
	ancillaryFiles[ancillaryManifestFilename] = manifestJSON
	fixture.ancillaryArchive = buildTarZst(t, ancillaryFiles)

	immutableLocations := []CardanoDatabaseLocation{}
	if opts.immutableBadMirror {
		immutableLocations = append(
			immutableLocations,
			CardanoDatabaseLocation{
				Type:                 locationTypeCloudStorage,
				URITemplate:          baseURL + "/files/imm-bad/{immutable_file_number}.tar.zst",
				CompressionAlgorithm: "zstandard",
			},
		)
	}
	immutableLocations = append(
		immutableLocations,
		CardanoDatabaseLocation{
			Type:                 locationTypeCloudStorage,
			URITemplate:          baseURL + "/files/imm/{immutable_file_number}.tar.zst",
			CompressionAlgorithm: "zstandard",
		},
	)

	// Artifact detail and list entry
	artifact := &CardanoDatabaseSnapshot{
		MerkleRoot:              merkleRoot,
		Network:                 "preprod",
		Beacon:                  Beacon{Epoch: 294, ImmutableFileNumber: opts.immutableFileNumber},
		TotalDbSizeUncompressed: 4096,
		Digests: CardanoDatabaseDigests{
			SizeUncompressed: int64(len(digestJSON)),
			Locations: []CardanoDatabaseLocation{
				{
					Type:                 locationTypeCloudStorage,
					URI:                  baseURL + "/files/digests.tar.zst",
					CompressionAlgorithm: "zstandard",
				},
				{
					Type: locationTypeAggregator,
					URI:  baseURL + "/artifact/cardano-database/digests",
				},
			},
		},
		Immutables: CardanoDatabaseImmutables{
			AverageSizeUncompressed: 64,
			Locations:               immutableLocations,
		},
		Ancillary: CardanoDatabaseAncillary{
			SizeUncompressed: int64(len(fixture.ancillaryArchive)),
			Locations: []CardanoDatabaseLocation{
				{
					Type:                 locationTypeCloudStorage,
					URI:                  baseURL + "/files/ancillary.tar.zst",
					CompressionAlgorithm: "zstandard",
				},
			},
		},
		CardanoNodeVersion: "11.0.1",
		CreatedAt:          "2026-06-12T00:00:00.000000000Z",
	}
	artifact.Hash = artifact.ComputeHash()
	if opts.tamperArtifactHash {
		artifact.Hash = strings.Repeat("d", 64)
	}
	fixture.artifact = artifact

	// Certificate chain (genesis <- leaf), structural verification
	_, _, g1, g2 := bls12381.Generators()
	g1Hex := hex.EncodeToString(g1.Marshal())
	g2Hex := hex.EncodeToString(g2.Marshal())
	params := ProtocolParameters{K: 1, M: 2, PhiF: 0.5}
	certMerkleRoot := merkleRoot
	if opts.tamperCertMerkleRoot {
		certMerkleRoot = strings.Repeat("e", 64)
	}
	leaf := Certificate{
		Epoch:                    294,
		AggregateVerificationKey: g2Hex,
		MultiSignature:           g1Hex,
		SignedEntityType: SignedEntityType{
			raw: json.RawMessage(fmt.Sprintf(
				`{"CardanoDatabase":{"epoch":294,"immutable_file_number":%d}}`,
				opts.immutableFileNumber,
			)),
		},
		Metadata: CertificateMetadata{
			Network:     "preprod",
			Parameters:  params,
			InitiatedAt: "2026-06-12T00:00:00Z",
			SealedAt:    "2026-06-12T00:01:00Z",
			Signers: []StakeDistributionParty{
				{PartyID: "pool1abc123", Stake: 42},
			},
		},
		ProtocolMessage: ProtocolMessage{
			MessageParts: map[string]string{
				"cardano_database_merkle_root": certMerkleRoot,
				"current_epoch":                "294",
			},
		},
	}
	genesis := Certificate{
		Epoch:            293,
		GenesisSignature: "genesis_sig",
		Metadata: CertificateMetadata{
			Parameters:  params,
			InitiatedAt: "2026-06-11T00:00:00Z",
			SealedAt:    "2026-06-11T00:01:00Z",
		},
		ProtocolMessage: ProtocolMessage{
			MessageParts: map[string]string{
				"current_epoch":                   "293",
				"next_aggregate_verification_key": g2Hex,
				"next_protocol_parameters":        params.ComputeHash(),
			},
		},
	}
	finalizeTestCertificate(t, &genesis)
	genesis.PreviousHash = genesis.Hash
	finalizeTestCertificate(t, &genesis)
	leaf.PreviousHash = genesis.Hash
	finalizeTestCertificate(t, &leaf)
	fixture.certs[genesis.Hash] = genesis
	fixture.certs[leaf.Hash] = leaf
	fixture.leafHash = leaf.Hash
	artifact.CertificateHash = leaf.Hash

	fixture.listItems = []CardanoDatabaseSnapshotListItem{
		{
			Hash:                    artifact.Hash,
			MerkleRoot:              artifact.MerkleRoot,
			Beacon:                  artifact.Beacon,
			CertificateHash:         artifact.CertificateHash,
			TotalDbSizeUncompressed: artifact.TotalDbSizeUncompressed,
			CardanoNodeVersion:      artifact.CardanoNodeVersion,
			CreatedAt:               artifact.CreatedAt,
		},
	}

	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(v))
	}
	mux.HandleFunc(
		"/",
		func(w http.ResponseWriter, r *http.Request) {
			p := r.URL.Path
			switch {
			case p == "/artifact/cardano-database":
				writeJSON(w, fixture.listItems)
			case p == "/artifact/cardano-database/digests":
				writeJSON(w, fixture.digestEntries)
			case p == "/artifact/cardano-database/"+artifact.Hash:
				writeJSON(w, fixture.artifact)
			case p == "/files/digests.tar.zst":
				if opts.digestsCloud404 {
					http.NotFound(w, r)
					return
				}
				_, _ = w.Write(fixture.digestArchive)
			case p == "/files/ancillary.tar.zst":
				_, _ = w.Write(fixture.ancillaryArchive)
			case strings.HasPrefix(p, "/files/imm-bad/"):
				name := strings.TrimSuffix(
					strings.TrimPrefix(p, "/files/imm-bad/"),
					".tar.zst",
				)
				num, ok := immutableFileNumberFromName(name + ".chunk")
				archive, exists := fixture.badImmutableArchives[num]
				if !ok || !exists {
					http.NotFound(w, r)
					return
				}
				_, _ = w.Write(archive)
			case strings.HasPrefix(p, "/files/imm/"):
				name := strings.TrimSuffix(
					strings.TrimPrefix(p, "/files/imm/"),
					".tar.zst",
				)
				num, ok := immutableFileNumberFromName(name + ".chunk")
				archive, exists := fixture.immutableArchives[num]
				if !ok || !exists {
					http.NotFound(w, r)
					return
				}
				fixture.immutableHits.Add(1)
				_, _ = w.Write(archive)
			case strings.HasPrefix(p, "/certificate/"):
				hash := strings.TrimPrefix(p, "/certificate/")
				cert, ok := fixture.certs[hash]
				if !ok {
					http.NotFound(w, r)
					return
				}
				writeJSON(w, cert)
			case p == "/artifact/mithril-stake-distributions":
				writeJSON(w, []MithrilStakeDistributionListItem{
					{Hash: "msd123", CertificateHash: fixture.leafHash, Epoch: 294},
				})
			case p == "/artifact/mithril-stake-distribution/msd123":
				writeJSON(w, MithrilStakeDistribution{
					Hash:            "msd123",
					CertificateHash: fixture.leafHash,
					Epoch:           294,
					Signers: []MithrilStakeDistributionParty{
						{
							PartyID:         "pool1abc123",
							Stake:           42,
							VerificationKey: g2Hex,
						},
					},
				})
			case p == "/artifact/cardano-stake-distributions":
				writeJSON(w, []CardanoStakeDistributionListItem{
					{Hash: "csd123", CertificateHash: fixture.leafHash, Epoch: 294},
				})
			case p == "/artifact/cardano-stake-distribution/csd123":
				writeJSON(w, CardanoStakeDistribution{
					Hash:            "csd123",
					CertificateHash: fixture.leafHash,
					Epoch:           294,
				})
			default:
				http.NotFound(w, r)
			}
		},
	)

	return fixture
}

func (f *v2Fixture) bootstrapConfig(downloadDir string) BootstrapConfig {
	return BootstrapConfig{
		Network:                  "preprod",
		Backend:                  BackendV2,
		AggregatorURL:            f.server.URL,
		DownloadDir:              downloadDir,
		VerifyCertificateChain:   true,
		AncillaryVerificationKey: f.ancillaryVKey,
	}
}

func TestBootstrapV2(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 2})
	downloadDir := t.TempDir()

	var progressCalled atomic.Int32
	cfg := fixture.bootstrapConfig(downloadDir)
	cfg.OnProgress = func(DownloadProgress) {
		progressCalled.Add(1)
	}
	result, err := Bootstrap(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Synthesized snapshot metadata
	require.NotNil(t, result.Snapshot)
	assert.Equal(t, fixture.artifact.Hash, result.Snapshot.Digest)
	assert.Equal(t, "preprod", result.Snapshot.Network)
	assert.Equal(t, uint64(294), result.Snapshot.Beacon.Epoch)
	assert.Equal(
		t,
		fixture.artifact.TotalDbSizeUncompressed,
		result.Snapshot.Size,
	)

	// All immutable trios extracted with verified content
	require.Equal(
		t,
		filepath.Join(downloadDir, "immutable-"+fixture.artifact.Hash),
		result.ExtractDir,
	)
	require.Equal(
		t,
		filepath.Join(result.ExtractDir, "immutable"),
		result.ImmutableDir,
	)
	require.True(t, hasChunkFiles(result.ImmutableDir))
	for name, content := range fixture.immutableContent {
		data, err := os.ReadFile(filepath.Join(result.ImmutableDir, name))
		require.NoError(t, err)
		assert.Equal(t, content, data, name)
	}

	// Ancillary extracted and verified
	require.NotEmpty(t, result.AncillaryDir)
	data, err := os.ReadFile(
		filepath.Join(result.AncillaryDir, "ledger", "100", "state"),
	)
	require.NoError(t, err)
	assert.Equal(t, []byte("ledger state data"), data)

	assert.Positive(t, int(progressCalled.Load()))
}

func TestBootstrapV2NoCertVerification(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 1})
	cfg := fixture.bootstrapConfig(t.TempDir())
	cfg.VerifyCertificateChain = false
	cfg.AncillaryVerificationKey = ""

	result, err := Bootstrap(context.Background(), cfg)
	require.NoError(t, err)
	require.True(t, hasChunkFiles(result.ImmutableDir))
	require.NotEmpty(t, result.AncillaryDir)
}

func TestBootstrapV2DigestsAggregatorFallback(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 1,
		digestsCloud404:     true,
	})
	result, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	require.NoError(t, err)
	require.True(t, hasChunkFiles(result.ImmutableDir))
}

func TestBootstrapV2DigestsMerkleMismatchFallsBack(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 1,
		digestsCloudBadRoot: true,
	})
	result, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	require.NoError(t, err)
	require.True(t, hasChunkFiles(result.ImmutableDir))
}

func TestBootstrapV2MerkleRootMismatch(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 1,
		tamperDigestList:    true,
	})
	_, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "merkle root mismatch")
}

func TestBootstrapV2ArtifactHashMismatch(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 1,
		tamperArtifactHash:  true,
	})
	_, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "artifact hash mismatch")
}

func TestBootstrapV2CertLeafMerkleRootMismatch(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber:  1,
		tamperCertMerkleRoot: true,
	})
	_, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cardano_database_merkle_root")
}

func TestBootstrapV2ImmutableDigestMismatch(t *testing.T) {
	for _, tamperImmutable := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("immutable_%05d", tamperImmutable), func(t *testing.T) {
			fixture := newV2Fixture(t, v2FixtureOptions{
				immutableFileNumber: 2,
				tamperImmutable:     &tamperImmutable,
			})
			downloadDir := t.TempDir()
			_, err := Bootstrap(
				context.Background(),
				fixture.bootstrapConfig(downloadDir),
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "digest")

			// The corrupted trio must not be left behind for resume reuse
			immutableDir := filepath.Join(
				downloadDir,
				"immutable-"+fixture.artifact.Hash,
				"immutable",
			)
			_, statErr := os.Stat(
				filepath.Join(
					immutableDir,
					fmt.Sprintf("%05d.chunk", tamperImmutable),
				),
			)
			assert.True(t, os.IsNotExist(statErr))
		})
	}
}

func TestBootstrapV2ImmutableValidationFallsBackToMirror(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 1,
		immutableBadMirror:  true,
	})
	result, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	require.NoError(t, err)
	require.True(t, hasChunkFiles(result.ImmutableDir))
	for name, content := range fixture.immutableContent {
		data, err := os.ReadFile(filepath.Join(result.ImmutableDir, name))
		require.NoError(t, err)
		assert.Equal(t, content, data, name)
	}
}

func TestBootstrapV2AncillaryBadSignature(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 1,
		badAncillarySig:     true,
	})
	result, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(t.TempDir()),
	)
	// Ancillary failure is non-fatal, matching v1 behavior: the node
	// continues without ledger state.
	require.NoError(t, err)
	assert.Empty(t, result.AncillaryDir)
	require.True(t, hasChunkFiles(result.ImmutableDir))
}

func TestBootstrapV2ResumeVerifiesCachedAncillary(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 1})
	downloadDir := t.TempDir()
	candidateDir := filepath.Join(
		downloadDir,
		"ancillary-"+fixture.artifact.Hash,
	)
	require.NoError(
		t,
		os.MkdirAll(filepath.Join(candidateDir, "ledger", "100"), 0o750),
	)
	staleState := []byte("stale ledger state")
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(candidateDir, "ledger", "100", "state"),
			staleState,
			0o640,
		),
	)
	staleDigest := sha256.Sum256(staleState)
	manifest := ancillaryManifest{
		Data: map[string]string{
			"ledger/100/state": hex.EncodeToString(staleDigest[:]),
		},
		Signature: strings.Repeat("0", ed25519.SignatureSize*2),
	}
	manifestJSON, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(candidateDir, ancillaryManifestFilename),
			manifestJSON,
			0o640,
		),
	)
	candidateArchive := filepath.Join(
		downloadDir,
		fmt.Sprintf(
			"%s-%s-ancillary.tar.zst",
			fixture.artifact.Network,
			truncateDigest(fixture.artifact.Hash),
		),
	)
	require.NoError(t, os.WriteFile(candidateArchive, []byte("stale"), 0o640))

	result, err := Bootstrap(
		context.Background(),
		fixture.bootstrapConfig(downloadDir),
	)
	require.NoError(t, err)
	data, err := os.ReadFile(
		filepath.Join(result.AncillaryDir, "ledger", "100", "state"),
	)
	require.NoError(t, err)
	assert.Equal(t, []byte("ledger state data"), data)
	assert.Equal(t, candidateArchive, result.AncillaryArchivePath)
}

func TestBootstrapV2Resume(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 2})
	downloadDir := t.TempDir()
	cfg := fixture.bootstrapConfig(downloadDir)

	_, err := Bootstrap(context.Background(), cfg)
	require.NoError(t, err)
	firstRunHits := fixture.immutableHits.Load()
	require.Positive(t, firstRunHits)

	result, err := Bootstrap(context.Background(), cfg)
	require.NoError(t, err)
	require.True(t, hasChunkFiles(result.ImmutableDir))
	assert.Equal(
		t,
		firstRunHits,
		fixture.immutableHits.Load(),
		"second run should not re-download verified immutable archives",
	)
}

func TestBootstrapEmptyBackendDefaultsToV2(t *testing.T) {
	fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 1})
	cfg := fixture.bootstrapConfig(t.TempDir())
	cfg.Backend = ""

	result, err := Bootstrap(context.Background(), cfg)
	require.NoError(t, err)
	require.True(t, hasChunkFiles(result.ImmutableDir))
}

func TestBootstrapUnsupportedBackend(t *testing.T) {
	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network: "preprod",
		Backend: "v3",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported Mithril backend")
}
