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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createChunkArchive creates a zstd-compressed tar archive that
// contains files mimicking an ImmutableDB structure.
func createChunkArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer

	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)

	tw := tar.NewWriter(zw)

	// Create immutable directory entry
	err = tw.WriteHeader(&tar.Header{
		Name:     "immutable/",
		Typeflag: tar.TypeDir,
		Mode:     0o750,
	})
	require.NoError(t, err)

	// Create chunk files
	chunkFiles := map[string]string{
		"immutable/00000.chunk":     "chunk0 data",
		"immutable/00000.primary":   "primary0 data",
		"immutable/00000.secondary": "secondary0 data",
	}
	for name, content := range chunkFiles {
		err = tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0o640,
			Size: int64(len(content)),
		})
		require.NoError(t, err)
		_, err = tw.Write([]byte(content))
		require.NoError(t, err)
	}

	err = tw.Close()
	require.NoError(t, err)
	err = zw.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

func finalizeTestCertificate(t *testing.T, cert *Certificate) {
	t.Helper()
	if cert.ProtocolMessage.MessageParts == nil {
		cert.ProtocolMessage.MessageParts = map[string]string{}
	}
	if !cert.IsGenesis() && cert.SignedEntityType.Raw() == nil {
		require.NoError(t, json.Unmarshal(
			[]byte(`{"MithrilStakeDistribution":0}`),
			&cert.SignedEntityType,
		))
	}
	cert.SignedMessage = cert.ProtocolMessage.ComputeHash()
	hash, err := cert.ComputeHash()
	require.NoError(t, err)
	cert.Hash = hash
}

func TestBootstrap(t *testing.T) {
	archiveData := createChunkArchive(t)

	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:  "abc123def456789012345678",
				Network: "preprod",
				Beacon: Beacon{
					Epoch:               270,
					ImmutableFileNumber: 5320,
				},
				CertificateHash:      "cert123",
				Size:                 int64(len(archiveData)),
				CreatedAt:            "2026-02-10T00:24:56.094721055Z",
				Locations:            []string{}, // Will be set below
				CompressionAlgorithm: "zstandard",
			},
		},
	}

	mux := http.NewServeMux()

	// Snapshot list endpoint
	mux.HandleFunc(
		"/artifact/snapshots",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(snapshots); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		},
	)

	// Download endpoint
	mux.HandleFunc(
		"/download/snapshot.tar.zst",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(archiveData)
		},
	)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// Set the download URL to point to our test server
	snapshots[0].Locations = []string{
		server.URL + "/download/snapshot.tar.zst",
	}

	downloadDir := t.TempDir()

	var progressCalled atomic.Int32
	result, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:          "preprod",
		AggregatorURL:    server.URL,
		DownloadDir:      downloadDir,
		CleanupAfterLoad: true,
		OnProgress: func(p DownloadProgress) {
			progressCalled.Add(1)
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "abc123def456789012345678", result.Snapshot.Digest)
	require.NotEmpty(t, result.ImmutableDir)
	require.NotEmpty(t, result.ArchivePath)
	assert.Greater(
		t,
		int(progressCalled.Load()),
		0,
		"OnProgress should have been called",
	)

	// Verify the immutable directory contains chunk files
	require.True(t, hasChunkFiles(result.ImmutableDir))
}

func TestBootstrapCertVerifyNoCertHash(t *testing.T) {
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:          "abc123",
				Network:         "preprod",
				Locations:       []string{"http://example.com/s"},
				CertificateHash: "",
			},
		},
	}

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(snapshots)
		}),
	)
	t.Cleanup(server.Close)

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:                "preprod",
		AggregatorURL:          server.URL,
		VerifyCertificateChain: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no certificate hash")
}

func TestBootstrapNoSnapshots(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))
		}),
	)
	t.Cleanup(server.Close)

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:       "preprod",
		AggregatorURL: server.URL,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no snapshots available")
}

func TestBootstrapNoLocations(t *testing.T) {
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:    "abc123",
				Network:   "preprod",
				Locations: []string{},
			},
		},
	}

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(snapshots); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}),
	)
	t.Cleanup(server.Close)

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:       "preprod",
		AggregatorURL: server.URL,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no download locations")
}

func TestBootstrapInvalidGenesisVerificationKey(t *testing.T) {
	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:                "preview",
		GenesisVerificationKey: "not-hex",
		VerifyCertificateChain: true,
	})
	require.Error(t, err)
	require.Contains(
		t,
		err.Error(),
		"parsing Mithril genesis verification key",
	)
}

func TestBootstrapUnknownNetwork(t *testing.T) {
	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network: "unknown_network",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolving aggregator URL")
}

func TestBootstrapResultCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, []byte("data"), 0o640)
	require.NoError(t, err)

	extractDir := filepath.Join(tmpDir, "extract")
	require.NoError(t, os.MkdirAll(extractDir, 0o750))

	ancillaryDir := filepath.Join(tmpDir, "ancillary")
	require.NoError(t, os.MkdirAll(ancillaryDir, 0o750))

	result := &BootstrapResult{
		ArchivePath:  archivePath,
		ExtractDir:   extractDir,
		AncillaryDir: ancillaryDir,
	}

	result.Cleanup(nil)

	// Individual paths should be removed
	_, err = os.Stat(archivePath)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(extractDir)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(ancillaryDir)
	require.True(t, os.IsNotExist(err))

	// Parent directory should NOT be removed
	_, err = os.Stat(tmpDir)
	require.NoError(t, err)
}

func TestBootstrapResultCleanupRemovesTempDir(t *testing.T) {
	tmpDir := t.TempDir()
	autoTempDir := filepath.Join(tmpDir, "auto-temp")
	require.NoError(t, os.MkdirAll(autoTempDir, 0o750))
	// Place a file inside to verify recursive removal.
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(autoTempDir, "leftover"),
			[]byte("data"),
			0o640,
		),
	)

	result := &BootstrapResult{
		TempDir: autoTempDir,
	}
	result.Cleanup(nil)

	_, err := os.Stat(autoTempDir)
	require.True(t, os.IsNotExist(err))
}

func TestFindImmutableDir(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(t *testing.T, baseDir string)
		expected string // relative to baseDir, or "" for not found
	}{
		{
			name: "chunks in root",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				err := os.WriteFile(
					filepath.Join(baseDir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "ROOT",
		},
		{
			name: "chunks in immutable subdir",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				dir := filepath.Join(baseDir, "immutable")
				err := os.MkdirAll(dir, 0o750)
				require.NoError(t, err)
				err = os.WriteFile(
					filepath.Join(dir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "immutable",
		},
		{
			name: "chunks in db/immutable subdir",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				dir := filepath.Join(baseDir, "db", "immutable")
				err := os.MkdirAll(dir, 0o750)
				require.NoError(t, err)
				err = os.WriteFile(
					filepath.Join(dir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "db/immutable",
		},
		{
			name: "single top-level dir with immutable inside",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				dir := filepath.Join(
					baseDir, "snapshot-data", "immutable",
				)
				err := os.MkdirAll(dir, 0o750)
				require.NoError(t, err)
				err = os.WriteFile(
					filepath.Join(dir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "snapshot-data/immutable",
		},
		{
			name: "single top-level dir with db/immutable",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				dir := filepath.Join(
					baseDir,
					"snapshot-data",
					"db",
					"immutable",
				)
				err := os.MkdirAll(dir, 0o750)
				require.NoError(t, err)
				err = os.WriteFile(
					filepath.Join(dir, "00000.chunk"),
					[]byte("data"),
					0o640,
				)
				require.NoError(t, err)
			},
			expected: "snapshot-data/db/immutable",
		},
		{
			name:     "empty directory",
			setup:    func(t *testing.T, baseDir string) {},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir := t.TempDir()
			tt.setup(t, baseDir)

			result := findImmutableDir(baseDir)
			switch tt.expected {
			case "":
				require.Empty(t, result)
			case "ROOT":
				require.Equal(t, baseDir, result)
			default:
				expected := filepath.Join(baseDir, tt.expected)
				require.Equal(t, expected, result)
			}
		})
	}
}

func TestVerifyCertificateChainAllowsDeepChains(t *testing.T) {
	const chainDepth = 150
	const snapshotDigest = "snapshot-digest-123"
	_, _, g1, g2 := bls12381.Generators()
	g1Hex := hex.EncodeToString(g1.Marshal())
	g2Hex := hex.EncodeToString(g2.Marshal())

	certsByName := make(map[string]Certificate, chainDepth+1)
	for i := 0; i <= chainDepth; i++ {
		hash := fmt.Sprintf("cert-%03d", i)
		prev := fmt.Sprintf("cert-%03d", i+1)
		if i == chainDepth {
			prev = hash
		}
		cert := Certificate{
			Epoch:                    0,
			PreviousHash:             prev,
			AggregateVerificationKey: g2Hex,
			Metadata: CertificateMetadata{
				Network:     "preprod",
				Version:     "0.1.0",
				Parameters:  ProtocolParameters{K: 1, M: 2, PhiF: 0.5},
				InitiatedAt: "2026-02-10T00:00:00Z",
				SealedAt:    "2026-02-10T00:01:00Z",
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"current_epoch": "0",
				},
			},
		}
		if i == 0 {
			cert.ProtocolMessage.MessageParts["snapshot_digest"] =
				snapshotDigest
		}
		if i == chainDepth {
			cert.GenesisSignature = "genesis_sig"
		} else {
			cert.MultiSignature = g1Hex
		}
		finalizeTestCertificate(t, &cert)
		certsByName[hash] = cert
	}
	certs := make(map[string]Certificate, chainDepth+1)
	for i := chainDepth; i >= 0; i-- {
		key := fmt.Sprintf("cert-%03d", i)
		cert := certsByName[key]
		if i == chainDepth {
			cert.PreviousHash = cert.Hash
		} else {
			parentKey := fmt.Sprintf("cert-%03d", i+1)
			cert.PreviousHash = certsByName[parentKey].Hash
		}
		finalizeTestCertificate(t, &cert)
		certsByName[key] = cert
		certs[cert.Hash] = cert
	}

	server := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		var hash string
		_, err := fmt.Sscanf(r.URL.Path, "/certificate/%s", &hash)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		cert, ok := certs[hash]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cert)
	}))
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		certsByName["cert-000"].Hash,
		snapshotDigest,
	)
	require.NoError(t, err)
}

func TestVerifyCertificateChainWithModeSTMRejectsContentHashMismatch(
	t *testing.T,
) {
	_, _, g1, g2 := bls12381.Generators()
	g1Bytes := g1.Bytes()
	g2Bytes := g2.Bytes()
	cert := Certificate{
		Hash:         "leaf",
		PreviousHash: "leaf",
		Epoch:        1,
		SignedEntityType: SignedEntityType{
			raw: json.RawMessage(
				`{"CardanoImmutableFilesFull":{"epoch":1,"immutable_file_number":1}}`,
			),
		},
		Metadata: CertificateMetadata{
			Parameters:  ProtocolParameters{K: 1, M: 1, PhiF: 1.0},
			InitiatedAt: "2026-02-10T00:00:00Z",
			SealedAt:    "2026-02-10T00:01:00Z",
		},
		ProtocolMessage: ProtocolMessage{
			MessageParts: map[string]string{
				"snapshot_digest": "digest",
				"current_epoch":   "1",
			},
		},
		SignedMessage: ProtocolMessage{
			MessageParts: map[string]string{
				"snapshot_digest": "digest",
				"current_epoch":   "1",
			},
		}.ComputeHash(),
		MultiSignature: hex.EncodeToString(
			g1Bytes[:],
		),
		AggregateVerificationKey: hex.EncodeToString(
			g2Bytes[:],
		),
	}
	certHash, err := cert.ComputeHash()
	require.NoError(t, err)
	cert.Hash = certHash
	cert.PreviousHash = cert.Hash

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/certificate/"+cert.Hash {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(&cert)
		}),
	)
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	_, err = VerifyCertificateChainWithMode(
		context.Background(),
		client,
		cert.Hash,
		"digest",
		VerificationModeSTM,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "content hash mismatch")
}

func TestVerifyCertificateChainWithModeReturnsDetails(t *testing.T) {
	_, _, g1, g2 := bls12381.Generators()
	certs := map[string]Certificate{
		"leaf": {
			Epoch:                    270,
			PreviousHash:             "root",
			MultiSignature:           hex.EncodeToString(g1.Marshal()),
			AggregateVerificationKey: hex.EncodeToString(g2.Marshal()),
			SignedEntityType: SignedEntityType{
				raw: json.RawMessage(
					`{"CardanoImmutableFilesFull":{"epoch":270,"immutable_file_number":5320}}`,
				),
			},
			Metadata: CertificateMetadata{
				Parameters:  ProtocolParameters{K: 1, M: 2, PhiF: 0.5},
				InitiatedAt: "2026-02-10T00:00:00Z",
				SealedAt:    "2026-02-10T00:01:00Z",
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"snapshot_digest": "matching_digest",
					"current_epoch":   "270",
				},
			},
		},
		"root": {
			Epoch:            269,
			GenesisSignature: "genesis_sig",
			Metadata: CertificateMetadata{
				Parameters:  ProtocolParameters{K: 1, M: 2, PhiF: 0.5},
				InitiatedAt: "2026-02-09T00:00:00Z",
				SealedAt:    "2026-02-09T00:01:00Z",
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"current_epoch":                   "269",
					"next_aggregate_verification_key": hex.EncodeToString(g2.Marshal()),
					"next_protocol_parameters":        ProtocolParameters{K: 1, M: 2, PhiF: 0.5}.ComputeHash(),
				},
			},
		},
	}
	root := certs["root"]
	finalizeTestCertificate(t, &root)
	root.PreviousHash = root.Hash
	finalizeTestCertificate(t, &root)
	certs["root"] = root
	leaf := certs["leaf"]
	leaf.PreviousHash = root.Hash
	finalizeTestCertificate(t, &leaf)
	certs["leaf"] = leaf

	server := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		hash := r.URL.Path[len("/certificate/"):]
		var cert Certificate
		ok := false
		for _, candidate := range certs {
			if candidate.Hash == hash {
				cert = candidate
				ok = true
				break
			}
		}
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cert)
	}))
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	result, err := VerifyCertificateChainWithMode(
		context.Background(),
		client,
		leaf.Hash,
		"matching_digest",
		VerificationModeStructural,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Certificates, 2)
	require.Equal(t, leaf.Hash, result.LeafCertificate.Hash)
	require.Equal(t, root.Hash, result.GenesisCertificate.Hash)
	require.Equal(t, "CardanoImmutableFilesFull", result.SignedEntityKind)
	require.Equal(t, "matching_digest", result.SnapshotDigest)
}

func TestBootstrapRejectsUnexpectedSignedEntityKind(t *testing.T) {
	_, _, g1, g2 := bls12381.Generators()
	g1Hex := hex.EncodeToString(g1.Marshal())
	g2Hex := hex.EncodeToString(g2.Marshal())
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:          "digest123",
				Network:         "preprod",
				CertificateHash: "cert_leaf",
				Locations:       []string{"https://example.com/snapshot.tar.zst"},
				Beacon: Beacon{
					Epoch:               270,
					ImmutableFileNumber: 5320,
				},
			},
		},
	}
	certs := map[string]Certificate{
		"cert_leaf": {
			Epoch:                    270,
			AggregateVerificationKey: g2Hex,
			MultiSignature:           g1Hex,
			SignedEntityType: SignedEntityType{
				raw: json.RawMessage(
					`{"MithrilStakeDistribution":{"epoch":270,"immutable_file_number":5320}}`,
				),
			},
			Metadata: CertificateMetadata{
				Network:     "preprod",
				Parameters:  ProtocolParameters{K: 1, M: 2, PhiF: 0.5},
				InitiatedAt: "2026-02-10T00:00:00Z",
				SealedAt:    "2026-02-10T00:01:00Z",
				Signers: []StakeDistributionParty{
					{PartyID: "pool1abc123", Stake: 42},
				},
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"snapshot_digest": "digest123",
					"current_epoch":   "270",
				},
			},
		},
		"cert_genesis": {
			Epoch:            269,
			GenesisSignature: "genesis",
			Metadata: CertificateMetadata{
				Parameters:  ProtocolParameters{K: 1, M: 2, PhiF: 0.5},
				InitiatedAt: "2026-02-09T00:00:00Z",
				SealedAt:    "2026-02-09T00:01:00Z",
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"current_epoch":                   "269",
					"next_aggregate_verification_key": g2Hex,
					"next_protocol_parameters":        ProtocolParameters{K: 1, M: 2, PhiF: 0.5}.ComputeHash(),
				},
			},
		},
	}
	genesis := certs["cert_genesis"]
	finalizeTestCertificate(t, &genesis)
	genesis.PreviousHash = genesis.Hash
	finalizeTestCertificate(t, &genesis)
	certs["cert_genesis"] = genesis
	leaf := certs["cert_leaf"]
	leaf.PreviousHash = genesis.Hash
	finalizeTestCertificate(t, &leaf)
	certs["cert_leaf"] = leaf
	snapshots[0].CertificateHash = leaf.Hash
	server := httptest.NewServer(http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/artifact/snapshots":
			_ = json.NewEncoder(w).Encode(snapshots)
		case "/certificate/" + leaf.Hash:
			_ = json.NewEncoder(w).Encode(certs["cert_leaf"])
		case "/certificate/" + genesis.Hash:
			_ = json.NewEncoder(w).Encode(certs["cert_genesis"])
		case "/artifact/mithril-stake-distributions":
			_ = json.NewEncoder(w).Encode([]MithrilStakeDistributionListItem{
				{Hash: "msd123", CertificateHash: leaf.Hash, Epoch: 270},
			})
		case "/artifact/mithril-stake-distribution/msd123":
			_ = json.NewEncoder(w).Encode(MithrilStakeDistribution{
				Hash:            "msd123",
				CertificateHash: leaf.Hash,
				Epoch:           270,
				Signers: []MithrilStakeDistributionParty{
					{
						PartyID:         "pool1abc123",
						Stake:           42,
						VerificationKey: g2Hex,
					},
				},
			})
		case "/artifact/cardano-stake-distributions":
			_ = json.NewEncoder(w).Encode([]CardanoStakeDistributionListItem{
				{Hash: "csd123", CertificateHash: leaf.Hash, Epoch: 270},
			})
		case "/artifact/cardano-stake-distribution/csd123":
			_ = json.NewEncoder(w).Encode(CardanoStakeDistribution{
				Hash:            "csd123",
				CertificateHash: leaf.Hash,
				Epoch:           270,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:                "preprod",
		AggregatorURL:          server.URL,
		VerifyCertificateChain: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected signed entity kind")
}
