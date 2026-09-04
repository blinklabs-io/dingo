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
	require.NotNil(t, cert)
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

func testGenesisKeyPair(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return fmt.Sprintf(
		`{"type":"GenesisVerificationKey_ed25519","cborHex":"5820%s"}`,
		hex.EncodeToString(publicKey),
	), privateKey
}

func signTestGenesisCertificate(
	t *testing.T,
	cert *Certificate,
	privateKey ed25519.PrivateKey,
) {
	t.Helper()
	cert.SignedMessage = cert.ProtocolMessage.ComputeHash()
	cert.GenesisSignature = hex.EncodeToString(
		ed25519.Sign(privateKey, []byte(cert.SignedMessage)),
	)
	finalizeTestCertificate(t, cert)
	cert.PreviousHash = cert.Hash
	finalizeTestCertificate(t, cert)
}

func TestBootstrap(t *testing.T) {
	archiveData := createChunkArchive(t)

	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:  "a123456789abcdef0a123456789abcdef0a123456789abcdef0a123456789abc",
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
		Network:           "preprod",
		Backend:           BackendV1,
		AggregatorURL:     server.URL,
		AllowInsecureHTTP: true,
		DownloadDir:       downloadDir,
		CleanupAfterLoad:  true,
		OnProgress: func(p DownloadProgress) {
			progressCalled.Add(1)
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(
		t,
		"a123456789abcdef0a123456789abcdef0a123456789abcdef0a123456789abc",
		result.Snapshot.Digest,
	)
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

func TestBootstrapUsesDigestSpecificExtractDir(t *testing.T) {
	archiveData := createChunkArchive(t)
	digest := "b123456789abcdef0b123456789abcdef0b123456789abcdef0b123456789abc"
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:    digest,
				Network:   "preprod",
				Size:      int64(len(archiveData)),
				Locations: []string{},
			},
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc(
		"/artifact/snapshots",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(snapshots))
		},
	)
	mux.HandleFunc(
		"/download/snapshot.tar.zst",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(archiveData)
		},
	)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	snapshots[0].Locations = []string{
		server.URL + "/download/snapshot.tar.zst",
	}
	downloadDir := t.TempDir()
	staleImmutableDir := filepath.Join(
		downloadDir,
		"immutable",
		"immutable",
	)
	require.NoError(t, os.MkdirAll(staleImmutableDir, 0o750))
	require.NoError(
		t,
		os.WriteFile(
			filepath.Join(staleImmutableDir, "00000.chunk"),
			[]byte("stale chunk"),
			0o640,
		),
	)

	result, err := Bootstrap(context.Background(), BootstrapConfig{
		Network:           "preprod",
		Backend:           BackendV1,
		AggregatorURL:     server.URL,
		AllowInsecureHTTP: true,
		DownloadDir:       downloadDir,
	})
	require.NoError(t, err)
	require.Equal(
		t,
		filepath.Join(downloadDir, "immutable-"+digest),
		result.ExtractDir,
	)
	require.NotEqual(t, staleImmutableDir, result.ImmutableDir)
	require.True(t, hasChunkFiles(result.ImmutableDir))
}

func TestBootstrapCertVerifyNoCertHash(t *testing.T) {
	genesisVerificationKey, _ := testGenesisKeyPair(t)
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:          "c123456789abcdef0c123456789abcdef0c123456789abcdef0c123456789abc",
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
		Backend:                BackendV1,
		AggregatorURL:          server.URL,
		AllowInsecureHTTP:      true,
		VerifyCertificateChain: true,
		GenesisVerificationKey: genesisVerificationKey,
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
		Network:           "preprod",
		Backend:           BackendV1,
		AggregatorURL:     server.URL,
		AllowInsecureHTTP: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no snapshots available")
}

func TestBootstrapNoLocations(t *testing.T) {
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:    "d123456789abcdef0d123456789abcdef0d123456789abcdef0d123456789abc",
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
		Network:           "preprod",
		Backend:           BackendV1,
		AggregatorURL:     server.URL,
		AllowInsecureHTTP: true,
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

func TestBootstrapRequiresGenesisVerificationKey(t *testing.T) {
	for _, backend := range []string{BackendV1, BackendV2} {
		for _, testKey := range []struct {
			name  string
			value string
		}{
			{name: "empty"},
			{name: "whitespace", value: " \n\t"},
		} {
			t.Run(backend+"/"+testKey.name, func(t *testing.T) {
				var requests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(
					func(w http.ResponseWriter, _ *http.Request) {
						requests.Add(1)
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte("[]"))
					},
				))
				t.Cleanup(server.Close)

				_, err := Bootstrap(context.Background(), BootstrapConfig{
					Network:                "preview",
					Backend:                backend,
					AggregatorURL:          server.URL,
					AllowInsecureHTTP:      true,
					VerifyCertificateChain: true,
					GenesisVerificationKey: testKey.value,
				})
				require.EqualError(
					t,
					err,
					"verified Mithril bootstrap requires a genesis verification key",
				)
				assert.Zero(t, requests.Load())
			})
		}
	}
}

func TestBootstrapWithoutVerificationAllowsMissingGenesisVerificationKey(
	t *testing.T,
) {
	for _, backend := range []string{BackendV1, BackendV2} {
		t.Run(backend, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) {
					requests.Add(1)
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte("[]"))
				},
			))
			t.Cleanup(server.Close)

			_, err := Bootstrap(context.Background(), BootstrapConfig{
				Network:                "preview",
				Backend:                backend,
				AggregatorURL:          server.URL,
				AllowInsecureHTTP:      true,
				VerifyCertificateChain: false,
			})
			require.Error(t, err)
			assert.NotContains(
				t,
				err.Error(),
				"genesis verification key",
			)
			assert.Equal(t, int32(1), requests.Load())
		})
	}
}

func TestBootstrapUnknownNetwork(t *testing.T) {
	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Network: "unknown_network",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolving aggregator URL")
}

// TestValidateSnapshotIdentity checks that validateSnapshotIdentity accepts
// known networks with a bounded hex digest and rejects everything else,
// including path-separator/traversal sequences in either field.
func TestValidateSnapshotIdentity(t *testing.T) {
	validDigest := "abc123def4567890abc123def4567890abc123def4567890abc123def4567890"
	tests := []struct {
		name            string
		expectedNetwork string
		network         string
		digest          string
		wantErr         bool
	}{
		{"valid mainnet", "", "mainnet", validDigest, false},
		{"valid preprod", "", "preprod", validDigest, false},
		{"valid preview", "", "preview", validDigest, false},
		{"too-short hex digest", "", "preprod", "abc123", true},
		{"unknown network", "", "devnet", validDigest, true},
		{"empty network", "", "", validDigest, true},
		{"network with path traversal", "", "../../../etc", validDigest, true},
		{"empty digest", "", "preprod", "", true},
		{"digest with slash", "", "preprod", "abc/def", true},
		{"digest with path traversal", "", "preprod", "../../etc/passwd", true},
		{"digest with space", "", "preprod", "abc 123", true},
		{"non-hex digest", "", "preprod", "not-hex-at-all", true},
		{
			"too-long digest",
			"",
			"preprod",
			strings.Repeat("a", 65),
			true,
		},
		{
			"custom network matching operator config is trusted",
			"devnet",
			"devnet",
			validDigest,
			false,
		},
		{
			"custom network not matching operator config is rejected",
			"devnet",
			"some-other-net",
			validDigest,
			true,
		},
		{
			"path traversal rejected even with a custom expected network",
			"devnet",
			"../../../etc",
			validDigest,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSnapshotIdentity(
				tt.expectedNetwork, tt.network, tt.digest,
			)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBootstrapRejectsPathTraversalInSnapshotNetwork verifies that a
// malicious aggregator response with a path-traversal sequence in
// snapshot.Network is rejected by Bootstrap before the archive download
// (and thus any filesystem access derived from that field) is attempted.
func TestBootstrapRejectsPathTraversalInSnapshotNetwork(t *testing.T) {
	var downloadHit atomic.Bool
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:  "a123456789abcdef0a123456789abcdef0a123456789abcdef0a123456789abc",
				Network: "../../../../tmp/evil",
				Size:    203,
				Locations: []string{
					"placeholder",
				},
			},
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc(
		"/artifact/snapshots",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(snapshots))
		},
	)
	mux.HandleFunc(
		"/download/snapshot.tar.zst",
		func(w http.ResponseWriter, r *http.Request) {
			downloadHit.Store(true)
			w.WriteHeader(http.StatusOK)
		},
	)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	snapshots[0].Locations = []string{
		server.URL + "/download/snapshot.tar.zst",
	}

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Backend:           BackendV1,
		AggregatorURL:     server.URL,
		AllowInsecureHTTP: true,
		DownloadDir:       t.TempDir(),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "validating snapshot metadata")
	require.False(
		t,
		downloadHit.Load(),
		"archive download must not be attempted for unvalidated metadata",
	)
}

// TestBootstrapRejectsPathTraversalInSnapshotDigest is the digest-field
// counterpart of TestBootstrapRejectsPathTraversalInSnapshotNetwork:
// snapshot.Digest containing a path-traversal sequence must be rejected
// before it can influence the cache-check or download path.
func TestBootstrapRejectsPathTraversalInSnapshotDigest(t *testing.T) {
	var downloadHit atomic.Bool
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:  "../../../../tmp/evil",
				Network: "preprod",
				Size:    203,
				Locations: []string{
					"placeholder",
				},
			},
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc(
		"/artifact/snapshots",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(snapshots))
		},
	)
	mux.HandleFunc(
		"/download/snapshot.tar.zst",
		func(w http.ResponseWriter, r *http.Request) {
			downloadHit.Store(true)
			w.WriteHeader(http.StatusOK)
		},
	)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	snapshots[0].Locations = []string{
		server.URL + "/download/snapshot.tar.zst",
	}

	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Backend:           BackendV1,
		AggregatorURL:     server.URL,
		AllowInsecureHTTP: true,
		DownloadDir:       t.TempDir(),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "validating snapshot metadata")
	require.False(
		t,
		downloadHit.Load(),
		"archive download must not be attempted for unvalidated metadata",
	)
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
		// Extraction never creates a symlink, so one in the extracted tree
		// is evidence the tree was tampered with rather than produced by
		// this node. Reporting the snapshot as absent re-extracts it from
		// the verified archive instead of reading the chain through a path
		// someone else chose.
		{
			name: "symlinked immutable dir",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				outside := t.TempDir()
				require.NoError(t, os.WriteFile(
					filepath.Join(outside, "00000.chunk"),
					[]byte("data"),
					0o640,
				))
				requireSymlinkSupport(
					t, outside, filepath.Join(baseDir, "immutable"),
				)
			},
			expected: "",
		},
		{
			name: "symlinked intermediate component",
			setup: func(t *testing.T, baseDir string) {
				t.Helper()
				outside := t.TempDir()
				dir := filepath.Join(outside, "immutable")
				require.NoError(t, os.MkdirAll(dir, 0o750))
				require.NoError(t, os.WriteFile(
					filepath.Join(dir, "00000.chunk"),
					[]byte("data"),
					0o640,
				))
				requireSymlinkSupport(
					t, outside, filepath.Join(baseDir, "db"),
				)
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir := t.TempDir()
			tt.setup(t, baseDir)

			result := findImmutableDir(baseDir)
			t.Cleanup(result.Close)
			switch tt.expected {
			case "":
				require.Nil(t, result)
			case "ROOT":
				require.Equal(t, baseDir, result.Path())
			default:
				expected := filepath.Join(baseDir, tt.expected)
				require.Equal(t, expected, result.Path())
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

	client := NewClient(server.URL, WithAllowInsecureHTTP())
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

	client := NewClient(server.URL, WithAllowInsecureHTTP())
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
					"current_epoch": "269",
					"next_aggregate_verification_key": hex.EncodeToString(
						g2.Marshal(),
					),
					"next_protocol_parameters": ProtocolParameters{
						K:    1,
						M:    2,
						PhiF: 0.5,
					}.ComputeHash(),
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

	client := NewClient(server.URL, WithAllowInsecureHTTP())
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
	genesisVerificationKey, genesisPrivateKey := testGenesisKeyPair(t)
	_, _, _, g2 := bls12381.Generators()
	g2Hex := hex.EncodeToString(g2.Marshal())
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:          "e123456789abcdef0e123456789abcdef0e123456789abcdef0e123456789abc",
				Network:         "preprod",
				CertificateHash: "cert_leaf",
				Locations: []string{
					"https://example.com/snapshot.tar.zst",
				},
				Beacon: Beacon{
					Epoch:               270,
					ImmutableFileNumber: 5320,
				},
			},
		},
	}
	certs := map[string]Certificate{
		"cert_leaf": {
			Epoch: 270,
			SignedEntityType: SignedEntityType{
				raw: json.RawMessage(
					`{"MithrilStakeDistribution":{"epoch":270,"immutable_file_number":5320}}`,
				),
			},
			Metadata: CertificateMetadata{
				Network:     "preprod",
				Parameters:  ProtocolParameters{K: 1, M: 1, PhiF: 1.0},
				InitiatedAt: "2026-02-10T00:00:00Z",
				SealedAt:    "2026-02-10T00:01:00Z",
				Signers: []StakeDistributionParty{
					{PartyID: "pool1abc123", Stake: 42},
				},
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"snapshot_digest": "e123456789abcdef0e123456789abcdef0e123456789abcdef0e123456789abc",
					"current_epoch":   "270",
				},
			},
		},
		"cert_genesis": {
			Epoch: 269,
			Metadata: CertificateMetadata{
				Parameters:  ProtocolParameters{K: 1, M: 1, PhiF: 1.0},
				InitiatedAt: "2026-02-09T00:00:00Z",
				SealedAt:    "2026-02-09T00:01:00Z",
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"current_epoch": "269",
					"next_protocol_parameters": ProtocolParameters{
						K:    1,
						M:    1,
						PhiF: 1.0,
					}.ComputeHash(),
				},
			},
		},
	}
	leaf := certs["cert_leaf"]
	leaf.SignedMessage = leaf.ProtocolMessage.ComputeHash()
	leaf.AggregateVerificationKey, leaf.MultiSignature =
		testCreateEncodedSTMProof(t, []byte(leaf.SignedMessage))
	genesis := certs["cert_genesis"]
	genesis.ProtocolMessage.
		MessageParts["next_aggregate_verification_key"] =
		leaf.AggregateVerificationKey
	signTestGenesisCertificate(t, &genesis, genesisPrivateKey)
	certs["cert_genesis"] = genesis
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
		Backend:                BackendV1,
		AggregatorURL:          server.URL,
		AllowInsecureHTTP:      true,
		VerifyCertificateChain: true,
		GenesisVerificationKey: genesisVerificationKey,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected signed entity kind")
}

// TestFindImmutableDirRefusesSymlinkedExtractDir covers the extraction
// directory itself being a symlink.
//
// The per-candidate checks below it never see this: the fast path asks whether
// the extraction directory holds chunk files directly, and following a symlink
// there reports a cached snapshot that this node never extracted. Bootstrap
// then skips extraction entirely and loads the chain from the link's target,
// which is the outcome the symlink refusal exists to prevent.
func TestFindImmutableDirRefusesSymlinkedExtractDir(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(outside, "00000.chunk"), []byte("theirs"), 0o640,
	))

	extractDir := filepath.Join(root, "immutable-abc123")
	requireSymlinkSupport(t, "outside", extractDir)

	assert.Nil(t, findImmutableDir(extractDir),
		"a symlinked extraction directory is not a cached snapshot")
}

// TestFindImmutableDirRefusesSwappedExtractDir covers the extraction directory
// being replaced after it has been opened and vetted.
//
// The lookup ends by handing back a pathname, and whoever loads the chain
// resolves that name again — a handle cannot be carried across that boundary,
// because the immutable DB is opened by name. So a directory swapped in behind
// the name is read in place of the tree that was inspected, unless the name is
// confirmed to still denote that tree before it is returned.
//
// The layout below is only reachable through the top-level enumeration, which
// is the second half of the same problem: enumerating the pathname lists the
// replacement's entries, and a name taken from there is checked against the
// tree that was opened while resolving into the replacement.
func TestFindImmutableDirRefusesSwappedExtractDir(t *testing.T) {
	parent := t.TempDir()
	extractDir := filepath.Join(parent, "immutable-abc123")
	ours := filepath.Join(extractDir, "snapshot-data", "immutable")
	require.NoError(t, os.MkdirAll(ours, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(ours, "00000.chunk"), []byte("ours"), 0o640,
	))

	root, err := openVerifiedDir(extractDir)
	require.NoError(t, err)

	// A writer with access to the download directory puts a tree of their own
	// under the same name and layout, as one could between the open and the
	// read.
	theirs := filepath.Join(parent, "theirs", "snapshot-data", "immutable")
	require.NoError(t, os.MkdirAll(theirs, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(theirs, "00000.chunk"), []byte("theirs"), 0o640,
	))
	requireDirectorySwap(t, extractDir, filepath.Join(parent, "moved-aside"))
	requireDirectorySwap(t, filepath.Join(parent, "theirs"), extractDir)

	assert.Nil(t, findImmutableDirIn(root, extractDir),
		"a swapped extraction directory is not the tree that was inspected")
}

// TestChunkDirUnderReturnsVerifiedPath pins the v2 handoff: the path is
// produced by the lookup that verified it rather than assembled by the caller,
// so the two cannot disagree.
func TestChunkDirUnderReturnsVerifiedPath(t *testing.T) {
	extractDir := t.TempDir()
	immutable := filepath.Join(extractDir, "immutable")
	require.NoError(t, os.MkdirAll(immutable, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(immutable, "00000.chunk"), []byte("data"), 0o640,
	))

	found := chunkDirUnder(extractDir, "immutable")
	require.NotNil(t, found)
	t.Cleanup(found.Close)
	assert.Equal(t, immutable, found.Path())
}

// TestChunkDirUnderRefusesSymlinkedBase keeps the v2 lookup on the same footing
// as the v1 one: the extraction directory is derived inside the download
// directory, so a symlink there is planted content rather than a layout choice.
func TestChunkDirUnderRefusesSymlinkedBase(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(root, "outside", "immutable")
	require.NoError(t, os.MkdirAll(outside, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(outside, "00000.chunk"), []byte("theirs"), 0o640,
	))

	extractDir := filepath.Join(root, "immutable-abc123")
	requireSymlinkSupport(t, "outside", extractDir)

	assert.Nil(t, chunkDirUnder(extractDir, "immutable"),
		"a symlinked extraction directory holds no cached snapshot")
}

// hasChunkFiles reports whether dir holds chunk files.
//
// Test-only. The production lookups resolve a candidate through a handle on
// its parent, because there the directory is derived inside the download
// directory and may have been tampered with. These assertions inspect a
// directory the test itself created, so a plain read says what they mean.
func hasChunkFiles(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	return holdsChunkFile(entries)
}

// findImmutableDir and chunkDirUnder open the extraction directory, vet it, and
// run the lookup in one call.
//
// Test-only. Production holds a single vetted handle on the extraction
// directory — the ledger-state fallback reads through the same one — and calls
// findImmutableDirIn / chunkDirIn with it, so that the ImmutableDB that was
// accepted and the ledger state that gets imported cannot come from two
// different resolutions of that directory's name. These wrappers keep the tests
// that only care about the lookup itself from having to stage that.

// findImmutableDir looks for the ImmutableDB directory in the
// extracted archive. It checks several common layouts:
//   - extractDir itself (contains .chunk files)
//   - extractDir/immutable/
//   - extractDir/db/immutable/
//   - any single top-level dir containing immutable/
//
// A candidate reached through a symlink is not accepted. Extraction never
// creates one, so a symlink in the extracted tree is evidence the tree was
// tampered with rather than produced by this node, and accepting it would load
// the chain from a directory somebody else chose. Reporting the snapshot as
// absent instead re-extracts it from the verified archive, which discards the
// tampered tree.
//
// The same holds for a tree substituted while the lookup runs: the handle the
// tree was inspected through is what is handed back, so the snapshot that gets
// loaded is the one that was inspected and not whatever later holds its name.
// The caller must Close the result.
func findImmutableDir(extractDir string) *vettedDir {
	// The extraction directory is checked before it is read, not after. It is
	// derived inside the download directory rather than chosen by the
	// operator, so a symlink there is planted content like anything else
	// below it — and asking whether it holds chunk files by pathname would
	// follow it and report a cached snapshot this node never extracted.
	root, err := openVerifiedDir(extractDir)
	if err != nil {
		return nil
	}
	defer root.Close()
	return findImmutableDirIn(root, extractDir)
}

// chunkDirUnder returns rel beneath base when rel is a directory holding chunk
// files, with both base and rel verified rather than only the last one, and nil
// when it is not. The caller must Close the result.
//
// Verifying only the last component is enough when the one above it is the
// operator's. Where that one is itself derived content — the extraction
// directory holding `immutable`, say — it has to be vetted too, or a symlink
// one level up carries the whole lookup.
//
// The directory is returned by the lookup that inspected it, as the handle it
// was inspected through, rather than as a name the caller reassembles. A caller
// that builds the name itself is naming whatever occupies it now, not what was
// verified a moment ago — and so is a caller handed only a name.
func chunkDirUnder(base, rel string) *vettedDir {
	baseRoot, err := openVerifiedDir(base)
	if err != nil {
		return nil
	}
	defer baseRoot.Close()
	return chunkDirIn(baseRoot, base, rel)
}

// TestBootstrapRefusesADigestThatNamesSomewhereElse covers the one piece of
// aggregator-supplied text that becomes a directory name.
//
// v1 derives `immutable-<digest>` and `ancillary-<digest>` inside the download
// directory, and `Cleanup` calls os.RemoveAll on both. The digest is the
// aggregator's string. Joining it raw does not stay inside: a digest beginning
// with a separator makes the "immutable-" prefix its own path element, and the
// ".." that follows pops it, so the first ".." buys nothing and every one after
// it climbs a level. The depth is the digest's to choose — "/../.." reaches the
// download directory's parent, "/../../.." its grandparent — and whatever it
// reaches is what gets extracted into and then removed.
//
// v2 is closed by a different check: it refuses an artifact whose hash is not
// the one it computes, and a computed hash is hex. v1 has no computed hash to
// compare against, so the constraint has to be stated.
//
// Two checks state it, and this test asserts the outcome rather than which one
// spoke. validateSnapshotIdentity constrains the digest to 64 hex characters
// and runs first; validateSnapshotDigest, immediately after it, refuses
// anything that is not a single path element. The second is narrower than the
// first and so never fires on the inputs below — it is what still holds if the
// format rule is ever loosened for a private aggregator, and it is exercised
// directly in TestValidateSnapshotDigest. What matters here is the property
// both exist for: the refusal lands before the digest names anything.
func TestBootstrapRefusesADigestThatNamesSomewhereElse(t *testing.T) {
	archiveData := createChunkArchive(t)

	for _, digest := range []string{
		// One level up from the download directory, then two, then a named
		// entry beside it. Nesting the download directory two deep below the
		// test root is what keeps the deepest of these observable — an escape
		// that climbed past the root would land in the system temp directory,
		// where this test could assert nothing about it.
		"/../..",
		"/../../..",
		"/../../pwned",
		// Neither of these escapes; both are still refused, because a digest
		// that is not one path element is not a digest.
		"a/b",
		"..",
	} {
		t.Run(digest, func(t *testing.T) {
			snapshots := []SnapshotListItem{{SnapshotBase: SnapshotBase{
				Digest:  digest,
				Network: "preprod",
				Beacon: Beacon{
					Epoch:               270,
					ImmutableFileNumber: 5320,
				},
				Size:                 int64(len(archiveData)),
				CompressionAlgorithm: "zstandard",
			}}}
			mux := http.NewServeMux()
			mux.HandleFunc(
				"/artifact/snapshots",
				func(w http.ResponseWriter, r *http.Request) {
					_ = json.NewEncoder(w).Encode(snapshots)
				},
			)
			mux.HandleFunc(
				"/download/snapshot.tar.zst",
				func(w http.ResponseWriter, r *http.Request) {
					_, _ = w.Write(archiveData)
				},
			)
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)
			snapshots[0].Locations = []string{
				server.URL + "/download/snapshot.tar.zst",
			}

			// Two levels of nesting below the root, so both the parent and
			// the grandparent of the download directory are somewhere this
			// test can look afterwards.
			root := t.TempDir()
			nested := filepath.Join(root, "nested")
			downloadDir := filepath.Join(nested, "downloads")
			require.NoError(t, os.MkdirAll(downloadDir, 0o750))

			_, err := Bootstrap(context.Background(), BootstrapConfig{
				AllowInsecureHTTP: true,
				Network:           "preprod",
				Backend:           BackendV1,
				AggregatorURL:     server.URL,
				DownloadDir:       downloadDir,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), digest,
				"the refusal must name the digest it refused")

			// Refused before anything is derived from it, so every level the
			// digest could have named is untouched — including the two above
			// the download directory, which is where extraction would have
			// gone and where Cleanup would then have pointed os.RemoveAll.
			for dir, want := range map[string]string{
				root:   "nested",
				nested: "downloads",
			} {
				entries, readErr := os.ReadDir(dir)
				require.NoError(t, readErr)
				require.Len(t, entries, 1,
					"nothing may be created above the download directory, "+
						"but %s gained an entry", dir)
				assert.Equal(t, want, entries[0].Name())
			}
			inside, readErr := os.ReadDir(downloadDir)
			require.NoError(t, readErr)
			assert.Empty(t, inside,
				"a refused digest must not reach the download either")
		})
	}
}

// TestValidateSnapshotDigest exercises the path-safety guard on its own.
//
// Reached directly because the format check ahead of it in Bootstrap already
// rejects everything here — going through Bootstrap would assert the format
// rule and say nothing about this one. The guard is the constraint that
// survives a loosened format rule, so it is worth holding to independently.
func TestValidateSnapshotDigest(t *testing.T) {
	for _, digest := range []string{
		"",
		"/../..",
		"/../../..",
		"/../../pwned",
		"a/b",
		"..",
		".",
		`a\b`,
	} {
		t.Run(digest, func(t *testing.T) {
			assert.ErrorIs(t,
				validateSnapshotDigest(digest), ErrUnsafeSnapshotDigest,
			)
		})
	}
	assert.NoError(t, validateSnapshotDigest(
		"a123456789abcdef0a123456789abcdef0a123456789abcdef0a123456789abc",
	))
}

// TestBootstrapSurvivesAnUnavailableAncillaryArchive covers the non-verified
// fallback: ancillary locations are advertised, every one of them fails, and
// the bootstrap has to complete without a ledger state rather than fail.
//
// Both backends make this failure explicitly non-fatal unless certificate
// verification is on, so the result they build carries no ancillary tree — and
// every use of that tree between the failure and the return has to tolerate its
// absence. `vettedDir`'s methods are nil-safe for exactly this reason: Path
// returns "", Root returns nil, Close does nothing. A nil `*vettedDir` is the
// representation of "there is no ancillary tree", not an error state, so the
// bootstrap reports it by carrying the nil rather than by branching around it.
//
// The failure is staged as an unusable archive rather than absent locations.
// Absent locations skip the download entirely, which does not exercise the
// branch that has to decide whether an ancillary error is fatal.
func TestBootstrapSurvivesAnUnavailableAncillaryArchive(t *testing.T) {
	assertNoAncillary := func(t *testing.T, result *BootstrapResult) {
		t.Helper()
		assert.Nil(t, result.AncillaryRoot,
			"a failed ancillary download must leave no handle")
		assert.Empty(t, result.AncillaryDir,
			"nor a directory name for one")
		assert.False(t, result.AncillaryVerified)
		// The half that has to survive: the immutable tree is what the load
		// reads, and it is unaffected by the ancillary failure.
		require.NotNil(t, result.ImmutableRoot)
		assert.True(t, hasChunkFiles(result.ImmutableDir))
	}

	t.Run("v1", func(t *testing.T) {
		archiveData := createChunkArchive(t)
		snapshots := []SnapshotListItem{{SnapshotBase: SnapshotBase{
			Digest:               "b123456789abcdef0b123456789abcdef0b123456789abcdef0b123456789abc",
			Network:              "preprod",
			Beacon:               Beacon{Epoch: 270, ImmutableFileNumber: 5320},
			Size:                 int64(len(archiveData)),
			CompressionAlgorithm: "zstandard",
		}}}
		mux := http.NewServeMux()
		mux.HandleFunc(
			"/artifact/snapshots",
			func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(snapshots)
			},
		)
		mux.HandleFunc(
			"/download/snapshot.tar.zst",
			func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write(archiveData)
			},
		)
		// Downloads cleanly and then fails to extract, so the failure lands
		// after the goroutine has committed to the ancillary path.
		mux.HandleFunc(
			"/download/ancillary.tar.zst",
			func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("not an archive"))
			},
		)
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)
		snapshots[0].Locations = []string{
			server.URL + "/download/snapshot.tar.zst",
		}
		snapshots[0].AncillaryLocations = []string{
			server.URL + "/download/ancillary.tar.zst",
		}

		result, err := Bootstrap(context.Background(), BootstrapConfig{
			AllowInsecureHTTP: true,
			Network:           "preprod",
			Backend:           BackendV1,
			AggregatorURL:     server.URL,
			DownloadDir:       t.TempDir(),
		})
		require.NoError(t, err,
			"a failed ancillary download is non-fatal without verification")
		t.Cleanup(result.CloseHandles)
		assertNoAncillary(t, result)
	})

	t.Run("v2", func(t *testing.T) {
		fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 1})
		// Served at request time from the field, so replacing it here makes
		// the download succeed and the extraction fail.
		fixture.ancillaryArchive = []byte("not an archive")

		cfg := fixture.bootstrapConfig(t.TempDir())
		cfg.VerifyCertificateChain = false
		cfg.AncillaryVerificationKey = ""

		result, err := Bootstrap(context.Background(), cfg)
		require.NoError(t, err,
			"a failed ancillary download is non-fatal without verification")
		t.Cleanup(result.CloseHandles)
		assertNoAncillary(t, result)
	})

	// Completing the bootstrap is only half of what the non-fatal branch
	// promises. The other half is that the ledger state then comes from the
	// main extraction, which is where v1-layout snapshots keep it — so this
	// runs the whole of Sync rather than stopping at the result.
	t.Run("v2 falls back to the extraction directory's ledger state",
		func(t *testing.T) {
			fixture := newV2Fixture(t, v2FixtureOptions{
				validImmutable:      true,
				fallbackLedgerState: true,
			})
			fixture.ancillaryArchive = []byte("not an archive")

			result, err := Sync(context.Background(), SyncConfig{
				Network:       "preprod",
				DataDir:       t.TempDir(),
				StorageMode:   "core",
				Backend:       BackendV2,
				AggregatorURL: fixture.server.URL,
				// The fixture serves over plain HTTP, so opt into the
				// insecure-transport escape hatch the same way the other
				// httptest-backed Sync tests do.
				AllowInsecureHTTP: true,
				VerifyCertChain:   false,
			})
			require.NoError(t, err)
			assert.Equal(t, uint64(1000), result.LedgerSlot,
				"the ledger state must come from the extraction directory "+
					"when the ancillary archive is unusable")
		})
}
