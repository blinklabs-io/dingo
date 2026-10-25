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
	"log/slog"
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

func TestBootstrapRedactsCredentialAggregatorURLFromLogs(t *testing.T) {
	t.Parallel()

	handler := &captureSlogHandler{}
	_, err := Bootstrap(context.Background(), BootstrapConfig{
		Backend: "unsupported",
		AggregatorURL: "https://operator:super-secret@aggregator.example/" +
			"?token=query-secret",
		Logger: slog.New(handler),
	})
	require.Error(t, err)

	found := false
	for _, record := range handler.records {
		if record.Message != "starting Mithril bootstrap" {
			continue
		}
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key != "aggregator" {
				return true
			}
			found = true
			assert.Equal(t, "https://aggregator.example/", attr.Value.String())
			return true
		})
	}
	require.True(t, found, "bootstrap log is missing aggregator URL")
}

func TestBootstrapInvalidGenesisVerificationKey(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	for _, backend := range []string{BackendV2} {
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
	t.Parallel()

	for _, backend := range []string{BackendV2} {
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
	t.Parallel()

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
	t.Parallel()

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

func TestBootstrapResultCleanup(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	extractDir := filepath.Join(tmpDir, "extract")
	require.NoError(t, os.MkdirAll(extractDir, 0o750))

	ancillaryDir := filepath.Join(tmpDir, "ancillary")
	require.NoError(t, os.MkdirAll(ancillaryDir, 0o750))
	extractRoot, err := os.OpenRoot(extractDir)
	require.NoError(t, err)
	ancillaryRoot, err := os.OpenRoot(ancillaryDir)
	require.NoError(t, err)

	result := &BootstrapResult{
		ExtractDir:    extractDir,
		ExtractRoot:   extractRoot,
		AncillaryDir:  ancillaryDir,
		AncillaryRoot: ancillaryRoot,
	}

	result.Cleanup(nil)

	// Individual paths should be removed
	_, err = os.Stat(extractDir)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(ancillaryDir)
	require.True(t, os.IsNotExist(err))

	// Parent directory should NOT be removed
	_, err = os.Stat(tmpDir)
	require.NoError(t, err)
}

func TestBootstrapResultCleanupRemovesTempDir(t *testing.T) {
	t.Parallel()

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
	tempRoot, err := os.OpenRoot(autoTempDir)
	require.NoError(t, err)

	result := &BootstrapResult{
		TempDir:  autoTempDir,
		TempRoot: tempRoot,
	}
	result.Cleanup(nil)

	_, err = os.Stat(autoTempDir)
	require.True(t, os.IsNotExist(err))
}

func TestBootstrapResultCleanupKeepsSubstitutedDirectory(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	tempDir := filepath.Join(parent, "auto-temp")
	require.NoError(t, os.Mkdir(tempDir, 0o700))
	tempRoot, err := os.OpenRoot(tempDir)
	require.NoError(t, err)
	extractDir := filepath.Join(tempDir, "extract")
	require.NoError(t, os.Mkdir(extractDir, 0o750))
	require.NoError(t, os.WriteFile(
		filepath.Join(extractDir, "ours"), []byte("ours"), 0o600,
	))
	extractRoot, err := os.OpenRoot(extractDir)
	require.NoError(t, err)

	movedDir := filepath.Join(tempDir, "moved")
	require.NoError(t, os.Rename(extractDir, movedDir))
	require.NoError(t, os.Mkdir(extractDir, 0o750))
	replacement := filepath.Join(extractDir, "replacement")
	require.NoError(t, os.WriteFile(replacement, []byte("keep"), 0o600))

	result := &BootstrapResult{
		ExtractDir:  extractDir,
		ExtractRoot: extractRoot,
		TempDir:     tempDir,
		TempRoot:    tempRoot,
	}
	result.Cleanup(slog.New(slog.DiscardHandler))

	data, err := os.ReadFile(replacement)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
	_, err = os.Stat(filepath.Join(movedDir, "ours"))
	require.True(t, os.IsNotExist(err))
}

func TestBootstrapResultCleanupKeepsSubstitutedArchive(t *testing.T) {
	t.Parallel()
	testBootstrapResultArchiveProtection(t)
}

func TestBootstrapResultCleanupKeepsSubstitutedTempDirectory(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	tempDir := filepath.Join(parent, "auto-temp")
	require.NoError(t, os.Mkdir(tempDir, 0o700))
	tempRoot, err := os.OpenRoot(tempDir)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(
		filepath.Join(tempDir, "ours"), []byte("ours"), 0o600,
	))

	movedDir := filepath.Join(parent, "moved")
	require.NoError(t, os.Rename(tempDir, movedDir))
	require.NoError(t, os.Mkdir(tempDir, 0o700))
	replacement := filepath.Join(tempDir, "replacement")
	require.NoError(t, os.WriteFile(replacement, []byte("keep"), 0o600))

	result := &BootstrapResult{TempDir: tempDir, TempRoot: tempRoot}
	result.Cleanup(slog.New(slog.DiscardHandler))

	data, err := os.ReadFile(replacement)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
	_, err = os.Stat(filepath.Join(movedDir, "ours"))
	require.True(t, os.IsNotExist(err))
}

func TestBootstrapTempFailureCleanupKeepsSubstitutedDirectory(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	tempDir := filepath.Join(parent, "auto-temp")
	require.NoError(t, os.Mkdir(tempDir, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(tempDir, "partial"), []byte("partial"), 0o600,
	))
	tempRoot, err := os.OpenRoot(tempDir)
	require.NoError(t, err)

	movedDir := filepath.Join(parent, "moved")
	require.NoError(t, os.Rename(tempDir, movedDir))
	require.NoError(t, os.Mkdir(tempDir, 0o700))
	replacement := filepath.Join(tempDir, "replacement")
	require.NoError(t, os.WriteFile(replacement, []byte("keep"), 0o600))

	err = cleanupBootstrapDir(&tempRoot, tempDir, nil)
	require.ErrorIs(t, err, errBootstrapCleanupIdentity)
	require.Nil(t, tempRoot)
	data, err := os.ReadFile(replacement)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
	_, err = os.Stat(filepath.Join(movedDir, "partial"))
	require.True(t, os.IsNotExist(err))
}

func TestCleanupBootstrapDirClearsClosedRoot(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	require.NoError(t, root.Close())

	err = cleanupBootstrapDir(&root, dir, nil)
	require.Error(t, err)
	require.Nil(t, root)
}

func TestCleanupRetainedDirectoriesRefuseFinalFileSubstitution(t *testing.T) {
	t.Parallel()

	for _, cleanupKind := range []string{"owned", "vetted"} {
		t.Run(cleanupKind, func(t *testing.T) {
			t.Parallel()
			parentPath := t.TempDir()
			name := "cleanup"
			path := filepath.Join(parentPath, name)
			require.NoError(t, os.Mkdir(path, 0o700))
			root, err := os.OpenRoot(path)
			require.NoError(t, err)
			info, err := root.Stat(".")
			require.NoError(t, err)

			remove := func(parent *os.Root, name, path string) error {
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
				return removeEmptyExtractDir(parent, name, path)
			}

			var cleanupErr error
			if cleanupKind == "owned" {
				parent, openErr := os.OpenRoot(parentPath)
				require.NoError(t, openErr)
				t.Cleanup(func() { _ = parent.Close() })
				cleanupErr = cleanupRetainedDirKeepingWithRemover(
					parent, &root, info, name, path, nil, nil, remove,
				)
			} else {
				cleanupErr = cleanupBootstrapDirWithRemover(
					&root, path, nil, remove,
				)
			}
			require.Error(t, cleanupErr)
			data, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			require.Equal(t, []byte("keep"), data)
		})
	}
}

func TestCleanupRetainedDirectoriesAllowFinalDisappearance(t *testing.T) {
	t.Parallel()

	for _, cleanupKind := range []string{"owned", "vetted"} {
		t.Run(cleanupKind, func(t *testing.T) {
			t.Parallel()
			parentPath := t.TempDir()
			name := "cleanup"
			path := filepath.Join(parentPath, name)
			require.NoError(t, os.Mkdir(path, 0o700))
			root, err := os.OpenRoot(path)
			require.NoError(t, err)
			info, err := root.Stat(".")
			require.NoError(t, err)
			remove := func(parent *os.Root, name, path string) error {
				require.NoError(t, os.Remove(path))
				return removeEmptyExtractDir(parent, name, path)
			}

			if cleanupKind == "owned" {
				parent, openErr := os.OpenRoot(parentPath)
				require.NoError(t, openErr)
				t.Cleanup(func() { _ = parent.Close() })
				require.NoError(t, cleanupRetainedDirKeepingWithRemover(
					parent, &root, info, name, path, nil, nil, remove,
				))
			} else {
				require.NoError(t, cleanupBootstrapDirWithRemover(
					&root, path, nil, remove,
				))
			}
		})
	}
}

func TestRemoveRetainedDirectoryShellAllowsPriorDisappearance(t *testing.T) {
	t.Parallel()
	parentPath := t.TempDir()
	name := "cleanup"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	info, err := os.Lstat(path)
	require.NoError(t, err)
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parent.Close() })
	require.NoError(t, os.Remove(path))
	require.NoError(t, removeRetainedDirShell(
		parent, info, name, path, removeEmptyExtractDir,
	))
}

func TestCleanupCreatedBootstrapDirectoryRefusesFinalFileSubstitution(t *testing.T) {
	t.Parallel()
	parentPath := t.TempDir()
	name := "created"
	path := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(path, 0o700))
	created, err := os.Lstat(path)
	require.NoError(t, err)
	parent, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parent.Close() })

	err = cleanupCreatedBootstrapDirWithRemover(
		parent,
		name,
		path,
		created,
		func(parent *os.Root, name, path string) error {
			require.NoError(t, os.Remove(path))
			require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
			return removeEmptyExtractDir(parent, name, path)
		},
	)
	require.Error(t, err)
	data, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, []byte("keep"), data)
}

func TestBootstrapDirectoryCloseReleasesHandlesWithoutRemoving(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	path, owned, err := createBootstrapDir(parent, "persistent-*")
	require.NoError(t, err)

	require.NoError(t, owned.close())
	require.Nil(t, owned.root)
	require.Nil(t, owned.parent)
	require.Nil(t, owned.childLock)
	require.Nil(t, owned.parentLock)
	require.DirExists(t, path)

	moved := path + "-moved"
	require.NoError(t, os.Rename(path, moved), "all lifetime locks must be released")
}

func TestVerifyCertificateChainAllowsDeepChains(t *testing.T) {
	t.Parallel()

	const chainDepth = 150
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
	)
	require.NoError(t, err)
}

func TestVerifyCertificateChainWithModeSTMRejectsContentHashMismatch(
	t *testing.T,
) {
	t.Parallel()

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
		VerificationModeSTM,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "content hash mismatch")
}

// TestChunkDirUnderReturnsVerifiedPath pins the v2 handoff: the path is
// produced by the lookup that verified it rather than assembled by the caller,
// so the two cannot disagree.
func TestChunkDirUnderReturnsVerifiedPath(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

// TestBootstrapRejectsAnUnavailableAncillaryArchive verifies that a v2
// bootstrap cannot succeed without the only supported ledger-state source.
func TestBootstrapRejectsAnUnavailableAncillaryArchive(t *testing.T) {
	t.Parallel()

	t.Run("v2", func(t *testing.T) {
		fixture := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 1})
		// Served at request time from the field, so replacing it here makes
		// the download succeed and the extraction fail.
		fixture.ancillaryArchive = []byte("not an archive")

		cfg := fixture.bootstrapConfig(t.TempDir())
		cfg.VerifyCertificateChain = false
		cfg.AncillaryVerificationKey = ""

		result, err := Bootstrap(context.Background(), cfg)
		require.ErrorContains(t, err, "mithril ancillary data unavailable")
		assert.Nil(t, result)
	})
}
