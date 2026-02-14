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

package ledgerstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/mithril"
	"github.com/stretchr/testify/require"
)

func TestVerifyCertificateChainGenesis(t *testing.T) {
	// Create a test server that serves a genesis certificate
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				cert := mithril.Certificate{
					Hash:             "genesis_hash",
					PreviousHash:     "genesis_hash",
					Epoch:            0,
					GenesisSignature: "sig123",
				}
				w.Header().
					Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(cert); err != nil {
					t.Errorf("failed to encode cert: %v", err)
				}
			},
		),
	)
	t.Cleanup(server.Close)

	client := mithril.NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"genesis_hash",
		"",
	)
	require.NoError(t, err)
}

func TestVerifyCertificateChainTwoDeep(t *testing.T) {
	certs := map[string]mithril.Certificate{
		"cert_a": {
			Hash:           "cert_a",
			PreviousHash:   "cert_genesis",
			Epoch:          5,
			MultiSignature: "multisig",
			SignedMessage:  "msg",
		},
		"cert_genesis": {
			Hash:             "cert_genesis",
			PreviousHash:     "cert_genesis",
			Epoch:            0,
			GenesisSignature: "genesis_sig",
		},
	}

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				// Extract hash from URL path
				path := r.URL.Path
				hash := path[len("/certificate/"):]
				cert, ok := certs[hash]
				if !ok {
					http.NotFound(w, r)
					return
				}
				w.Header().
					Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(cert); err != nil {
					t.Errorf("failed to encode cert: %v", err)
				}
			},
		),
	)
	t.Cleanup(server.Close)

	client := mithril.NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"cert_a",
		"",
	)
	require.NoError(t, err)
}

func TestVerifyCertificateChainCycleDetection(t *testing.T) {
	certs := map[string]mithril.Certificate{
		"cert_a": {
			Hash:           "cert_a",
			PreviousHash:   "cert_b",
			MultiSignature: "sig",
		},
		"cert_b": {
			Hash:           "cert_b",
			PreviousHash:   "cert_a",
			MultiSignature: "sig",
		},
	}

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				hash := r.URL.Path[len("/certificate/"):]
				cert, ok := certs[hash]
				if !ok {
					http.NotFound(w, r)
					return
				}
				w.Header().
					Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(cert); err != nil {
					t.Errorf("failed to encode cert: %v", err)
				}
			},
		),
	)
	t.Cleanup(server.Close)

	client := mithril.NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"cert_a",
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cycle detected")
}

func TestVerifyCertificateChainNilClient(t *testing.T) {
	err := VerifyCertificateChain(
		context.Background(),
		nil,
		"some_hash",
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "client is nil")
}

func TestVerifyCertificateChainEmptyHash(t *testing.T) {
	client := mithril.NewClient("http://example.com")
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"",
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

func TestVerifySnapshotDigest(t *testing.T) {
	content := []byte("test snapshot content for hashing")
	h := sha256.Sum256(content)
	expectedDigest := hex.EncodeToString(h[:])

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, content, 0o640)
	require.NoError(t, err)

	err = VerifySnapshotDigest(archivePath, expectedDigest)
	require.NoError(t, err)
}

func TestVerifySnapshotDigestMismatch(t *testing.T) {
	content := []byte("test snapshot content")

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, content, 0o640)
	require.NoError(t, err)

	err = VerifySnapshotDigest(
		archivePath,
		"0000000000000000000000000000000000000000000000000000000000000000",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch")
}

func TestVerifyChecksumFileNoChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// No .checksum file - should succeed (not an error)
	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}

func TestVerifyChecksumFileEmptyChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(checksumPath, []byte("  \n"), 0o640)
	require.NoError(t, err)

	// Empty checksum - should succeed (skip verification)
	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}

func TestVerifyChecksumFileMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("00000000"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch")
}

func TestVerifyChecksumFileValid(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	content := []byte("test data for crc32")
	err := os.WriteFile(lstatePath, content, 0o640)
	require.NoError(t, err)

	// Compute the actual CRC32 for the content
	h := crc32.NewIEEE()
	_, err = h.Write(content)
	require.NoError(t, err)
	checksum := fmt.Sprintf("%08x", h.Sum32())

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte(checksum),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}
