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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyCertChainGenesis(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				cert := Certificate{
					Hash:             "genesis",
					PreviousHash:     "genesis",
					GenesisSignature: "sig",
				}
				w.Header().
					Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(cert); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			},
		),
	)
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"genesis",
		"",
	)
	require.NoError(t, err)
}

func TestVerifyCertChainThreeDeep(t *testing.T) {
	certs := map[string]Certificate{
		"leaf": {
			Hash:           "leaf",
			PreviousHash:   "middle",
			MultiSignature: "sig",
		},
		"middle": {
			Hash:           "middle",
			PreviousHash:   "root",
			MultiSignature: "sig",
		},
		"root": {
			Hash:             "root",
			PreviousHash:     "root",
			GenesisSignature: "genesis_sig",
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
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			},
		),
	)
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"leaf",
		"",
	)
	require.NoError(t, err)
}

func TestVerifyCertChainMissingCert(t *testing.T) {
	certs := map[string]Certificate{
		"leaf": {
			Hash:           "leaf",
			PreviousHash:   "missing_cert",
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
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			},
		),
	)
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"leaf",
		"",
	)
	require.Error(t, err)
	require.Contains(
		t,
		err.Error(),
		"fetching certificate missing_cert",
	)
}

func TestVerifyCertChainEmptyPreviousHash(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				cert := Certificate{
					Hash:           "orphan",
					PreviousHash:   "",
					MultiSignature: "sig",
				}
				w.Header().
					Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(cert); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			},
		),
	)
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"orphan",
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty previous_hash")
}

func TestVerifyCertChainDigestMismatch(t *testing.T) {
	certs := map[string]Certificate{
		"leaf": {
			Hash:           "leaf",
			PreviousHash:   "root",
			MultiSignature: "sig",
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"snapshot_digest": "wrong_digest",
				},
			},
		},
		"root": {
			Hash:             "root",
			PreviousHash:     "root",
			GenesisSignature: "genesis_sig",
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
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			},
		),
	)
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"leaf",
		"expected_digest",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_digest mismatch")
}

func TestVerifyCertChainDigestMatch(t *testing.T) {
	certs := map[string]Certificate{
		"leaf": {
			Hash:           "leaf",
			PreviousHash:   "root",
			MultiSignature: "sig",
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"snapshot_digest": "matching_digest",
				},
			},
		},
		"root": {
			Hash:             "root",
			PreviousHash:     "root",
			GenesisSignature: "genesis_sig",
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
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			},
		),
	)
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"leaf",
		"matching_digest",
	)
	require.NoError(t, err)
}

func TestBootstrapWithCertVerification(t *testing.T) {
	archiveData := createChunkArchive(t)

	snapshots := []SnapshotListItem{
		{
			Digest:  "abc123def456789012345678",
			Network: "preprod",
			Beacon: Beacon{
				Epoch:               270,
				ImmutableFileNumber: 5320,
			},
			CertificateHash:      "cert_leaf",
			Size:                 int64(len(archiveData)),
			CreatedAt:            "2026-02-10T00:24:56.094721055Z",
			Locations:            []string{},
			CompressionAlgorithm: "zstandard",
		},
	}

	certs := map[string]Certificate{
		"cert_leaf": {
			Hash:           "cert_leaf",
			PreviousHash:   "cert_genesis",
			MultiSignature: "sig",
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"snapshot_digest": "abc123def456789012345678",
				},
			},
		},
		"cert_genesis": {
			Hash:             "cert_genesis",
			PreviousHash:     "cert_genesis",
			GenesisSignature: "genesis_sig",
		},
	}

	mux := http.NewServeMux()

	mux.HandleFunc(
		"/artifact/snapshots",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().
				Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(snapshots); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		},
	)

	mux.HandleFunc(
		"/certificate/",
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
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		},
	)

	mux.HandleFunc(
		"/download/snapshot.tar.zst",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().
				Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(archiveData)
		},
	)

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	snapshots[0].Locations = []string{
		server.URL + "/download/snapshot.tar.zst",
	}

	downloadDir := t.TempDir()

	result, err := Bootstrap(
		context.Background(),
		BootstrapConfig{
			Network:                "preprod",
			AggregatorURL:          server.URL,
			DownloadDir:            downloadDir,
			VerifyCertificateChain: true,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(
		t,
		"abc123def456789012345678",
		result.Snapshot.Digest,
	)
}
