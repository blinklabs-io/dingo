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

func newTestServer(
	t *testing.T,
	handler http.HandlerFunc,
) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func TestListSnapshots(t *testing.T) {
	expected := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:  "abc123def456",
				Network: "preprod",
				Beacon: Beacon{
					Epoch:               270,
					ImmutableFileNumber: 5320,
				},
				CertificateHash: "cert123",
				Size:            3267621057,
				AncillarySize:   666693003,
				CreatedAt:       "2026-02-10T00:24:56.094721055Z",
				Locations: []string{
					"https://example.com/snapshot.tar.zst",
				},
				AncillaryLocations: []string{
					"https://example.com/ancillary.tar.zst",
				},
				CompressionAlgorithm: "zstandard",
				CardanoNodeVersion:   "10.5.3",
			},
		},
	}

	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Use t.Errorf (not require) because httptest handlers
		// run in a separate goroutine; require calls t.FailNow
		// which panics from non-test goroutines.
		if r.URL.Path != "/artifact/snapshots" {
			t.Errorf(
				"expected path /artifact/snapshots, got %s",
				r.URL.Path,
			)
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf(
				"expected Accept application/json, got %s",
				r.Header.Get("Accept"),
			)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(expected); err != nil {
			http.Error(
				w, err.Error(),
				http.StatusInternalServerError,
			)
		}
	})

	client := NewClient(server.URL)
	snapshots, err := client.ListSnapshots(context.Background())
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.Equal(t, expected[0].Digest, snapshots[0].Digest)
	require.Equal(t, expected[0].Network, snapshots[0].Network)
	require.Equal(t, expected[0].Beacon.Epoch, snapshots[0].Beacon.Epoch)
	require.Equal(
		t,
		expected[0].Beacon.ImmutableFileNumber,
		snapshots[0].Beacon.ImmutableFileNumber,
	)
	require.Equal(t, expected[0].Size, snapshots[0].Size)
	require.Equal(t, expected[0].Locations, snapshots[0].Locations)
	require.Equal(
		t,
		expected[0].CompressionAlgorithm,
		snapshots[0].CompressionAlgorithm,
	)
}

func TestGetSnapshot(t *testing.T) {
	expected := SnapshotListItem{
		SnapshotBase: SnapshotBase{
			Digest:  "abc123def456",
			Network: "preprod",
			Beacon: Beacon{
				Epoch:               270,
				ImmutableFileNumber: 5320,
			},
			CertificateHash: "cert123",
			Size:            3267621057,
			CreatedAt:       "2026-02-10T00:24:56.094721055Z",
			Locations: []string{
				"https://example.com/snapshot.tar.zst",
			},
			CompressionAlgorithm: "zstandard",
			CardanoNodeVersion:   "10.5.3",
		},
	}

	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/artifact/snapshot/abc123def456" {
			t.Errorf(
				"expected path /artifact/snapshot/abc123def456, got %s",
				r.URL.Path,
			)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(expected); err != nil {
			http.Error(
				w, err.Error(),
				http.StatusInternalServerError,
			)
		}
	})

	client := NewClient(server.URL)
	snapshot, err := client.GetSnapshot(
		context.Background(),
		"abc123def456",
	)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, expected.Digest, snapshot.Digest)
	require.Equal(t, expected.Network, snapshot.Network)
	require.Equal(t, expected.Size, snapshot.Size)
}

func TestGetCertificate(t *testing.T) {
	expected := Certificate{
		Hash:         "certhash123",
		PreviousHash: "prevhash456",
		Epoch:        270,
		Metadata: CertificateMetadata{
			Network: "preprod",
			Version: "0.1.0",
			Parameters: ProtocolParameters{
				K:    5,
				M:    100,
				PhiF: 0.65,
			},
			InitiatedAt: "2026-02-10T00:06:46.282994610Z",
			SealedAt:    "2026-02-10T00:07:45.906010670Z",
			Signers: []StakeDistributionParty{
				{
					PartyID: "pool1abc123",
					Stake:   1484379914025,
				},
			},
		},
		ProtocolMessage: ProtocolMessage{
			MessageParts: map[string]string{
				"snapshot_digest": "abc123def456",
			},
		},
		SignedMessage:            "signedmsg123",
		AggregateVerificationKey: "avk123",
		MultiSignature:           "multisig123",
		GenesisSignature:         "",
	}

	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/certificate/certhash123" {
			t.Errorf(
				"expected path /certificate/certhash123, got %s",
				r.URL.Path,
			)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(expected); err != nil {
			http.Error(
				w, err.Error(),
				http.StatusInternalServerError,
			)
		}
	})

	client := NewClient(server.URL)
	cert, err := client.GetCertificate(
		context.Background(),
		"certhash123",
	)
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.Equal(t, expected.Hash, cert.Hash)
	require.Equal(t, expected.PreviousHash, cert.PreviousHash)
	require.Equal(t, expected.Epoch, cert.Epoch)
	require.Equal(
		t,
		expected.Metadata.Network,
		cert.Metadata.Network,
	)
	require.Equal(
		t,
		expected.Metadata.Parameters.K,
		cert.Metadata.Parameters.K,
	)
	require.Len(t, cert.Metadata.Signers, 1)
	require.False(t, cert.IsGenesis())
}

func TestGetCertificateGenesis(t *testing.T) {
	expected := Certificate{
		Hash:             "genesis_cert_hash",
		PreviousHash:     "genesis_cert_hash",
		Epoch:            0,
		GenesisSignature: "genesis_sig_abc123",
	}

	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(expected); err != nil {
			http.Error(
				w, err.Error(),
				http.StatusInternalServerError,
			)
		}
	})

	client := NewClient(server.URL)
	cert, err := client.GetCertificate(
		context.Background(),
		"genesis_cert_hash",
	)
	require.NoError(t, err)
	require.NotNil(t, cert)
	require.True(t, cert.IsGenesis())
	require.True(t, cert.IsChainingToItself())
}

func TestGetLatestSnapshot(t *testing.T) {
	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:  "latest_digest",
				Network: "preprod",
				Beacon: Beacon{
					Epoch:               270,
					ImmutableFileNumber: 5320,
				},
				Size:      3267621057,
				Locations: []string{"https://example.com/latest.tar.zst"},
			},
		},
		{
			SnapshotBase: SnapshotBase{
				Digest:  "older_digest",
				Network: "preprod",
				Beacon: Beacon{
					Epoch:               269,
					ImmutableFileNumber: 5319,
				},
				Size:      3267176247,
				Locations: []string{"https://example.com/older.tar.zst"},
			},
		},
	}

	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(snapshots); err != nil {
			http.Error(
				w, err.Error(),
				http.StatusInternalServerError,
			)
		}
	})

	client := NewClient(server.URL)
	latest, err := client.GetLatestSnapshot(context.Background())
	require.NoError(t, err)
	require.NotNil(t, latest)
	require.Equal(t, "latest_digest", latest.Digest)
}

func TestGetLatestSnapshotEmpty(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte("[]")); err != nil {
			t.Errorf("writing response: %v", err)
		}
	})

	client := NewClient(server.URL)
	_, err := client.GetLatestSnapshot(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "no snapshots available")
}

func TestClientErrorHandling(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})

	client := NewClient(server.URL)

	_, err := client.ListSnapshots(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "404")

	_, err = client.GetSnapshot(
		context.Background(),
		"nonexistent",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "404")

	_, err = client.GetCertificate(
		context.Background(),
		"nonexistent",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "404")
}

func TestClientContextCancellation(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte("[]")); err != nil {
			t.Errorf("writing response: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	client := NewClient(server.URL)
	_, err := client.ListSnapshots(ctx)
	require.Error(t, err)
}

func TestAggregatorURLForNetwork(t *testing.T) {
	tests := []struct {
		name    string
		network string
		want    string
		wantErr bool
	}{
		{
			name:    "mainnet",
			network: "mainnet",
			want:    "https://aggregator.release-mainnet.api.mithril.network/aggregator",
		},
		{
			name:    "preprod",
			network: "preprod",
			want:    "https://aggregator.release-preprod.api.mithril.network/aggregator",
		},
		{
			name:    "preview",
			network: "preview",
			want:    "https://aggregator.pre-release-preview.api.mithril.network/aggregator",
		},
		{
			name:    "unknown network",
			network: "testnet",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AggregatorURLForNetwork(tt.network)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSignedEntityTypeCardanoImmutableFilesFull(t *testing.T) {
	raw := `{"CardanoImmutableFilesFull":{"epoch":270,"immutable_file_number":5320}}`
	var set SignedEntityType
	err := json.Unmarshal([]byte(raw), &set)
	require.NoError(t, err)

	beacon := set.CardanoImmutableFilesFull()
	require.NotNil(t, beacon)
	require.Equal(t, uint64(270), beacon.Epoch)
	require.Equal(t, uint64(5320), beacon.ImmutableFileNumber)
}

func TestSignedEntityTypeOther(t *testing.T) {
	raw := `{"MithrilStakeDistribution":{"epoch":270}}`
	var set SignedEntityType
	err := json.Unmarshal([]byte(raw), &set)
	require.NoError(t, err)

	beacon := set.CardanoImmutableFilesFull()
	require.Nil(t, beacon)
}

func TestSnapshotCreatedAtTime(t *testing.T) {
	s := &SnapshotListItem{
		SnapshotBase: SnapshotBase{
			CreatedAt: "2026-02-10T00:24:56.094721055Z",
		},
	}
	ts, err := s.CreatedAtTime()
	require.NoError(t, err)
	require.Equal(t, 2026, ts.Year())
	require.Equal(t, 2, int(ts.Month()))
	require.Equal(t, 10, ts.Day())
}

func TestWithHTTPClient(t *testing.T) {
	customClient := &http.Client{}
	client := NewClient(
		"https://example.com",
		WithHTTPClient(customClient),
	)
	require.Equal(t, customClient, client.httpClient)
}

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

func TestVerifyCertChainCycleDetection(t *testing.T) {
	certs := map[string]Certificate{
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
					http.Error(
						w,
						err.Error(),
						http.StatusInternalServerError,
					)
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
		"cert_a",
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cycle detected")
}

func TestVerifyCertChainNilClient(t *testing.T) {
	err := VerifyCertificateChain(
		context.Background(),
		nil,
		"some_hash",
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "client is nil")
}

func TestVerifyCertChainEmptyHash(t *testing.T) {
	client := NewClient("http://example.com")
	err := VerifyCertificateChain(
		context.Background(),
		client,
		"",
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

func TestBootstrapWithCertVerification(t *testing.T) {
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
				CertificateHash:      "cert_leaf",
				Size:                 int64(len(archiveData)),
				CreatedAt:            "2026-02-10T00:24:56.094721055Z",
				Locations:            []string{},
				CompressionAlgorithm: "zstandard",
			},
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
