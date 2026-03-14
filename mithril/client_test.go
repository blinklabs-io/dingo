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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func standardTestMetadata() CertificateMetadata {
	return CertificateMetadata{
		Network: "preprod",
		Version: "0.1.0",
		Parameters: ProtocolParameters{
			K: 5, M: 100, PhiF: 0.65,
		},
		InitiatedAt: "2026-02-10T00:06:46.282994610Z",
		SealedAt:    "2026-02-10T00:07:45.906010670Z",
	}
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

func TestListMithrilStakeDistributions(t *testing.T) {
	expected := []MithrilStakeDistributionListItem{
		{
			Hash:            "msd123",
			CertificateHash: "cert123",
			Epoch:           270,
		},
	}
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/artifact/mithril-stake-distributions" {
			t.Errorf(
				"expected path /artifact/mithril-stake-distributions, got %s",
				r.URL.Path,
			)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expected)
	})

	client := NewClient(server.URL)
	ret, err := client.ListMithrilStakeDistributions(context.Background())
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, expected[0].Hash, ret[0].Hash)
}

func TestGetMithrilStakeDistribution(t *testing.T) {
	expected := MithrilStakeDistribution{
		Hash:            "msd123",
		CertificateHash: "cert123",
		Epoch:           270,
		Signers: []MithrilStakeDistributionParty{
			{PartyID: "pool1abc", Stake: 42, VerificationKey: "vk1"},
		},
	}
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/artifact/mithril-stake-distribution/msd123" {
			t.Errorf(
				"expected path /artifact/mithril-stake-distribution/msd123, got %s",
				r.URL.Path,
			)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expected)
	})

	client := NewClient(server.URL)
	ret, err := client.GetMithrilStakeDistribution(
		context.Background(),
		"msd123",
	)
	require.NoError(t, err)
	require.NotNil(t, ret)
	require.Equal(t, expected.Hash, ret.Hash)
	require.Len(t, ret.Signers, 1)
}

func TestListCardanoStakeDistributions(t *testing.T) {
	expected := []CardanoStakeDistributionListItem{
		{
			Hash:            "csd123",
			CertificateHash: "cert123",
			Epoch:           270,
		},
	}
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/artifact/cardano-stake-distributions" {
			t.Errorf(
				"expected path /artifact/cardano-stake-distributions, got %s",
				r.URL.Path,
			)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expected)
	})

	client := NewClient(server.URL)
	ret, err := client.ListCardanoStakeDistributions(context.Background())
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, expected[0].Hash, ret[0].Hash)
}

func TestGetCardanoStakeDistribution(t *testing.T) {
	expected := CardanoStakeDistribution{
		Hash:            "csd123",
		CertificateHash: "cert123",
		Epoch:           270,
		Pools: []CardanoStakeDistributionParty{
			{PoolID: "pool1abc", Stake: 42},
		},
	}
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/artifact/cardano-stake-distribution/csd123" {
			t.Errorf(
				"expected path /artifact/cardano-stake-distribution/csd123, got %s",
				r.URL.Path,
			)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(expected)
	})

	client := NewClient(server.URL)
	ret, err := client.GetCardanoStakeDistribution(
		context.Background(),
		"csd123",
	)
	require.NoError(t, err)
	require.NotNil(t, ret)
	require.Equal(t, expected.Hash, ret.Hash)
	require.Len(t, ret.Pools, 1)
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

	kind, err := set.Kind()
	require.NoError(t, err)
	require.Equal(t, "CardanoImmutableFilesFull", kind)
	beacon := set.CardanoImmutableFilesFull()
	require.NotNil(t, beacon)
	require.Equal(t, uint64(270), beacon.Epoch)
	require.Equal(t, uint64(5320), beacon.ImmutableFileNumber)
}

func TestSignedEntityTypeMithrilStakeDistribution(t *testing.T) {
	raw := `{"MithrilStakeDistribution":{"epoch":270}}`
	var set SignedEntityType
	err := json.Unmarshal([]byte(raw), &set)
	require.NoError(t, err)

	kind, err := set.Kind()
	require.NoError(t, err)
	require.Equal(t, "MithrilStakeDistribution", kind)
	beacon := set.MithrilStakeDistribution()
	require.NotNil(t, beacon)
	require.Equal(t, uint64(270), beacon.Epoch)

	other := set.CardanoImmutableFilesFull()
	require.Nil(t, other)
}

func TestSignedEntityTypeMithrilStakeDistributionScalar(t *testing.T) {
	raw := `{"MithrilStakeDistribution":275}`
	var set SignedEntityType
	err := json.Unmarshal([]byte(raw), &set)
	require.NoError(t, err)

	kind, err := set.Kind()
	require.NoError(t, err)
	require.Equal(t, "MithrilStakeDistribution", kind)
	beacon := set.MithrilStakeDistribution()
	require.NotNil(t, beacon)
	require.Equal(t, uint64(275), beacon.Epoch)
}

func TestProtocolMessageComputeHashMatchesUpstreamEnumOrder(t *testing.T) {
	msg := ProtocolMessage{
		MessageParts: map[string]string{
			"current_epoch":                   "275",
			"next_aggregate_verification_key": "avk",
			"next_protocol_parameters":        "pparams",
			"snapshot_digest":                 "digest",
		},
	}
	require.Equal(
		t,
		"71ca187b1d4b92eb9645da46f0fb7464534f520e5c0bef1d96d4244410df2ce5",
		msg.ComputeHash(),
	)
}

func TestSignedEntityTypeCardanoStakeDistribution(t *testing.T) {
	raw := `{"CardanoStakeDistribution":{"epoch":314}}`
	var set SignedEntityType
	err := json.Unmarshal([]byte(raw), &set)
	require.NoError(t, err)

	kind, err := set.Kind()
	require.NoError(t, err)
	require.Equal(t, "CardanoStakeDistribution", kind)
	beacon := set.CardanoStakeDistribution()
	require.NotNil(t, beacon)
	require.Equal(t, uint64(314), beacon.Epoch)
}

func TestSignedEntityTypeCardanoStakeDistributionScalar(t *testing.T) {
	raw := `{"CardanoStakeDistribution":314}`
	var set SignedEntityType
	err := json.Unmarshal([]byte(raw), &set)
	require.NoError(t, err)

	kind, err := set.Kind()
	require.NoError(t, err)
	require.Equal(t, "CardanoStakeDistribution", kind)
	beacon := set.CardanoStakeDistribution()
	require.NotNil(t, beacon)
	require.Equal(t, uint64(314), beacon.Epoch)
}

func TestSignedEntityTypeUnknownKind(t *testing.T) {
	raw := `{"CardanoTransactions":{"epoch":271}}`
	var set SignedEntityType
	err := json.Unmarshal([]byte(raw), &set)
	require.NoError(t, err)

	kind, err := set.Kind()
	require.NoError(t, err)
	require.Equal(t, "CardanoTransactions", kind)
	beacon := set.CardanoImmutableFilesFull()
	require.Nil(t, beacon)
}

func TestSignedEntityTypeCardanoTransactions(t *testing.T) {
	raw := `{"CardanoTransactions":{"epoch":271,"block_number":54321}}`
	var set SignedEntityType
	err := json.Unmarshal([]byte(raw), &set)
	require.NoError(t, err)

	kind, err := set.Kind()
	require.NoError(t, err)
	require.Equal(t, "CardanoTransactions", kind)
	beacon := set.CardanoTransactions()
	require.NotNil(t, beacon)
	require.Equal(t, uint64(271), beacon.Epoch)
	require.Equal(t, uint64(54321), beacon.BlockNumber)
}

func TestSignedEntityTypeCardanoTransactionsFeedHash(t *testing.T) {
	raw := `{"CardanoTransactions":{"epoch":271,"block_number":54321}}`
	var set SignedEntityType
	err := json.Unmarshal([]byte(raw), &set)
	require.NoError(t, err)

	hasher := sha256.New()
	err = set.feedHash(hasher)
	require.NoError(t, err)
	got := fmt.Sprintf("%x", hasher.Sum(nil))

	// Build expected hash: epoch(271) + block_number(54321)
	expected := sha256.New()
	expected.Write(uint64ToBigEndianBytes(271))
	expected.Write(uint64ToBigEndianBytes(54321))
	want := fmt.Sprintf("%x", expected.Sum(nil))

	require.Equal(t, want, got)
}

func TestNetworkConfigForNetwork(t *testing.T) {
	cfg, err := NetworkConfigForNetwork("preview")
	require.NoError(t, err)
	require.Equal(
		t,
		"https://aggregator.pre-release-preview.api.mithril.network/aggregator",
		cfg.AggregatorURL,
	)
	require.Contains(t, cfg.GenesisVerificationKeyURL, "/preview/genesis.vkey")
	require.Contains(
		t,
		cfg.AncillaryVerificationKeyURL,
		"/preview/genesis-ancillary.vkey",
	)
}

func TestGenesisVerificationKeyURLForNetwork(t *testing.T) {
	url, err := GenesisVerificationKeyURLForNetwork("mainnet")
	require.NoError(t, err)
	require.Contains(t, url, "/mainnet/genesis.vkey")
}

func TestAncillaryVerificationKeyURLForNetwork(t *testing.T) {
	url, err := AncillaryVerificationKeyURLForNetwork("preprod")
	require.NoError(t, err)
	require.Contains(t, url, "/preprod/genesis-ancillary.vkey")
}

func TestCertificateAggregateVerificationKeyBytes(t *testing.T) {
	cert := &Certificate{
		AggregateVerificationKey: "61626364",
	}
	decoded, err := cert.AggregateVerificationKeyBytes()
	require.NoError(t, err)
	require.Equal(t, []byte("abcd"), decoded)
}

func TestCertificateMultiSignatureBytes(t *testing.T) {
	cert := &Certificate{
		MultiSignature: "YWJjZA==",
	}
	decoded, err := cert.MultiSignatureBytes()
	require.NoError(t, err)
	require.Equal(t, []byte("abcd"), decoded)
}

func TestMithrilStakeDistributionPartyVerificationKeyBytes(t *testing.T) {
	party := &MithrilStakeDistributionParty{
		VerificationKey: "61626364",
	}
	decoded, err := party.VerificationKeyBytes()
	require.NoError(t, err)
	require.Equal(t, []byte("abcd"), decoded)
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
	cert := Certificate{
		Epoch:            0,
		PreviousHash:     "",
		GenesisSignature: "sig",
		Metadata:         standardTestMetadata(),
		ProtocolMessage: ProtocolMessage{
			MessageParts: map[string]string{
				"current_epoch": "0",
			},
		},
	}
	finalizeTestCertificate(t, &cert)
	cert.PreviousHash = cert.Hash
	finalizeTestCertificate(t, &cert)
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
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
		cert.Hash,
		"",
	)
	require.NoError(t, err)
}

func TestVerifyCertChainThreeDeep(t *testing.T) {
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	certs := map[string]Certificate{
		"leaf": {
			Epoch:                    2,
			AggregateVerificationKey: g2Hex,
			MultiSignature:           g1Hex,
			Metadata:                 standardTestMetadata(),
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{"current_epoch": "2"},
			},
		},
		"middle": {
			Epoch:                    1,
			AggregateVerificationKey: g2Hex,
			MultiSignature:           g1Hex,
			Metadata:                 standardTestMetadata(),
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"current_epoch":                   "1",
					"next_aggregate_verification_key": g2Hex,
					"next_protocol_parameters":        standardTestMetadata().Parameters.ComputeHash(),
				},
			},
		},
		"root": {
			Epoch:                    0,
			GenesisSignature:         "genesis_sig",
			AggregateVerificationKey: g2Hex,
			Metadata:                 standardTestMetadata(),
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"current_epoch":                   "0",
					"next_aggregate_verification_key": g2Hex,
					"next_protocol_parameters":        standardTestMetadata().Parameters.ComputeHash(),
				},
			},
		},
	}
	root := certs["root"]
	finalizeTestCertificate(t, &root)
	root.PreviousHash = root.Hash
	finalizeTestCertificate(t, &root)
	certs["root"] = root
	middle := certs["middle"]
	middle.PreviousHash = root.Hash
	finalizeTestCertificate(t, &middle)
	certs["middle"] = middle
	leaf := certs["leaf"]
	leaf.PreviousHash = middle.Hash
	finalizeTestCertificate(t, &leaf)
	certs["leaf"] = leaf

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
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
		leaf.Hash,
		"",
	)
	require.NoError(t, err)
}

func TestVerifyCertChainMissingCert(t *testing.T) {
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	certs := map[string]Certificate{
		"leaf": {
			Epoch:                    1,
			PreviousHash:             "missing_cert",
			AggregateVerificationKey: g2Hex,
			MultiSignature:           g1Hex,
			Metadata:                 standardTestMetadata(),
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{"current_epoch": "1"},
			},
		},
	}
	leaf := certs["leaf"]
	finalizeTestCertificate(t, &leaf)
	certs["leaf"] = leaf

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
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
		leaf.Hash,
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
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	cert := Certificate{
		Epoch:                    1,
		PreviousHash:             "",
		AggregateVerificationKey: g2Hex,
		MultiSignature:           g1Hex,
		Metadata:                 standardTestMetadata(),
		ProtocolMessage: ProtocolMessage{
			MessageParts: map[string]string{"current_epoch": "1"},
		},
	}
	finalizeTestCertificate(t, &cert)
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
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
		cert.Hash,
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty previous_hash")
}

func TestVerifyCertChainDigestMismatch(t *testing.T) {
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	certs := map[string]Certificate{
		"leaf": {
			Epoch:                    1,
			AggregateVerificationKey: g2Hex,
			MultiSignature:           g1Hex,
			Metadata:                 standardTestMetadata(),
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"snapshot_digest": "wrong_digest",
					"current_epoch":   "1",
				},
			},
		},
		"root": {
			Epoch:                    1,
			GenesisSignature:         "genesis_sig",
			AggregateVerificationKey: g2Hex,
			Metadata:                 standardTestMetadata(),
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{"current_epoch": "1"},
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

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
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
		leaf.Hash,
		"expected_digest",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_digest mismatch")
}

func TestVerifyCertChainDigestMatch(t *testing.T) {
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	certs := map[string]Certificate{
		"leaf": {
			Epoch:                    1,
			AggregateVerificationKey: g2Hex,
			MultiSignature:           g1Hex,
			Metadata:                 standardTestMetadata(),
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"snapshot_digest": "matching_digest",
					"current_epoch":   "1",
				},
			},
		},
		"root": {
			Epoch:                    1,
			GenesisSignature:         "genesis_sig",
			AggregateVerificationKey: g2Hex,
			Metadata:                 standardTestMetadata(),
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{"current_epoch": "1"},
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

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
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
		leaf.Hash,
		"matching_digest",
	)
	require.NoError(t, err)
}

// TestVerifyCertChainRejectsStaleHashCrossReferences verifies that
// cross-referencing certificates with stale hashes is rejected.
// Content-addressed hashing makes true certificate cycles
// computationally infeasible — re-hashing one cert invalidates
// the other's reference. The seen-set cycle guard in
// VerifyCertificateChainWithMode is defense-in-depth against a
// hypothetical aggregator bug; this test confirms the chain
// breaks on the stale reference.
func TestVerifyCertChainRejectsStaleHashCrossReferences(t *testing.T) {
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	certA := Certificate{
		Epoch:                    1,
		AggregateVerificationKey: g2Hex,
		MultiSignature:           g1Hex,
		Metadata:                 standardTestMetadata(),
		ProtocolMessage: ProtocolMessage{
			MessageParts: map[string]string{"current_epoch": "1"},
		},
	}
	certB := Certificate{
		Epoch:                    1,
		AggregateVerificationKey: g2Hex,
		MultiSignature:           g1Hex,
		Metadata:                 standardTestMetadata(),
		ProtocolMessage: ProtocolMessage{
			MessageParts: map[string]string{"current_epoch": "1"},
		},
	}
	// Finalize both with initial hashes.
	finalizeTestCertificate(t, &certA)
	finalizeTestCertificate(t, &certB)
	// Cross-reference: A→B, then B→A. Re-hashing A changes its
	// hash, so B's PreviousHash now points to A's NEW hash, but
	// A's PreviousHash still holds B's OLD hash.
	certA.PreviousHash = certB.Hash
	finalizeTestCertificate(t, &certA)
	certB.PreviousHash = certA.Hash
	finalizeTestCertificate(t, &certB)
	// certA.PreviousHash is certB's OLD hash (before certB was
	// re-hashed), so the lookup will fail.
	byHash := map[string]Certificate{
		certA.Hash: certA,
		certB.Hash: certB,
	}

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				hash := r.URL.Path[len("/certificate/"):]
				cert, ok := byHash[hash]
				if !ok {
					http.NotFound(w, r)
					return
				}
				w.Header().
					Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(cert)
			},
		),
	)
	t.Cleanup(server.Close)

	client := NewClient(server.URL)
	err := VerifyCertificateChain(
		context.Background(),
		client,
		certA.Hash,
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetching certificate")
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
	genesisPubKey, genesisPrivKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	genesisKeyText := fmt.Sprintf(
		`{"type":"GenesisVerificationKey_ed25519","description":"Mithril Genesis Verification Key","cborHex":"5820%s"}`,
		hex.EncodeToString(genesisPubKey),
	)

	snapshots := []SnapshotListItem{
		{
			SnapshotBase: SnapshotBase{
				Digest:  "abc123def456789012345678",
				Network: "preprod",
				Beacon: Beacon{
					Epoch:               270,
					ImmutableFileNumber: 5320,
				},
				CertificateHash:      "placeholder",
				Size:                 int64(len(archiveData)),
				CreatedAt:            "2026-02-10T00:24:56.094721055Z",
				Locations:            []string{},
				CompressionAlgorithm: "zstandard",
			},
		},
	}

	certs := map[string]Certificate{
		"cert_leaf": {
			Epoch:                    270,
			AggregateVerificationKey: "",
			MultiSignature:           "",
			SignedEntityType: SignedEntityType{
				raw: json.RawMessage(
					`{"CardanoImmutableFilesFull":{"epoch":270,"immutable_file_number":5320}}`,
				),
			},
			Metadata: CertificateMetadata{
				Network:     "preprod",
				Version:     "0.1.0",
				Parameters:  ProtocolParameters{K: 1, M: 1, PhiF: 1.0},
				InitiatedAt: "2026-02-10T00:06:46.282994610Z",
				SealedAt:    "2026-02-10T00:07:45.906010670Z",
				Signers:     nil,
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"snapshot_digest": "abc123def456789012345678",
					"current_epoch":   "270",
				},
			},
		},
		"cert_genesis": {
			Epoch:            269,
			GenesisSignature: "",
			Metadata: CertificateMetadata{
				Network:     "preprod",
				Version:     "0.1.0",
				Parameters:  ProtocolParameters{K: 1, M: 1, PhiF: 1.0},
				InitiatedAt: "2026-02-09T00:06:46.282994610Z",
				SealedAt:    "2026-02-09T00:07:45.906010670Z",
			},
			ProtocolMessage: ProtocolMessage{
				MessageParts: map[string]string{
					"current_epoch":                   "269",
					"next_aggregate_verification_key": "",
					"next_protocol_parameters":        ProtocolParameters{K: 1, M: 1, PhiF: 1.0}.ComputeHash(),
				},
			},
		},
	}
	leafCert := certs["cert_leaf"]
	leafCert.SignedMessage = leafCert.ProtocolMessage.ComputeHash()
	leafCert.AggregateVerificationKey, leafCert.MultiSignature = testCreateEncodedSTMProof(
		t,
		[]byte(leafCert.SignedMessage),
	)
	certs["cert_leaf"] = leafCert

	genesisCert := certs["cert_genesis"]
	genesisCert.ProtocolMessage.MessageParts["next_aggregate_verification_key"] = leafCert.AggregateVerificationKey
	genesisCert.SignedMessage = genesisCert.ProtocolMessage.ComputeHash()
	genesisSignature := ed25519.Sign(genesisPrivKey, []byte(genesisCert.SignedMessage))
	genesisCert.GenesisSignature = hex.EncodeToString(genesisSignature)
	hash, err := genesisCert.ComputeHash()
	require.NoError(t, err)
	genesisCert.Hash = hash
	genesisCert.PreviousHash = genesisCert.Hash
	hash, err = genesisCert.ComputeHash()
	require.NoError(t, err)
	genesisCert.Hash = hash
	certs["cert_genesis"] = genesisCert

	leafCert = certs["cert_leaf"]
	leafCert.PreviousHash = genesisCert.Hash
	hash, err = leafCert.ComputeHash()
	require.NoError(t, err)
	leafCert.Hash = hash
	certs["cert_leaf"] = leafCert
	snapshots[0].CertificateHash = leafCert.Hash

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
			w.Header().
				Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(cert); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		},
	)

	mux.HandleFunc(
		"/artifact/mithril-stake-distributions",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(
				[]MithrilStakeDistributionListItem{
					{
						Hash:            "msd123",
						CertificateHash: leafCert.Hash,
						Epoch:           270,
					},
				},
			)
		},
	)

	mux.HandleFunc(
		"/artifact/mithril-stake-distribution/msd123",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(
				MithrilStakeDistribution{
					Hash:            "msd123",
					CertificateHash: leafCert.Hash,
					Epoch:           270,
					Signers:         nil,
				},
			)
		},
	)

	mux.HandleFunc(
		"/artifact/cardano-stake-distributions",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(
				[]CardanoStakeDistributionListItem{
					{
						Hash:            "csd123",
						CertificateHash: leafCert.Hash,
						Epoch:           270,
					},
				},
			)
		},
	)

	mux.HandleFunc(
		"/artifact/cardano-stake-distribution/csd123",
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(
				CardanoStakeDistribution{
					Hash:            "csd123",
					CertificateHash: leafCert.Hash,
					Epoch:           270,
					Pools:           nil,
				},
			)
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
			GenesisVerificationKey: genesisKeyText,
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
