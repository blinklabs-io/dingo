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
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	cardanocfg "github.com/blinklabs-io/dingo/config/cardano"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLiveMithrilV2Verification exercises the full v2 verification
// chain against the real aggregator: artifact self-hash, certificate
// chain (STM), digest list merkle root, and download + digest
// verification of a single immutable archive. Bandwidth use is kept
// small (one immutable archive plus the digest list).
func TestLiveMithrilV2Verification(t *testing.T) {
	if os.Getenv("DINGO_LIVE_MITHRIL") == "" {
		t.Skip("set DINGO_LIVE_MITHRIL=1 to run live Mithril v2 verification")
	}

	network := os.Getenv("DINGO_LIVE_MITHRIL_NETWORK")
	if network == "" {
		network = "preprod"
	}
	_, thisFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), ".."))
	configPath := os.Getenv("DINGO_LIVE_MITHRIL_CONFIG")
	if configPath == "" {
		configPath = filepath.Join(
			repoRoot,
			"config",
			"cardano",
			network,
			"config.json",
		)
	}
	if _, err := os.Stat(configPath); err != nil {
		if os.IsNotExist(err) {
			t.Skipf("cardano config not available at %s: %v", configPath, err)
		}
		t.Fatalf("unexpected error checking config at %s: %v", configPath, err)
	}

	cfg, err := cardanocfg.NewCardanoNodeConfigFromFile(configPath)
	require.NoError(t, err)
	require.NotEmpty(t, cfg.MithrilGenesisVerificationKey)

	aggregatorURL := os.Getenv("DINGO_LIVE_MITHRIL_AGGREGATOR_URL")
	if aggregatorURL == "" {
		aggregatorURL, err = AggregatorURLForNetwork(network)
		require.NoError(t, err)
	}

	client := NewClient(aggregatorURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Latest v2 artifact with verified self-hash
	artifact, err := client.GetLatestCardanoDatabaseSnapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, artifact.Hash, artifact.ComputeHash())
	require.NotEmpty(t, artifact.CertificateHash)
	require.NotEmpty(t, artifact.MerkleRoot)

	// Full certificate chain with STM signatures and genesis binding
	bootstrapCfg := BootstrapConfig{
		Network:                network,
		Backend:                BackendV2,
		VerifyCertificateChain: true,
		GenesisVerificationKey: cfg.MithrilGenesisVerificationKey,
		AncillaryVerificationKey: cfg.
			MithrilGenesisAncillaryVerificationKey,
		Logger: slog.Default(),
	}
	require.NoError(
		t,
		verifyArtifactCertificateV2(ctx, bootstrapCfg, client, artifact),
	)

	// Digest list fetched and verified against the certified root
	downloadDir := t.TempDir()
	digests, err := fetchVerifiedDigests(
		ctx, client, bootstrapCfg, artifact, downloadDir,
	)
	require.NoError(t, err)
	require.NotEmpty(t, digests)

	// Download a single immutable archive and verify its trio
	var location *CardanoDatabaseLocation
	for i := range artifact.Immutables.Locations {
		if artifact.Immutables.Locations[i].URITemplate != "" {
			location = &artifact.Immutables.Locations[i]
			break
		}
	}
	require.NotNil(t, location)
	extractDir := filepath.Join(downloadDir, "extract")
	require.NoError(t, fetchImmutableArchive(
		ctx,
		bootstrapCfg,
		slog.Default(),
		location,
		0,
		downloadDir,
		extractDir,
	))
	bytes, err := checkImmutableTrio(
		filepath.Join(extractDir, "immutable"), 0, digests,
	)
	require.NoError(t, err)
	assert.Positive(t, bytes)

	// Spot-check: the digest map entries are well-formed sha256 hex
	for name, digest := range digests {
		decoded, err := hex.DecodeString(digest)
		require.NoError(t, err, name)
		require.Len(t, decoded, 32, name)
		break
	}
}
