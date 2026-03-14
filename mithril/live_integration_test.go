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
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	cardanocfg "github.com/blinklabs-io/dingo/config/cardano"
	"github.com/stretchr/testify/require"
)

func TestLiveMithrilSTMVerification(t *testing.T) {
	if os.Getenv("DINGO_LIVE_MITHRIL") == "" {
		t.Skip("set DINGO_LIVE_MITHRIL=1 to run live Mithril STM verification")
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	snapshot, err := client.GetLatestSnapshot(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, snapshot.CertificateHash)
	require.NotEmpty(t, snapshot.Digest)

	verification, err := VerifyCertificateChainWithMode(
		ctx,
		client,
		snapshot.CertificateHash,
		snapshot.Digest,
		VerificationModeSTM,
	)
	require.NoError(t, err)
	require.NotNil(t, verification)
	require.NotNil(t, verification.LeafCertificate)
	require.NotNil(t, verification.GenesisCertificate)

	require.NoError(
		t,
		VerifyGenesisCertificateSignature(
			verification.GenesisCertificate,
			cfg.MithrilGenesisVerificationKey,
		),
	)

	material, err := BuildVerificationMaterial(ctx, client, verification)
	require.NoError(t, err)
	require.NotNil(t, material)
	require.NoError(t, ValidateVerificationMaterial(material))
}
