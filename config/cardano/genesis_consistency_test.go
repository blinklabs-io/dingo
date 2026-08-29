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

package cardano

import (
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

func TestValidateGenesisConsistencyNoGenesis(t *testing.T) {
	// With neither (or only one) genesis loaded there is nothing to
	// cross-check, so the consistency check must pass.
	require.NoError(t, (&CardanoNodeConfig{}).validateGenesisConsistency())

	onlyByron := &CardanoNodeConfig{
		byronGenesis: &byron.ByronGenesis{StartTime: 1000},
	}
	require.NoError(t, onlyByron.validateGenesisConsistency())
}

func TestValidateGenesisConsistencyMatch(t *testing.T) {
	c := &CardanoNodeConfig{
		byronGenesis: &byron.ByronGenesis{StartTime: 1666656000},
		shelleyGenesis: &shelley.ShelleyGenesis{
			SystemStart: time.Unix(1666656000, 0).UTC(),
		},
	}
	require.NoError(t, c.validateGenesisConsistency())
}

func TestValidateGenesisConsistencyMismatch(t *testing.T) {
	c := &CardanoNodeConfig{
		byronGenesis: &byron.ByronGenesis{StartTime: 1506203091},
		shelleyGenesis: &shelley.ShelleyGenesis{
			SystemStart: time.Unix(1666656000, 0).UTC(),
		},
	}
	require.Error(t, c.validateGenesisConsistency())
}

func genesisRat(num, denom int64) cbor.Rat {
	return cbor.Rat{Rat: big.NewRat(num, denom)}
}

func TestValidateEpochLengthFitsNonceWindow(t *testing.T) {
	testCases := []struct {
		name        string
		k           int
		epochLength int
		f           *big.Rat
		expectErr   bool
	}{
		{
			// mainnet and preprod: 4k/f = 172800.
			name:        "mainnet",
			k:           2160,
			epochLength: 432000,
			f:           big.NewRat(1, 20),
		},
		{
			// preview and sanchonet: 4k/f = 34560.
			name:        "preview",
			k:           432,
			epochLength: 86400,
			f:           big.NewRat(1, 20),
		},
		{
			// The bundled devnet: 4k/f = 400.
			name:        "devnet",
			k:           100,
			epochLength: 600,
			f:           big.NewRat(1, 1),
		},
		{
			// The generated DevNet under internal/test/devnet: 4k/f = 400.
			name:        "internal devnet",
			k:           40,
			epochLength: 500,
			f:           big.NewRat(2, 5),
		},
		{
			// An epoch exactly as long as its window is already
			// degenerate: candidate_nonce.go pins the cutoff to the
			// epoch's first slot at >=, not >.
			name:        "window equals epoch length",
			k:           10,
			epochLength: 40,
			f:           big.NewRat(1, 1),
			expectErr:   true,
		},
		{
			// The devnet as shipped before this check existed:
			// 4k/f = 8640 against a 5 slot epoch.
			name:        "devnet before the fix",
			k:           2160,
			epochLength: 5,
			f:           big.NewRat(1, 1),
			expectErr:   true,
		},
		{
			// Ceiling division must round the window up, not down:
			// 4*10/3 = 13.33 does not fit a 13 slot epoch.
			name:        "window rounds up past epoch length",
			k:           10,
			epochLength: 13,
			f:           big.NewRat(3, 1),
			expectErr:   true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			c := &CardanoNodeConfig{
				shelleyGenesis: &shelley.ShelleyGenesis{
					SecurityParam:    testCase.k,
					EpochLength:      testCase.epochLength,
					ActiveSlotsCoeff: cbor.Rat{Rat: testCase.f},
				},
			}
			err := c.validateEpochLengthFitsNonceWindow()
			if testCase.expectErr {
				require.Error(t, err)
				require.Contains(
					t,
					err.Error(),
					"randomness stabilisation window",
				)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateEpochLengthFitsNonceWindowIncompleteGenesis(t *testing.T) {
	// A genesis missing any input to 4k/f is not one this check can speak
	// to, so it must pass rather than reject on a zero value.
	testCases := []struct {
		name    string
		genesis *shelley.ShelleyGenesis
	}{
		{name: "no shelley genesis"},
		{
			name: "zero security param",
			genesis: &shelley.ShelleyGenesis{
				EpochLength:      432000,
				ActiveSlotsCoeff: genesisRat(1, 20),
			},
		},
		{
			name: "zero epoch length",
			genesis: &shelley.ShelleyGenesis{
				SecurityParam:    2160,
				ActiveSlotsCoeff: genesisRat(1, 20),
			},
		},
		{
			name: "nil active slots coeff",
			genesis: &shelley.ShelleyGenesis{
				SecurityParam: 2160,
				EpochLength:   432000,
			},
		},
		{
			name: "zero active slots coeff",
			genesis: &shelley.ShelleyGenesis{
				SecurityParam:    2160,
				EpochLength:      432000,
				ActiveSlotsCoeff: genesisRat(0, 1),
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			c := &CardanoNodeConfig{shelleyGenesis: testCase.genesis}
			require.NoError(t, c.validateEpochLengthFitsNonceWindow())
		})
	}
}

// TestEmbeddedConfigsPassGenesisConsistency loads every embedded network
// config through the real loader, which runs validateGenesisConsistency. It
// is the check that the bundled configs themselves satisfy the invariants,
// not just that the validator computes them correctly.
func TestEmbeddedConfigsPassGenesisConsistency(t *testing.T) {
	entries, err := EmbeddedConfigFS.ReadDir(".")
	require.NoError(t, err)
	networks := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			networks = append(networks, entry.Name())
		}
	}
	require.NotEmpty(t, networks)

	for _, network := range networks {
		t.Run(network, func(t *testing.T) {
			cfg, err := NewCardanoNodeConfigFromEmbedFS(
				EmbeddedConfigFS,
				network+"/config.json",
			)
			require.NoError(t, err)
			require.NoError(t, cfg.validateGenesisConsistency())
		})
	}
}

// TestDevnetGenesisIsUsable pins the devnet timing parameters that make the
// bundled single node devnet usable. It shipped with a 5 slot epoch at 0.1
// second slots, which put an epoch boundary every half second and left the
// 4k/f window with no room inside the epoch at all.
func TestDevnetGenesisIsUsable(t *testing.T) {
	cfg, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigFS,
		"devnet/config.json",
	)
	require.NoError(t, err)

	shelleyGenesis := cfg.ShelleyGenesis()
	require.NotNil(t, shelleyGenesis)
	require.Equal(t, 600, shelleyGenesis.EpochLength)
	require.Equal(t, 100, shelleyGenesis.SecurityParam)
	require.Equal(
		t,
		big.NewRat(1, 1),
		shelleyGenesis.SlotLength.Rat,
		"one second slots",
	)

	byronGenesis := cfg.ByronGenesis()
	require.NotNil(t, byronGenesis)
	// Byron k is independent of the Shelley securityParam. Real networks
	// happen to set them equal and to give both eras the same epoch
	// duration, but neither is required, and Byron is inert on this devnet
	// anyway: TestShelleyHardForkAtEpoch is 0, so internal/node/load.go
	// returns the Shelley securityParam and the other two readers of Byron
	// k are Byron-era only. k=60 keeps a Byron epoch (10k slots) at the
	// same 600 slots as the Shelley epoch, with a round 1s Byron slot.
	require.Equal(t, 60, byronGenesis.ProtocolConsts.K)
	require.Equal(t, 1000, byronGenesis.BlockVersionData.SlotDuration)
}

// TestDevnetCostModelsCoverEveryPricedParameter checks that the devnet
// genesis prices at least every cost model parameter the evaluator knows
// about. The devnet declares protocol version 11 but shipped the original
// Conway models (PlutusV1 166, PlutusV2 175, PlutusV3 251 entries), so
// nothing added since Chang was priced and the script data hash a builder
// computed from current cost models never matched the chain.
func TestDevnetCostModelsCoverEveryPricedParameter(t *testing.T) {
	cfg, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigFS,
		"devnet/config.json",
	)
	require.NoError(t, err)

	shelleyGenesis := cfg.ShelleyGenesis()
	require.NotNil(t, shelleyGenesis)
	require.Equal(
		t,
		uint(11),
		shelleyGenesis.ProtocolParameters.ProtocolVersion.Major,
		"the cost models below are the ones the public networks run at this protocol version",
	)

	alonzoGenesis := cfg.AlonzoGenesis()
	require.NotNil(t, alonzoGenesis)
	conwayGenesis := cfg.ConwayGenesis()
	require.NotNil(t, conwayGenesis)

	testCases := []struct {
		name      string
		model     []int64
		langVer   lang.LanguageVersion
		wantCount int
	}{
		{
			name:      "PlutusV1",
			model:     alonzoGenesis.CostModels["PlutusV1"],
			langVer:   lang.LanguageVersionV1,
			wantCount: 332,
		},
		{
			name:      "PlutusV2",
			model:     alonzoGenesis.CostModels["PlutusV2"],
			langVer:   lang.LanguageVersionV2,
			wantCount: 332,
		},
		{
			name:      "PlutusV3",
			model:     conwayGenesis.PlutusV3CostModel,
			langVer:   lang.LanguageVersionV3,
			wantCount: 350,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// The absolute count is what mainnet, preprod and preview all
			// publish at protocol version 11.
			require.Len(t, testCase.model, testCase.wantCount)
			// plutigo's parameter table is the set the evaluator can price.
			// costModelFromList stops at len(data), so a model shorter than
			// the table silently leaves builtins at their default cost.
			require.GreaterOrEqual(
				t,
				len(testCase.model),
				len(lang.GetParamNamesForVersion(testCase.langVer)),
				"cost model must price every parameter plutigo knows",
			)
		})
	}
}
