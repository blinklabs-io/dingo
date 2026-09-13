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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

// Byron/Shelley security-parameter values that pass
// validateSecurityParameters, so tests targeting an unrelated invariant in
// validateGenesisConsistency do not also exercise that one.
const (
	validByronK           = 432
	validShelleySecParam  = 432
	validShelleyActiveNum = 1
	validShelleyActiveDen = 20
)

func TestValidateGenesisConsistencyNoGenesis(t *testing.T) {
	t.Parallel()

	// With neither (or only one) genesis loaded there is nothing to
	// cross-check, so the consistency check must pass.
	require.NoError(t, (&CardanoNodeConfig{}).validateGenesisConsistency())

	onlyByron := &CardanoNodeConfig{
		byronGenesis: &byron.ByronGenesis{
			StartTime: 1000,
			ProtocolConsts: byron.ByronGenesisProtocolConsts{
				K: validByronK,
			},
		},
	}
	require.NoError(t, onlyByron.validateGenesisConsistency())
}

func TestValidateGenesisConsistencyMatch(t *testing.T) {
	t.Parallel()

	c := &CardanoNodeConfig{
		byronGenesis: &byron.ByronGenesis{
			StartTime: 1666656000,
			ProtocolConsts: byron.ByronGenesisProtocolConsts{
				K: validByronK,
			},
		},
		shelleyGenesis: &shelley.ShelleyGenesis{
			SystemStart:      time.Unix(1666656000, 0).UTC(),
			MaxKESEvolutions: 62,
			SecurityParam:    validShelleySecParam,
			ActiveSlotsCoeff: genesisRat(
				validShelleyActiveNum,
				validShelleyActiveDen,
			),
		},
	}
	require.NoError(t, c.validateGenesisConsistency())
}

func TestValidateGenesisConsistencyMismatch(t *testing.T) {
	t.Parallel()

	c := &CardanoNodeConfig{
		byronGenesis: &byron.ByronGenesis{
			StartTime: 1506203091,
			ProtocolConsts: byron.ByronGenesisProtocolConsts{
				K: validByronK,
			},
		},
		shelleyGenesis: &shelley.ShelleyGenesis{
			SystemStart:      time.Unix(1666656000, 0).UTC(),
			MaxKESEvolutions: 62,
			SecurityParam:    validShelleySecParam,
			ActiveSlotsCoeff: genesisRat(
				validShelleyActiveNum,
				validShelleyActiveDen,
			),
		},
	}
	err := c.validateGenesisConsistency()
	require.Error(t, err)
	require.ErrorContains(t, err, "genesis system start mismatch")
}

// TestValidateGenesisConsistencyRejectsMissingMaxKESEvolutions is a
// regression test for a human-review finding: nothing rejected a Shelley
// genesis with a missing or non-positive maxKESEvolutions at load time,
// so the failure only surfaced once header verification ran -- on every
// single header, since issue #3528 made header crypto verification
// unconditional -- with nothing naming the genesis field as the cause.
func TestValidateGenesisConsistencyRejectsMissingMaxKESEvolutions(t *testing.T) {
	t.Parallel()

	t.Run("zero maxKESEvolutions is rejected", func(t *testing.T) {
		t.Parallel()
		c := &CardanoNodeConfig{
			shelleyGenesis: &shelley.ShelleyGenesis{},
		}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "maxKESEvolutions must be positive")
	})

	t.Run("negative maxKESEvolutions is rejected", func(t *testing.T) {
		t.Parallel()
		c := &CardanoNodeConfig{
			shelleyGenesis: &shelley.ShelleyGenesis{MaxKESEvolutions: -1},
		}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "maxKESEvolutions must be positive")
	})

	t.Run("positive maxKESEvolutions is accepted", func(t *testing.T) {
		t.Parallel()
		c := &CardanoNodeConfig{
			shelleyGenesis: &shelley.ShelleyGenesis{
				MaxKESEvolutions: 62,
				SecurityParam:    validShelleySecParam,
				ActiveSlotsCoeff: genesisRat(
					validShelleyActiveNum,
					validShelleyActiveDen,
				),
			},
		}
		require.NoError(t, c.validateGenesisConsistency())
	})

	t.Run("no shelley genesis is not this check's concern", func(t *testing.T) {
		t.Parallel()
		require.NoError(
			t,
			(&CardanoNodeConfig{}).validateGenesisConsistency(),
		)
	})
}

func genesisRat(num, denom int64) cbor.Rat {
	return cbor.Rat{Rat: big.NewRat(num, denom)}
}

// validShelleyGenesisForSecurityParamTests returns a Shelley genesis whose
// security-parameter fields are valid, so each sub-test below only varies
// the single field it names.
func validShelleyGenesisForSecurityParamTests() *shelley.ShelleyGenesis {
	return &shelley.ShelleyGenesis{
		MaxKESEvolutions: 62,
		SecurityParam:    validShelleySecParam,
		ActiveSlotsCoeff: genesisRat(
			validShelleyActiveNum,
			validShelleyActiveDen,
		),
	}
}

// TestValidateGenesisConsistencyRejectsInvalidSecurityParameters is a
// regression test for issue #1649 case R2. Neither
// LoadShelleyGenesisFromReader/LoadByronGenesisFromReader (used broadly by
// unit tests to build deliberately-invalid fixtures for other guards, e.g.
// TestVerifyBlockLeaderEligibility_ZeroActiveSlotsCoeffRejects) nor
// genesis_consistency.go rejected a non-positive Shelley securityParam or an
// activeSlotsCoeff outside (0, 1], or a non-positive Byron k. That let an
// invalid `serve` genesis reach LedgerState, whose securityParamForEra /
// calculateStabilityWindowForEra (ledger/state.go) treat the value as
// "unavailable" and silently substitute blockfetchBatchSlotThresholdDefault
// (50000) as k and as the stability window for chain-selection rollback
// depth, the consumed-UTxO prune window, and the hard-fork safe zone --
// rather than failing the way internal/node/load.go's
// loadSecurityParamForConfig and ledger/eras/shape.go's
// StabilityWindowForEra already do for the same inputs.
func TestValidateGenesisConsistencyRejectsInvalidSecurityParameters(t *testing.T) {
	t.Parallel()

	t.Run("zero shelley security param is rejected", func(t *testing.T) {
		t.Parallel()
		g := validShelleyGenesisForSecurityParamTests()
		g.SecurityParam = 0
		c := &CardanoNodeConfig{shelleyGenesis: g}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "security parameter")
	})

	t.Run("negative shelley security param is rejected", func(t *testing.T) {
		t.Parallel()
		g := validShelleyGenesisForSecurityParamTests()
		g.SecurityParam = -1
		c := &CardanoNodeConfig{shelleyGenesis: g}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "security parameter")
	})

	t.Run("zero activeSlotsCoeff is rejected", func(t *testing.T) {
		t.Parallel()
		g := validShelleyGenesisForSecurityParamTests()
		g.ActiveSlotsCoeff = genesisRat(0, 1)
		c := &CardanoNodeConfig{shelleyGenesis: g}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "activeSlotsCoeff")
	})

	t.Run("nil activeSlotsCoeff is rejected", func(t *testing.T) {
		t.Parallel()
		g := validShelleyGenesisForSecurityParamTests()
		g.ActiveSlotsCoeff = cbor.Rat{}
		c := &CardanoNodeConfig{shelleyGenesis: g}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "activeSlotsCoeff")
	})

	t.Run("activeSlotsCoeff above one is rejected", func(t *testing.T) {
		t.Parallel()
		g := validShelleyGenesisForSecurityParamTests()
		g.ActiveSlotsCoeff = genesisRat(3, 2)
		c := &CardanoNodeConfig{shelleyGenesis: g}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "activeSlotsCoeff")
	})

	t.Run("activeSlotsCoeff exactly one is accepted", func(t *testing.T) {
		t.Parallel()
		g := validShelleyGenesisForSecurityParamTests()
		g.ActiveSlotsCoeff = genesisRat(1, 1)
		c := &CardanoNodeConfig{shelleyGenesis: g}
		require.NoError(t, c.validateGenesisConsistency())
	})

	t.Run("zero byron k is rejected even without shelley genesis", func(t *testing.T) {
		t.Parallel()
		c := &CardanoNodeConfig{
			byronGenesis: &byron.ByronGenesis{
				ProtocolConsts: byron.ByronGenesisProtocolConsts{K: 0},
			},
		}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "byron genesis: security parameter")
	})

	t.Run("negative byron k is rejected", func(t *testing.T) {
		t.Parallel()
		shelleyGenesis := validShelleyGenesisForSecurityParamTests()
		shelleyGenesis.SystemStart = time.Unix(1666656000, 0).UTC()
		c := &CardanoNodeConfig{
			byronGenesis: &byron.ByronGenesis{
				StartTime: 1666656000,
				ProtocolConsts: byron.ByronGenesisProtocolConsts{
					K: -1,
				},
			},
			shelleyGenesis: shelleyGenesis,
		}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "byron genesis: security parameter")
	})

	t.Run("valid byron and shelley security parameters are accepted", func(t *testing.T) {
		t.Parallel()
		shelleyGenesis := validShelleyGenesisForSecurityParamTests()
		shelleyGenesis.SystemStart = time.Unix(1666656000, 0).UTC()
		c := &CardanoNodeConfig{
			byronGenesis: &byron.ByronGenesis{
				StartTime: 1666656000,
				ProtocolConsts: byron.ByronGenesisProtocolConsts{
					K: validByronK,
				},
			},
			shelleyGenesis: shelleyGenesis,
		}
		require.NoError(t, c.validateGenesisConsistency())
	})
}

// TestNewCardanoNodeConfigFromFileRejectsInvalidSecurityParam proves the
// rejection reaches the real `serve` config-load entry point
// (NewCardanoNodeConfigFromFile -> loadGenesisConfigs), not only the
// unit-level validator: a Shelley genesis on disk with securityParam 0 must
// fail to load, where before this fix it loaded silently and left
// LedgerState to substitute a fabricated k=50000.
func TestNewCardanoNodeConfigFromFileRejectsInvalidSecurityParam(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 0,
		"maxKESEvolutions": 62,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	require.NoError(t, os.WriteFile(
		filepath.Join(tmpDir, "shelley-genesis.json"),
		[]byte(shelleyGenesisJSON),
		0o600,
	))
	configJSON := `{"ShelleyGenesisFile": "shelley-genesis.json"}`
	configPath := filepath.Join(tmpDir, "config.json")
	require.NoError(t, os.WriteFile(configPath, []byte(configJSON), 0o600))

	_, err := NewCardanoNodeConfigFromFile(configPath)
	require.Error(t, err)
	require.ErrorContains(t, err, "security parameter")
}

// TestValidateGenesisConsistencyRejectsOverflowingStabilityWindow pins the
// uint64 bound on the 4k/f window. A positive k and an in-range
// activeSlotsCoeff can still produce a window that does not fit in uint64;
// calculateStabilityWindowForEra (ledger/state.go) then substitutes 50000
// and nonceStabilityWindow (ledger/candidate_nonce.go) returns 0.
// validateEpochLengthFitsNonceWindow bounds the window only when
// epochLength is positive, so these fixtures leave epochLength at zero.
func TestValidateGenesisConsistencyRejectsOverflowingStabilityWindow(
	t *testing.T,
) {
	t.Parallel()

	t.Run("4k/f overflowing uint64 is rejected", func(t *testing.T) {
		t.Parallel()
		// 4 * 1 / (1/2^62) = 2^64; 3k/f = 3*2^62 still fits.
		g := validShelleyGenesisForSecurityParamTests()
		g.SecurityParam = 1
		g.ActiveSlotsCoeff = genesisRat(1, 1<<62)
		c := &CardanoNodeConfig{shelleyGenesis: g}
		err := c.validateGenesisConsistency()
		require.Error(t, err)
		require.ErrorContains(t, err, "stability window")
	})

	t.Run("4k/f within uint64 is accepted", func(t *testing.T) {
		t.Parallel()
		// 4 * 1 / (1/(2^62-1)) = 2^64-4.
		g := validShelleyGenesisForSecurityParamTests()
		g.SecurityParam = 1
		g.ActiveSlotsCoeff = genesisRat(1, 1<<62-1)
		c := &CardanoNodeConfig{shelleyGenesis: g}
		require.NoError(t, c.validateGenesisConsistency())
	})
}

func TestValidateEpochLengthFitsNonceWindow(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
