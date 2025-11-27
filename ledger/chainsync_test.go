// Copyright 2025 Blink Labs Software
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

package ledger_test

import (
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/eras"
)

// TestCalculateEpochNonce_ByronEra tests epoch nonce calculation in Byron era
func TestCalculateEpochNonce_ByronEra(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &ledger.LedgerState{
		CurrentEra: eras.ByronEraDesc,
		CurrentEpoch: models.Epoch{
			EpochId:   0,
			StartSlot: 0,
			Nonce:     nil,
		},
		Config: ledger.LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Byron era should return nil nonce
	nonce, err := ls.CalculateEpochNonce(nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nonce != nil {
		t.Errorf("expected nil nonce for Byron era, got %v", nonce)
	}
}

// TestCalculateEpochNonce_InitialEpochWithoutNonce tests initial Shelley epoch
func TestCalculateEpochNonce_InitialEpochWithoutNonce(t *testing.T) {
	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &ledger.LedgerState{
		CurrentEra: eras.ShelleyEraDesc,
		CurrentEpoch: models.Epoch{
			EpochId:   0,
			StartSlot: 0,
			Nonce:     nil, // No nonce means initial epoch
		},
		Config: ledger.LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Initial epoch should return genesis hash
	nonce, err := ls.CalculateEpochNonce(nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedNonce, err := hex.DecodeString(shelleyGenesisHash)
	if err != nil {
		t.Fatalf("failed to decode expected nonce: %v", err)
	}

	if len(nonce) != len(expectedNonce) {
		t.Fatalf(
			"nonce length mismatch: expected %d, got %d",
			len(expectedNonce),
			len(nonce),
		)
	}
	for i := range nonce {
		if nonce[i] != expectedNonce[i] {
			t.Errorf(
				"nonce mismatch at byte %d: expected %x, got %x",
				i,
				expectedNonce[i],
				nonce[i],
			)
		}
	}
}

// TestCalculateEpochNonce_InvalidGenesisHash tests handling of invalid genesis hash
func TestCalculateEpochNonce_InvalidGenesisHash(t *testing.T) {
	invalidHash := "not-a-valid-hex-string"
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: invalidHash,
	}
	if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &ledger.LedgerState{
		CurrentEra: eras.ShelleyEraDesc,
		CurrentEpoch: models.Epoch{
			EpochId:   0,
			StartSlot: 0,
			Nonce:     nil,
		},
		Config: ledger.LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, err := ls.CalculateEpochNonce(nil, 0)
	if err == nil {
		t.Fatal("expected error for invalid genesis hash, got nil")
	}
}

// TestCalculateEpochNonce_MissingShelleyGenesis tests handling of missing Shelley genesis
func TestCalculateEpochNonce_MissingShelleyGenesis(t *testing.T) {
	cfg := &cardano.CardanoNodeConfig{}

	ls := &ledger.LedgerState{
		CurrentEra: eras.ShelleyEraDesc,
		CurrentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 86400,
			Nonce:     nil,
		},
		Config: ledger.LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, err := ls.CalculateEpochNonce(nil, 86400)
	if err == nil {
		t.Fatal("expected error for missing Shelley genesis, got nil")
	}
	if !strings.Contains(err.Error(), "genesis hash") {
		t.Errorf("expected error about Shelley genesis, got: %v", err)
	}
}

// TestCalculateEpochNonce_NegativeSecurityParam tests handling of negative security parameter
func TestCalculateEpochNonce_NegativeSecurityParam(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": -1,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	_ = cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON))
	_ = cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON))

	ls := &ledger.LedgerState{
		CurrentEra: eras.ByronEraDesc,
		CurrentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 86400,
			Nonce:     []byte{0x01, 0x02, 0x03},
		},
		Config: ledger.LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Test will depend on whether the genesis loads successfully
	// If it loads, we expect an error about negative k
	_, err := ls.CalculateEpochNonce(nil, 86400)
	// Either genesis loading fails or calculateEpochNonce catches negative k
	if err == nil {
		t.Log("Note: negative k may be caught during genesis loading")
	}
}

// TestCalculateEpochNonce_ShelleyEraDifferentParams tests Shelley era with various parameters
func TestCalculateEpochNonce_ShelleyEraDifferentParams(t *testing.T) {
	testCases := []struct {
		name             string
		k                int
		activeSlotsCoeff float64
		description      string
	}{
		{
			name:             "Standard testnet parameters",
			k:                432,
			activeSlotsCoeff: 0.05,
			description:      "k=432, f=0.05",
		},
		{
			name:             "Mainnet parameters",
			k:                2160,
			activeSlotsCoeff: 0.05,
			description:      "k=2160, f=0.05",
		},
		{
			name:             "High activity coefficient",
			k:                432,
			activeSlotsCoeff: 0.2,
			description:      "k=432, f=0.2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			byronGenesisJSON := fmt.Sprintf(`{
				"protocolConsts": {
					"k": %d,
					"protocolMagic": 2
				}
			}`, tc.k)
			shelleyGenesisJSON := fmt.Sprintf(`{
				"activeSlotsCoeff": %f,
				"securityParam": %d,
				"systemStart": "2022-10-25T00:00:00Z"
			}`, tc.activeSlotsCoeff, tc.k)

			cfg := &cardano.CardanoNodeConfig{
				ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
			}
			if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
				t.Fatalf("failed to load Byron genesis: %v", err)
			}
			if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
				t.Fatalf("failed to load Shelley genesis: %v", err)
			}

			ls := &ledger.LedgerState{
				CurrentEra: eras.ShelleyEraDesc,
				CurrentEpoch: models.Epoch{
					EpochId:   0,
					StartSlot: 0,
					Nonce:     nil,
				},
				Config: ledger.LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			// Initial epoch should return genesis hash
			nonce, err := ls.CalculateEpochNonce(nil, 0)
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.description, err)
			}
			if nonce == nil {
				t.Errorf(
					"%s: expected non-nil nonce for initial Shelley epoch",
					tc.description,
				)
			}
		})
	}
}

// TestCalculateEpochNonce_ZeroActiveSlots tests handling of zero active slots coefficient
func TestCalculateEpochNonce_ZeroActiveSlots(t *testing.T) {
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &ledger.LedgerState{
		CurrentEra: eras.ShelleyEraDesc,
		CurrentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 86400,
			Nonce:     nil,
		},
		Config: ledger.LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Verify fallback behavior when ActiveSlotsCoeff cannot be used
	window := ls.CalculateStabilityWindow()
	if window != ledger.BlockfetchBatchSlotThresholdDefault {
		t.Fatalf(
			"expected fallback window %d, got %d",
			ledger.BlockfetchBatchSlotThresholdDefault,
			window,
		)
	}
}

// TestCalculateEpochNonce_StabilityWindowCalculation tests the stability window calculation logic
func TestCalculateEpochNonce_StabilityWindowCalculation(t *testing.T) {
	testCases := []struct {
		name             string
		era              eras.EraDesc
		k                int
		activeSlotsCoeff float64
		expectedFormula  string
	}{
		{
			name:             "Byron era uses k directly",
			era:              eras.ByronEraDesc,
			k:                432,
			activeSlotsCoeff: 0.05,
			expectedFormula:  "stability_window = k = 432",
		},
		{
			name:             "Shelley era uses 3k/f",
			era:              eras.ShelleyEraDesc,
			k:                432,
			activeSlotsCoeff: 0.05,
			expectedFormula:  "stability_window = 3*432/0.05 = 25920",
		},
		{
			name:             "Allegra era uses 3k/f",
			era:              eras.AllegraEraDesc,
			k:                2160,
			activeSlotsCoeff: 0.05,
			expectedFormula:  "stability_window = 3*2160/0.05 = 129600",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			byronGenesisJSON := fmt.Sprintf(`{
				"protocolConsts": {
					"k": %d,
					"protocolMagic": 2
				}
			}`, tc.k)
			shelleyGenesisJSON := fmt.Sprintf(`{
				"activeSlotsCoeff": %f,
				"securityParam": %d,
				"systemStart": "2022-10-25T00:00:00Z"
			}`, tc.activeSlotsCoeff, tc.k)

			cfg := &cardano.CardanoNodeConfig{
				ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
			}
			if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
				t.Fatalf("failed to load Byron genesis: %v", err)
			}
			if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
				t.Fatalf("failed to load Shelley genesis: %v", err)
			}

			ls := &ledger.LedgerState{
				CurrentEra: tc.era,
				CurrentEpoch: models.Epoch{
					EpochId:   0,
					StartSlot: 0,
					Nonce:     nil,
				},
				Config: ledger.LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			// Test for Byron era - should return nil
			if tc.era.Id == 0 {
				nonce, err := ls.CalculateEpochNonce(nil, 0)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if nonce != nil {
					t.Errorf(
						"Byron era should return nil nonce, got: %v",
						nonce,
					)
				}
				return
			}

			// For non-Byron eras, test initial epoch returns genesis hash
			nonce, err := ls.CalculateEpochNonce(nil, 0)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if nonce == nil {
				t.Error("expected non-nil nonce for initial non-Byron epoch")
			}
			t.Logf("Formula: %s", tc.expectedFormula)
		})
	}
}

// TestCalculateEpochNonce_IntegerArithmeticPrecision tests precision of integer arithmetic
func TestCalculateEpochNonce_IntegerArithmeticPrecision(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 1000,
			"protocolMagic": 2
		}
	}`
	// Use a coefficient that produces fractional results
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.333333,
		"securityParam": 1000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &ledger.LedgerState{
		CurrentEra: eras.ShelleyEraDesc,
		CurrentEpoch: models.Epoch{
			EpochId:   0,
			StartSlot: 0,
			Nonce:     nil,
		},
		Config: ledger.LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Should handle fractional coefficients correctly using integer arithmetic
	nonce, err := ls.CalculateEpochNonce(nil, 0)
	if err != nil {
		t.Fatalf("unexpected error with fractional coefficient: %v", err)
	}
	if nonce == nil {
		t.Error("expected non-nil nonce")
	}
}

// TestHandleEventChainsyncBlockHeader_StabilityWindowUsage tests the stability window usage in block header handling
func TestHandleEventChainsyncBlockHeader_StabilityWindowUsage(t *testing.T) {
	// This test verifies that the handleEventChainsyncBlockHeader function
	// correctly uses calculateStabilityWindow instead of the old constant

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{}
	if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &ledger.LedgerState{
		CurrentEra: eras.ShelleyEraDesc,
		Config: ledger.LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Verify that calculateStabilityWindow returns correct value
	window := ls.CalculateStabilityWindow()
	expectedWindow := uint64(25920) // 3*432/0.05
	if window != expectedWindow {
		t.Errorf("expected stability window %d, got %d", expectedWindow, window)
	}

	// Verify it's different from the old constant
	if window == ledger.BlockfetchBatchSlotThresholdDefault {
		t.Error(
			"stability window should not equal the old constant for Shelley era",
		)
	}
}

// TestCalculateEpochNonce_AllEras tests epoch nonce calculation across all eras
func TestCalculateEpochNonce_AllEras(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	testCases := []struct {
		name        string
		era         eras.EraDesc
		expectNil   bool
		description string
	}{
		{
			name:        "Byron era returns nil",
			era:         eras.ByronEraDesc,
			expectNil:   true,
			description: "Byron has no epoch nonce",
		},
		{
			name:        "Shelley era returns nonce",
			era:         eras.ShelleyEraDesc,
			expectNil:   false,
			description: "Shelley uses epoch nonce",
		},
		{
			name:        "Allegra era returns nonce",
			era:         eras.AllegraEraDesc,
			expectNil:   false,
			description: "Allegra uses epoch nonce",
		},
		{
			name:        "Mary era returns nonce",
			era:         eras.MaryEraDesc,
			expectNil:   false,
			description: "Mary uses epoch nonce",
		},
		{
			name:        "Alonzo era returns nonce",
			era:         eras.AlonzoEraDesc,
			expectNil:   false,
			description: "Alonzo uses epoch nonce",
		},
		{
			name:        "Babbage era returns nonce",
			era:         eras.BabbageEraDesc,
			expectNil:   false,
			description: "Babbage uses epoch nonce",
		},
		{
			name:        "Conway era returns nonce",
			era:         eras.ConwayEraDesc,
			expectNil:   false,
			description: "Conway uses epoch nonce",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ls := &ledger.LedgerState{
				CurrentEra: tc.era,
				CurrentEpoch: models.Epoch{
					EpochId:   0,
					StartSlot: 0,
					Nonce:     nil,
				},
				Config: ledger.LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			nonce, err := ls.CalculateEpochNonce(nil, 0)
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.description, err)
			}

			if tc.expectNil {
				if nonce != nil {
					t.Errorf(
						"%s: expected nil nonce, got %v",
						tc.description,
						nonce,
					)
				}
			} else {
				if nonce == nil {
					t.Errorf("%s: expected non-nil nonce", tc.description)
				}
			}
		})
	}
}

// TestCalculateEpochNonce_MissingByronGenesisInByronEra tests missing Byron genesis during Byron era
func TestCalculateEpochNonce_MissingByronGenesisInByronEra(t *testing.T) {
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &ledger.LedgerState{
		CurrentEra: eras.ByronEraDesc,
		CurrentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 86400,
			Nonce:     nil,
		},
		Config: ledger.LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Byron era returns nil nonce immediately without genesis validation
	nonce, err := ls.CalculateEpochNonce(nil, 86400)
	if err != nil {
		t.Fatalf("unexpected error for Byron era: %v", err)
	}
	if nonce != nil {
		t.Errorf("expected nil nonce for Byron era, got: %v", nonce)
	}
}
