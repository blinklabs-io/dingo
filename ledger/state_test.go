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

package ledger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

func TestLedgerProcessBlocksFromSourceReturnsNilWhenReaderCloses(
	t *testing.T,
) {
	ls := &LedgerState{
		validationEnabled: true,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	readChainResultCh := make(chan readChainResult, 1)
	close(readChainResultCh)

	err := ls.ledgerProcessBlocksFromSource(
		context.Background(),
		readChainResultCh,
	)
	require.NoError(t, err)
}

func TestHandleLedgerProcessBlocksErrorLogsPersistentValidationFailure(
	t *testing.T,
) {
	haltErr := fmt.Errorf("process block batch: %w", errHaltLedgerPipeline)
	fatalCalled := false
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			FatalErrorFunc: func(error) {
				fatalCalled = true
			},
		},
	}

	ls.handleLedgerProcessBlocksError(haltErr)
	require.False(t, fatalCalled)
}

func TestHandleLedgerProcessBlocksErrorDoesNotReportFatalErrors(
	t *testing.T,
) {
	fatalCalled := false
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			FatalErrorFunc: func(error) {
				fatalCalled = true
			},
		},
	}

	ls.handleLedgerProcessBlocksError(errRestartLedgerPipeline)
	require.False(t, fatalCalled)

	ls.handleLedgerProcessBlocksError(errors.New("transient"))
	require.False(t, fatalCalled)
}

// It verifies that calculating the stability window is synchronized with
// concurrent currentEra updates from block processing.
func TestCalculateStabilityWindowConcurrentCurrentEraAccess(t *testing.T) {
	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Go(func() {
		<-start
		for i := range 100 {
			ls.Lock()
			if i%2 == 0 {
				ls.currentEra = eras.BabbageEraDesc
			} else {
				ls.currentEra = eras.ConwayEraDesc
			}
			ls.Unlock()
		}
		close(done)
	})

	for range 8 {
		wg.Go(func() {
			<-start
			for {
				select {
				case <-done:
					return
				default:
					_ = ls.calculateStabilityWindow()
				}
			}
		})
	}

	close(start)
	wg.Wait()
}

func TestShouldSkipPhase2ValidationForBlockUsesSecurityParam(t *testing.T) {
	const securityParam uint64 = 37
	cfg := newTestShelleyGenesisCfg(t)
	cfg.ShelleyGenesis().SecurityParam = int(securityParam)

	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	const referenceBlockNumber uint64 = 1000
	immutableTipBlockNumber := referenceBlockNumber - securityParam

	require.True(t, ls.shouldSkipPhase2ValidationForBlock(
		immutableTipBlockNumber,
		referenceBlockNumber,
		eras.ShelleyEraDesc.Id,
	))
	require.False(t, ls.shouldSkipPhase2ValidationForBlock(
		immutableTipBlockNumber+1,
		referenceBlockNumber,
		eras.ShelleyEraDesc.Id,
	))
	require.False(t, ls.shouldSkipPhase2ValidationForBlock(
		0,
		securityParam-1,
		eras.ShelleyEraDesc.Id,
	))
}

func TestShouldSkipPhase2ValidationForBlockRequiresSecurityParam(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	require.False(t, ls.shouldSkipPhase2ValidationForBlock(
		0,
		1000,
		eras.ShelleyEraDesc.Id,
	))
}

func TestShouldSkipPhase2ValidationForBlockAtCurrentTipRefreshesChainTip(
	t *testing.T,
) {
	const securityParam uint64 = 2
	cfg := newTestShelleyGenesisCfg(t)
	cfg.ShelleyGenesis().SecurityParam = int(securityParam)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{
			securityParam: int(securityParam),
		}),
	)

	rawBlocks := make([]chain.RawBlock, 0, 5)
	var prevHash []byte
	for blockNumber := uint64(1); blockNumber <= 5; blockNumber++ {
		block := makeTestBlock(blockNumber*10, blockNumber)
		block.PrevHash = prevHash
		rawBlocks = append(rawBlocks, chain.RawBlock{
			Slot:        block.Slot,
			Hash:        block.Hash,
			BlockNumber: block.Number,
			Type:        block.Type,
			PrevHash:    block.PrevHash,
			Cbor:        block.Cbor,
		})
		prevHash = block.Hash
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(rawBlocks))

	ls := &LedgerState{
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	require.True(t, ls.shouldSkipPhase2ValidationForBlockAtCurrentTip(
		3,
		eras.ShelleyEraDesc.Id,
	))

	rollbackPoint := ocommon.NewPoint(
		rawBlocks[3].Slot,
		rawBlocks[3].Hash,
	)
	require.NoError(t, cm.PrimaryChain().Rollback(rollbackPoint))

	require.False(t, ls.shouldSkipPhase2ValidationForBlockAtCurrentTip(
		3,
		eras.ShelleyEraDesc.Id,
	))
}

// TestCalculateStabilityWindow_ByronEra tests the stability window calculation for Byron era
func TestCalculateStabilityWindow_ByronEra(t *testing.T) {
	testCases := []struct {
		name           string
		k              int
		expectedWindow uint64
	}{
		{
			name:           "Byron era with k=432",
			k:              432,
			expectedWindow: 864,
		},
		{
			name:           "Byron era with k=2160",
			k:              2160,
			expectedWindow: 4320,
		},
		{
			name:           "Byron era with k=1",
			k:              1,
			expectedWindow: 2,
		},
		{
			name:           "Byron era with k=100",
			k:              100,
			expectedWindow: 200,
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

			ls := &LedgerState{
				currentEra: eras.ByronEraDesc, // Byron era has Id = 0
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			result := ls.calculateStabilityWindow()
			if result != tc.expectedWindow {
				t.Errorf(
					"expected stability window %d, got %d",
					tc.expectedWindow,
					result,
				)
			}
		})
	}
}

// TestCalculateStabilityWindow_ShelleyEra tests the stability window calculation for Shelley+ eras
func TestCalculateStabilityWindow_ShelleyEra(t *testing.T) {
	testCases := []struct {
		name             string
		k                int
		activeSlotsCoeff float64
		expectedWindow   uint64
		description      string
	}{
		{
			name:             "Shelley era with k=432, f=0.05",
			k:                432,
			activeSlotsCoeff: 0.05,
			// 3k/f = 3*432/0.05 = 1296/0.05 = 25920
			expectedWindow: 25920,
			description:    "Standard Shelley parameters",
		},
		{
			name:             "Shelley era with k=2160, f=0.05",
			k:                2160,
			activeSlotsCoeff: 0.05,
			// 3k/f = 3*2160/0.05 = 6480/0.05 = 129600
			expectedWindow: 129600,
			description:    "Mainnet parameters",
		},
		{
			name:             "Shelley era with k=100, f=0.1",
			k:                100,
			activeSlotsCoeff: 0.1,
			// 3k/f = 3*100/0.1 = 300/0.1 = 3000
			expectedWindow: 3000,
			description:    "Higher active slots coefficient",
		},
		{
			name:             "Shelley era with k=432, f=0.2",
			k:                432,
			activeSlotsCoeff: 0.2,
			// 3k/f = 3*432/0.2 = 1296/0.2 = 6480
			expectedWindow: 6480,
			description:    "Even higher active slots coefficient",
		},
		{
			name:             "Shelley era with k=50, f=0.5",
			k:                50,
			activeSlotsCoeff: 0.5,
			// 3k/f = 3*50/0.5 = 150/0.5 = 300
			expectedWindow: 300,
			description:    "Very high active slots coefficient",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			byronGenesisJSON := `{
				"protocolConsts": {
					"k": 432,
					"protocolMagic": 2
				}
			}`

			shelleyGenesisJSON := fmt.Sprintf(`{
				"activeSlotsCoeff": %f,
				"securityParam": %d,
				"systemStart": "2022-10-25T00:00:00Z"
			}`, tc.activeSlotsCoeff, tc.k)

			cfg := &cardano.CardanoNodeConfig{}
			if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
				t.Fatalf("failed to load Byron genesis: %v", err)
			}
			if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
				t.Fatalf("failed to load Shelley genesis: %v", err)
			}

			ls := &LedgerState{
				currentEra: eras.ShelleyEraDesc, // Shelley era has Id = 1
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			result := ls.calculateStabilityWindow()
			if result != tc.expectedWindow {
				t.Errorf(
					"%s: expected stability window %d, got %d",
					tc.description,
					tc.expectedWindow,
					result,
				)
			}
		})
	}
}

// TestCalculateStabilityWindow_EdgeCases tests edge cases and error conditions
func TestCalculateStabilityWindow_EdgeCases(t *testing.T) {
	t.Run("Missing Byron genesis returns default", func(t *testing.T) {
		cfg := &cardano.CardanoNodeConfig{}
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.05,
			"securityParam": 432,
			"systemStart": "2022-10-25T00:00:00Z"
		}`
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		ls := &LedgerState{
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		if result != blockfetchBatchSlotThresholdDefault {
			t.Errorf(
				"expected default threshold %d, got %d",
				blockfetchBatchSlotThresholdDefault,
				result,
			)
		}
	})

	t.Run("Missing Shelley genesis returns default", func(t *testing.T) {
		cfg := &cardano.CardanoNodeConfig{}
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}

		ls := &LedgerState{
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		if result != 864 {
			t.Errorf("expected default threshold %d, got %d", 864, result)
		}
	})

	t.Run("Zero k in Byron era returns default", func(t *testing.T) {
		cfg := &cardano.CardanoNodeConfig{}
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 0,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.05,
			"securityParam": 432,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		_ = cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON))
		_ = cfg.LoadShelleyGenesisFromReader(
			strings.NewReader(shelleyGenesisJSON),
		)

		ls := &LedgerState{
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		if result != blockfetchBatchSlotThresholdDefault {
			t.Errorf(
				"expected default threshold %d for zero k, got %d",
				blockfetchBatchSlotThresholdDefault,
				result,
			)
		}
	})

	t.Run("Zero k in Shelley era returns default", func(t *testing.T) {
		cfg := &cardano.CardanoNodeConfig{}
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.05,
			"securityParam": 0,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		_ = cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON))
		_ = cfg.LoadShelleyGenesisFromReader(
			strings.NewReader(shelleyGenesisJSON),
		)

		ls := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		if result != blockfetchBatchSlotThresholdDefault {
			t.Errorf(
				"expected default threshold %d for zero k, got %d",
				blockfetchBatchSlotThresholdDefault,
				result,
			)
		}
	})
}

// TestCalculateStabilityWindow_ActiveSlotsCoefficientEdgeCases tests various active slots coefficient scenarios
func TestCalculateStabilityWindow_ActiveSlotsCoefficientEdgeCases(
	t *testing.T,
) {
	t.Run("Very small active slots coefficient", func(t *testing.T) {
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.01,
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

		ls := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		// 3*432/0.01 = 129600
		expectedWindow := uint64(129600)
		if result != expectedWindow {
			t.Errorf(
				"expected stability window %d, got %d",
				expectedWindow,
				result,
			)
		}
	})

	t.Run("Rounding up with remainder", func(t *testing.T) {
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.07,
			"securityParam": 100,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		cfg := &cardano.CardanoNodeConfig{}
		if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		ls := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		// 3*100/0.07 = 300/0.07 = 4285.714... should round up to 4286
		if result < 4285 || result > 4287 {
			t.Errorf("expected stability window around 4286, got %d", result)
		}
	})

	t.Run("Precision with fractional coefficient", func(t *testing.T) {
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 432,
				"protocolMagic": 2
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.333333,
			"securityParam": 1000,
			"systemStart": "2022-10-25T00:00:00Z"
		}`

		cfg := &cardano.CardanoNodeConfig{}
		if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		ls := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := ls.calculateStabilityWindow()
		// 3*1000/0.333333 ≈ 9000
		if result == 0 {
			t.Error("expected non-zero stability window")
		}
		if result < 8999 || result > 9002 {
			t.Errorf("expected stability window around 9000, got %d", result)
		}
	})
}

// TestCalculateStabilityWindow_AllEras tests calculation across different eras
func TestCalculateStabilityWindow_AllEras(t *testing.T) {
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

	testCases := []struct {
		name           string
		era            eras.EraDesc
		expectedWindow uint64
	}{
		{
			name:           "Byron era",
			era:            eras.ByronEraDesc,
			expectedWindow: 864, // 2k
		},
		{
			name:           "Shelley era",
			era:            eras.ShelleyEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Allegra era",
			era:            eras.AllegraEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Mary era",
			era:            eras.MaryEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Alonzo era",
			era:            eras.AlonzoEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Babbage era",
			era:            eras.BabbageEraDesc,
			expectedWindow: 25920, // 3k/f
		},
		{
			name:           "Conway era",
			era:            eras.ConwayEraDesc,
			expectedWindow: 25920, // 3k/f
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{
				currentEra: tc.era,
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			result := ls.calculateStabilityWindow()
			if result != tc.expectedWindow {
				t.Errorf(
					"era %s: expected stability window %d, got %d",
					tc.era.Name,
					tc.expectedWindow,
					result,
				)
			}
		})
	}
}

// TestCalculateStabilityWindow_Integration tests the function in realistic scenarios
func TestCalculateStabilityWindow_Integration(t *testing.T) {
	t.Run("Mainnet-like configuration", func(t *testing.T) {
		byronGenesisJSON := `{
			"protocolConsts": {
				"k": 2160,
				"protocolMagic": 764824073
			}
		}`
		shelleyGenesisJSON := `{
			"activeSlotsCoeff": 0.05,
			"securityParam": 2160,
			"systemStart": "2017-09-23T21:44:51Z"
		}`

		cfg := &cardano.CardanoNodeConfig{}
		if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
			t.Fatalf("failed to load Byron genesis: %v", err)
		}
		if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
			t.Fatalf("failed to load Shelley genesis: %v", err)
		}

		// Test Byron era with mainnet params
		lsByron := &LedgerState{
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		resultByron := lsByron.calculateStabilityWindow()
		if resultByron != 4320 {
			t.Errorf(
				"Byron era: expected stability window 4320, got %d",
				resultByron,
			)
		}

		// Test Shelley era with mainnet params
		lsShelley := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		resultShelley := lsShelley.calculateStabilityWindow()
		// 3*2160/0.05 = 129600
		if resultShelley != 129600 {
			t.Errorf(
				"Shelley era: expected stability window 129600, got %d",
				resultShelley,
			)
		}
	})

	t.Run("Preview testnet configuration", func(t *testing.T) {
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

		lsShelley := &LedgerState{
			currentEra: eras.ShelleyEraDesc,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
			},
		}

		result := lsShelley.calculateStabilityWindow()
		// 3*432/0.05 = 25920
		if result != 25920 {
			t.Errorf(
				"Preview testnet: expected stability window 25920, got %d",
				result,
			)
		}
	})
}

// TestCalculateStabilityWindow_LargeValues tests with large but valid values
func TestCalculateStabilityWindow_LargeValues(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 1000000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{}
	if err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result := ls.calculateStabilityWindow()
	// 3*1000000/0.05 = 60000000
	expectedWindow := uint64(60000000)
	if result != expectedWindow {
		t.Errorf("expected stability window %d, got %d", expectedWindow, result)
	}
}

func newNonceReadyTestConfig(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()

	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.5,
		"securityParam": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)
	return cfg
}

func newNonceReadyTestLedgerState(
	t *testing.T,
	eventBus *event.EventBus,
	tipSlot uint64,
) *LedgerState {
	t.Helper()

	return &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             10,
			StartSlot:           1000,
			LengthInSlots:       100,
			EraId:               eras.ShelleyEraDesc.Id,
			Nonce:               nil,
			EvolvingNonce:       []byte{0x02},
			CandidateNonce:      []byte{0x03},
			LastEpochBlockNonce: []byte{0x04},
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: tipSlot,
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newNonceReadyTestConfig(t),
			EventBus:          eventBus,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

func TestLedgerStateIsNearTipUsesStabilityWindow(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newNonceReadyTestConfig(t),
		},
		currentEra: eras.ShelleyEraDesc,
	}
	ls.syncUpstreamTipSlot.Store(1000)

	assert.False(t, ls.isNearTip(993), "gap above 3k/f should be catch-up")
	assert.True(t, ls.isNearTip(994), "gap equal to 3k/f should be near tip")
	assert.True(t, ls.isNearTip(1001), "local tip beyond upstream is near tip")
}

func TestNextEpochNonceReadyCutoffSlot(t *testing.T) {
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
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Shelley → TPraos: stabilityWindow = 3k/f = 3*432/0.05 = 25920
	// cutoff = epochStart + epochLength - 25920
	//        = 106963200 + 86400 - 25920 = 107023680
	cutoffSlot, ok := ls.nextEpochNonceReadyCutoffSlot(models.Epoch{
		EpochId:       1238,
		StartSlot:     106963200,
		LengthInSlots: 86400,
		EraId:         eras.ShelleyEraDesc.Id,
	})
	require.True(t, ok)
	assert.Equal(t, uint64(107023680), cutoffSlot)
}

func TestNextEpochNonceReadyEpoch(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.5,
		"securityParam": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	currentSlot := uint64(1095)
	provider := newMockSlotTimeProvider(
		time.Now().Add(-time.Duration(currentSlot)*time.Second),
		time.Second,
		100,
	)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())
	clock.nowFunc = func() time.Time {
		return provider.systemStart.Add(time.Duration(currentSlot) * time.Second)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             10,
			StartSlot:           1000,
			LengthInSlots:       100,
			EraId:               eras.ShelleyEraDesc.Id,
			Nonce:               nil,
			EvolvingNonce:       []byte{0x02},
			CandidateNonce:      []byte{0x03},
			LastEpochBlockNonce: []byte{0x04},
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1095,
			},
		},
		slotClock: clock,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.syncUpstreamTipSlot.Store(1100)

	readyEpoch, ok := ls.NextEpochNonceReadyEpoch()
	require.True(t, ok)
	assert.Equal(t, uint64(11), readyEpoch)
}

func TestComputeNextEpochNonceUsesImportedTipAnchor(t *testing.T) {
	db, err := database.New(&database.Config{DataDir: ""})
	require.NoError(t, err)
	defer db.Close()

	tipNonce := bytes.Repeat([]byte{0x22}, 32)
	candidateNonce := bytes.Repeat([]byte{0x33}, 32)

	require.NoError(t, db.SetBlockNonce(
		bytes.Repeat([]byte{0x44}, 32),
		1050,
		tipNonce,
		false,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:        10,
			StartSlot:      1000,
			LengthInSlots:  100,
			Nonce:          bytes.Repeat([]byte{0x11}, 32),
			EvolvingNonce:  tipNonce,
			CandidateNonce: candidateNonce,
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1050,
			},
		},
		// currentTipBlockNonce is intentionally unset to mimic a snapshot
		// import where the in-memory tip-nonce cache hasn't been populated.
		// This forces computeEpochNonceForSlot past its in-memory short-circuit
		// and exercises the DB-resume anchor lookup against block_nonce rows.
		config: LedgerStateConfig{
			CardanoNodeConfig: newNonceReadyTestConfig(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	got := ls.computeNextEpochNonce(ls.currentEpoch, ls.currentEra)
	require.Equal(t, candidateNonce, got)
	require.NotEqual(t, tipNonce, got)
}

func TestNextEpochNonceReadyEpochNotReadyBeforeCutoff(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.5,
		"securityParam": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	currentSlot := uint64(1085)
	provider := newMockSlotTimeProvider(
		time.Now().Add(-time.Duration(currentSlot)*time.Second),
		time.Second,
		100,
	)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())
	clock.nowFunc = func() time.Time {
		return provider.systemStart.Add(time.Duration(currentSlot) * time.Second)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             10,
			StartSlot:           1000,
			LengthInSlots:       100,
			EraId:               eras.ShelleyEraDesc.Id,
			Nonce:               nil,
			EvolvingNonce:       []byte{0x02},
			CandidateNonce:      []byte{0x03},
			LastEpochBlockNonce: []byte{0x04},
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1085,
			},
		},
		slotClock: clock,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.syncUpstreamTipSlot.Store(1100)

	readyEpoch, ok := ls.NextEpochNonceReadyEpoch()
	require.False(t, ok)
	assert.Equal(t, uint64(0), readyEpoch)
}

func TestEmitNextEpochNonceReadyRequiresLedgerTipAtCutoff(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	_, evtCh := eventBus.Subscribe(event.EpochNonceReadyEventType)
	ls := newNonceReadyTestLedgerState(t, eventBus, 1085)

	ls.emitNextEpochNonceReady(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		SlotTick{Slot: 1095, Epoch: 10},
		ls.currentEpoch,
		ls.currentEra,
		1085,
	)

	select {
	case evt := <-evtCh:
		t.Fatalf("unexpected nonce-ready event published: %#v", evt)
	case <-time.After(100 * time.Millisecond):
	}

	assert.Equal(t, uint64(0), ls.nextNonceReadyEpoch.Load())
}

func TestResetNextEpochNonceReadyAllowsReEmit(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	_, evtCh := eventBus.Subscribe(event.EpochNonceReadyEventType)
	ls := newNonceReadyTestLedgerState(t, eventBus, 1095)
	ls.nextNonceReadyEpoch.Store(11)
	ls.resetNextEpochNonceReady()

	ls.emitNextEpochNonceReady(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		SlotTick{Slot: 1095, Epoch: 10},
		ls.currentEpoch,
		ls.currentEra,
		1095,
	)

	select {
	case evt := <-evtCh:
		readyEvent, ok := evt.Data.(event.EpochNonceReadyEvent)
		require.True(t, ok)
		assert.Equal(t, uint64(10), readyEvent.CurrentEpoch)
		assert.Equal(t, uint64(11), readyEvent.ReadyEpoch)
	case <-time.After(time.Second):
		t.Fatal("expected nonce-ready event after rollback reset")
	}
}

func TestNextEpochNonceReadyCutoffSlotShortEpoch(t *testing.T) {
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
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Shelley → 3k/f = 25920, which exceeds the 100-slot epoch, so the
	// cutoff degenerates to the epoch start.
	cutoffSlot, ok := ls.nextEpochNonceReadyCutoffSlot(models.Epoch{
		EpochId:       42,
		StartSlot:     1000,
		LengthInSlots: 100,
		EraId:         eras.ShelleyEraDesc.Id,
	})
	require.True(t, ok)
	assert.Equal(t, uint64(1000), cutoffSlot)
}

// TestDatabaseWorkerPoolBasic tests basic worker pool functionality
func TestDatabaseWorkerPoolBasic(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	// Use a nil database for testing - workers don't actually need a real one
	pool := NewDatabaseWorkerPool(nil, config)
	require.NotNil(t, pool)

	var executedCount atomic.Int32

	// Submit a simple operation
	resultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			executedCount.Add(1)
			return nil
		},
		ResultChan: resultChan,
	})

	// Wait for result with timeout
	select {
	case result := <-resultChan:
		assert.NoError(t, result.Error)
		assert.Equal(t, int32(1), executedCount.Load())
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for operation result")
	}

	pool.Shutdown()
}

// TestDatabaseWorkerPoolInFlightOperations tests that shutdown waits for in-flight operations
func TestDatabaseWorkerPoolInFlightOperations(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 2
	config.TaskQueueSize = 10

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32
	var wg sync.WaitGroup

	// Submit multiple operations
	for range 5 {
		wg.Add(1)
		resultChan := make(chan DatabaseResult, 1)

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				// Simulate work with short delay
				time.Sleep(10 * time.Millisecond)
				completedCount.Add(1)
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain result in goroutine
		go func(ch chan DatabaseResult) {
			defer wg.Done()
			result := <-ch
			// Error is expected if shutdown occurred before operation completed
			// But we should receive the error in the channel
			_ = result.Error
		}(resultChan)
	}

	// Wait for at least one operation to start processing
	require.Eventually(t, func() bool {
		return completedCount.Load() > 0
	}, 5*time.Second, 5*time.Millisecond, "at least one operation should start")

	// Shutdown the pool - this should wait for all operations to complete
	pool.Shutdown()

	// Wait for all result handlers
	wg.Wait()

	// Verify all operations completed
	assert.Equal(
		t,
		int32(5),
		completedCount.Load(),
		"not all operations completed before shutdown returned",
	)
}

// TestDatabaseWorkerPoolShutdownWithErrors tests error handling during shutdown
func TestDatabaseWorkerPoolShutdownWithErrors(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 2
	config.TaskQueueSize = 10

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32

	// Submit operations, some will error
	for i := range 3 {
		resultChan := make(chan DatabaseResult, 1)
		operationIndex := i

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				time.Sleep(20 * time.Millisecond)
				completedCount.Add(1)
				if operationIndex == 1 {
					return fmt.Errorf("operation %d failed", operationIndex)
				}
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain results
		go func() {
			select {
			case <-resultChan:
			case <-time.After(10 * time.Second):
			}
		}()
	}

	// Shutdown should wait for all operations to complete
	pool.Shutdown()

	// Verify all operations completed even with errors
	assert.Equal(
		t,
		int32(3),
		completedCount.Load(),
		"not all operations completed",
	)
}

// TestDatabaseWorkerPoolQueueFull tests behavior when queue is full
func TestDatabaseWorkerPoolQueueFull(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 1 // Very small queue

	pool := NewDatabaseWorkerPool(nil, config)

	// Submit some operations
	for range 3 {
		resultChan := make(chan DatabaseResult, 1)
		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain result
		go func(ch chan DatabaseResult) {
			<-ch
		}(resultChan)
	}

	// Shutdown should complete successfully
	pool.Shutdown()
}

// TestDatabaseWorkerPoolSubmitAfterShutdown tests that submitting after shutdown fails
func TestDatabaseWorkerPoolSubmitAfterShutdown(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	pool := NewDatabaseWorkerPool(nil, config)

	// Shutdown the pool
	pool.Shutdown()

	// Try to submit an operation after shutdown
	resultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			return nil
		},
		ResultChan: resultChan,
	})

	// Should get a shutdown error
	select {
	case result := <-resultChan:
		assert.Error(t, result.Error)
		assert.Contains(t, result.Error.Error(), "shut down")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for error result")
	}
}

// TestDatabaseWorkerPoolShutdownDoesNotPanicWithInFlightOperations verifies that
// shutdown remains panic-free while operations are still queued or running.
func TestDatabaseWorkerPoolShutdownDoesNotPanicWithInFlightOperations(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 2
	config.TaskQueueSize = 20

	pool := NewDatabaseWorkerPool(nil, config)

	// Barrier: workers block until release so Shutdown overlaps in-flight work.
	hold := make(chan struct{})
	var inFlight atomic.Int32

	for range 10 {
		resultChan := make(chan DatabaseResult, 1)
		go func(ch chan DatabaseResult) {
			<-ch
		}(resultChan)

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				inFlight.Add(1)
				defer inFlight.Add(-1)
				<-hold
				return nil
			},
			ResultChan: resultChan,
		})
	}

	testutil.WaitForCondition(
		t,
		func() bool { return inFlight.Load() > 0 },
		2*time.Second,
		"at least one operation should be running",
	)

	shutdownDone := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(shutdownDone)
	}()

	close(hold)

	select {
	case <-shutdownDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for Shutdown")
	}
}

// TestDatabaseWorkerPoolConcurrency tests the pool under concurrent load
func TestDatabaseWorkerPoolConcurrency(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 5
	config.TaskQueueSize = 50

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32

	// Submit many operations
	numOperations := 20
	for range numOperations {
		resultChan := make(chan DatabaseResult, 1)

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				completedCount.Add(1)
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain result immediately
		go func(ch chan DatabaseResult) {
			<-ch
		}(resultChan)
	}

	// Shutdown pool - should wait for all operations
	pool.Shutdown()

	// All operations should complete
	assert.Equal(t, int32(numOperations), completedCount.Load())
}

// TestDatabaseWorkerPoolMultipleShutdowns tests that multiple shutdown calls are safe
func TestDatabaseWorkerPoolMultipleShutdowns(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	pool := NewDatabaseWorkerPool(nil, config)

	// Submit an operation
	resultChan := make(chan DatabaseResult, 1)
	pool.Submit(DatabaseOperation{
		OpFunc: func(db *database.Database) error {
			return nil
		},
		ResultChan: resultChan,
	})

	// Drain result
	<-resultChan

	// Call shutdown multiple times - should be safe
	pool.Shutdown()
	pool.Shutdown() // Should not panic
	pool.Shutdown() // Should not panic
}

// TestDatabaseWorkerPoolResultChannelFull tests handling of full result channels
func TestDatabaseWorkerPoolResultChannelFull(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 5

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32

	// Submit operations
	for range 3 {
		resultChan := make(chan DatabaseResult, 1)

		pool.Submit(DatabaseOperation{
			OpFunc: func(db *database.Database) error {
				completedCount.Add(1)
				return nil
			},
			ResultChan: resultChan,
		})

		// Drain result
		go func(ch chan DatabaseResult) {
			<-ch
		}(resultChan)
	}

	// Shutdown should work
	pool.Shutdown()

	// All operations should complete
	assert.Equal(t, int32(3), completedCount.Load())
}

// TestTransitionToEra_ReturnsResultWithoutMutating tests that transitionToEra
// returns computed state without mutating LedgerState fields
func TestTransitionToEra_ReturnsResultWithoutMutating(t *testing.T) {
	// Setup: Create genesis configs for the transition
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"epochLength": 432000,
		"slotLength": 1,
		"protocolParams": {
			"protocolVersion": {"major": 2, "minor": 0},
			"decentralisationParam": 1,
			"maxBlockBodySize": 65536,
			"maxBlockHeaderSize": 1100,
			"maxTxSize": 16384,
			"minFeeA": 44,
			"minFeeB": 155381,
			"minUTxOValue": 1000000,
			"keyDeposit": 2000000,
			"poolDeposit": 500000000,
			"eMax": 18,
			"nOpt": 150,
			"a0": 0.3,
			"rho": 0.003,
			"tau": 0.2,
			"minPoolCost": 340000000
		},
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	// Create in-memory database
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ByronEraDesc,
		currentPParams: nil, // Start with nil
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Capture original state
	originalEra := ls.currentEra
	originalPParams := ls.currentPParams

	// Execute transition in a transaction
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		result, err := ls.transitionToEra(
			txn,
			eras.ShelleyEraDesc.Id,
			0,   // startEpoch
			0,   // addedSlot
			nil, // currentPParams (Byron has none)
		)
		if err != nil {
			return err
		}

		// Verify result contains expected values
		assert.NotNil(t, result)
		assert.Equal(t, eras.ShelleyEraDesc.Id, result.NewEra.Id)
		assert.Equal(t, "Shelley", result.NewEra.Name)
		// Shelley transition creates protocol parameters
		assert.NotNil(t, result.NewPParams)

		// Verify LedgerState was NOT mutated
		assert.Equal(
			t,
			originalEra.Id,
			ls.currentEra.Id,
			"currentEra should not be mutated",
		)
		assert.Equal(
			t,
			originalPParams,
			ls.currentPParams,
			"currentPParams should not be mutated",
		)

		return nil
	})
	require.NoError(t, err)
}

// TestTransitionToEra_ChainedTransitions tests multiple era transitions in sequence
func TestTransitionToEra_ChainedTransitions(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"epochLength": 432000,
		"slotLength": 1,
		"protocolParams": {
			"protocolVersion": {"major": 2, "minor": 0},
			"decentralisationParam": 1,
			"maxBlockBodySize": 65536,
			"maxBlockHeaderSize": 1100,
			"maxTxSize": 16384,
			"minFeeA": 44,
			"minFeeB": 155381,
			"minUTxOValue": 1000000,
			"keyDeposit": 2000000,
			"poolDeposit": 500000000,
			"eMax": 18,
			"nOpt": 150,
			"a0": 0.3,
			"rho": 0.003,
			"tau": 0.2,
			"minPoolCost": 340000000
		},
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ByronEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Chain transitions from Byron -> Shelley -> Allegra
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		// Track working state as we chain transitions
		workingPParams := ls.currentPParams

		// Byron -> Shelley
		result1, err := ls.transitionToEra(
			txn,
			eras.ShelleyEraDesc.Id,
			0,
			0,
			workingPParams,
		)
		require.NoError(t, err)
		workingPParams = result1.NewPParams

		// Shelley -> Allegra
		result2, err := ls.transitionToEra(
			txn,
			eras.AllegraEraDesc.Id,
			1,
			432000,
			workingPParams,
		)
		require.NoError(t, err)

		// Verify final result
		assert.Equal(t, eras.AllegraEraDesc.Id, result2.NewEra.Id)
		assert.NotNil(t, result2.NewPParams)

		// Verify LedgerState still has original Byron era
		assert.Equal(t, eras.ByronEraDesc.Id, ls.currentEra.Id)

		return nil
	})
	require.NoError(t, err)
}

func TestTransitionToEraTranslatesConwayGovernanceWhenProtocolAlreadyDijkstra(
	t *testing.T,
) {
	db := newTestDB(t)

	fee := uint(1234)
	action := &conway.ConwayParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
		ParamUpdate: conway.ConwayProtocolParameterUpdate{
			MinFeeA: &fee,
		},
		PolicyHash: []byte{0xaa, 0xbb},
	}
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)
	ratifiedEpoch := uint64(10)
	ratifiedSlot := uint64(200)
	proposal := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0xe1}, 32),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeParameterChange),
		ProposedEpoch: 9,
		ExpiresEpoch:  20,
		GovActionCbor: actionCbor,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AddedSlot:     100,
		AnchorURL:     "https://example.invalid/transition-translate",
		AnchorHash:    bytes.Repeat([]byte{0xe2}, 32),
		ReturnAddress: bytes.Repeat([]byte{0xe3}, 29),
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))

	newCborRat := func(num, denom int64) *cbor.Rat {
		return &cbor.Rat{Rat: big.NewRat(num, denom)}
	}
	newRat := func(num, denom int64) cbor.Rat {
		return cbor.Rat{Rat: big.NewRat(num, denom)}
	}
	currentPParams := &conway.ConwayProtocolParameters{
		A0:  newCborRat(0, 1),
		Rho: newCborRat(0, 1),
		Tau: newCborRat(0, 1),
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: dijkstra.MinProtocolVersionDijkstra,
		},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  newCborRat(1, 1),
			StepPrice: newCborRat(1, 1),
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    newRat(1, 2),
			CommitteeNormal:       newRat(1, 2),
			CommitteeNoConfidence: newRat(1, 2),
			HardForkInitiation:    newRat(1, 2),
			PpSecurityGroup:       newRat(1, 2),
		},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    newRat(1, 2),
			CommitteeNormal:       newRat(1, 2),
			CommitteeNoConfidence: newRat(1, 2),
			UpdateToConstitution:  newRat(1, 2),
			HardForkInitiation:    newRat(1, 2),
			PpNetworkGroup:        newRat(1, 2),
			PpEconomicGroup:       newRat(1, 2),
			PpTechnicalGroup:      newRat(1, 2),
			PpGovGroup:            newRat(1, 2),
			TreasuryWithdrawal:    newRat(1, 2),
		},
		MinFeeRefScriptCostPerByte: newCborRat(1, 1),
	}
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		activeEras:     eras.ErasWithDijkstra,
		currentPParams: currentPParams,
		config: LedgerStateConfig{
			CardanoNodeConfig: &cardano.CardanoNodeConfig{},
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		_, err := ls.transitionToEra(
			txn,
			eras.DijkstraEraDesc.Id,
			11,
			300,
			currentPParams,
		)
		return err
	})
	require.NoError(t, err)

	got, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	var translated dijkstra.DijkstraParameterChangeGovAction
	_, err = cbor.Decode(got.GovActionCbor, &translated)
	require.NoError(t, err)
	require.NotNil(t, translated.ParamUpdate.MinFeeA)
	require.Equal(t, uint(1234), *translated.ParamUpdate.MinFeeA)
	require.Equal(t, []byte{0xaa, 0xbb}, translated.PolicyHash)
}

// TestEpochRolloverResult_FieldsPopulated tests that EpochRolloverResult
// contains all expected fields after processEpochRollover
func TestEpochRolloverResult_FieldsPopulated(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"epochLength": 432000,
		"slotLength": 1,
		"protocolParams": {
			"protocolVersion": {"major": 2, "minor": 0},
			"decentralisationParam": 1,
			"maxBlockBodySize": 65536,
			"maxBlockHeaderSize": 1100,
			"maxTxSize": 16384,
			"minFeeA": 44,
			"minFeeB": 155381,
			"minUTxOValue": 1000000,
			"keyDeposit": 2000000,
			"poolDeposit": 500000000,
			"eMax": 18,
			"nOpt": 150,
			"a0": 0.3,
			"rho": 0.003,
			"tau": 0.2,
			"minPoolCost": 340000000
		},
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    0, // Triggers initial epoch creation
			LengthInSlots: 0,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Execute epoch rollover for initial epoch
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		result, err := ls.processEpochRollover(
			txn,
			ls.currentEpoch,
			ls.currentEra,
			ls.currentPParams,
		)
		require.NoError(t, err)

		// Verify result fields are populated
		assert.NotNil(t, result)
		assert.NotEmpty(
			t,
			result.NewEpochCache,
			"NewEpochCache should be populated",
		)
		assert.Equal(t, uint64(0), result.NewCurrentEpoch.EpochId)
		assert.Equal(t, false, result.CheckpointWrittenForEpoch)

		// Verify LedgerState was NOT mutated
		assert.Equal(t, uint64(0), ls.currentEpoch.EpochId)
		assert.Empty(t, ls.epochCache, "epochCache should not be mutated")

		return nil
	})
	require.NoError(t, err)
}

// TestEpochRollover_NoDeadlockDuringTransaction tests that epoch rollover
// does not hold LedgerState lock during database operations.
// This simulates the scenario that caused the original deadlock.
func TestEpochRollover_NoDeadlockDuringTransaction(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"epochLength": 432000,
		"slotLength": 1,
		"protocolParams": {
			"protocolVersion": {"major": 2, "minor": 0},
			"decentralisationParam": 1,
			"maxBlockBodySize": 65536,
			"maxBlockHeaderSize": 1100,
			"maxTxSize": 16384,
			"minFeeA": 44,
			"minFeeB": 155381,
			"minUTxOValue": 1000000,
			"keyDeposit": 2000000,
			"poolDeposit": 500000000,
			"eMax": 18,
			"nOpt": 150,
			"a0": 0.3,
			"rho": 0.003,
			"tau": 0.2,
			"minPoolCost": 340000000
		},
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    0,
			LengthInSlots: 0,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// This test verifies that the pattern doesn't deadlock:
	// 1. Take RLock to capture snapshot
	// 2. Release RLock
	// 3. Execute transaction (which might need to acquire lock in recovery)
	// 4. Take Lock briefly to apply results
	// 5. Release Lock

	errChan := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)

		// Step 1: Capture snapshot with RLock
		ls.RLock()
		snapshotEra := ls.currentEra
		snapshotEpoch := ls.currentEpoch
		snapshotPParams := ls.currentPParams
		ls.RUnlock()

		// Step 2: Execute transaction WITHOUT holding lock
		var result *EpochRolloverResult
		txn := db.Transaction(true)
		err := txn.Do(func(txn *database.Txn) error {
			var err error
			result, err = ls.processEpochRollover(
				txn,
				snapshotEpoch,
				snapshotEra,
				snapshotPParams,
			)
			return err
		})
		if err != nil {
			errChan <- err
			return
		}

		// Step 3: Apply results with brief Lock
		ls.Lock()
		if result != nil {
			ls.epochCache = result.NewEpochCache
			ls.currentEpoch = result.NewCurrentEpoch
			ls.currentEra = result.NewCurrentEra
		}
		ls.Unlock()
	}()

	// If this test times out, we have a deadlock
	select {
	case <-done:
		// Success - no deadlock
		select {
		case err := <-errChan:
			require.NoError(t, err)
		default:
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock detected - epoch rollover did not complete in time")
	}
}

// TestEpochRollover_ConcurrentReaders tests that the epoch rollover pattern
// allows concurrent readers during the transaction phase
func TestEpochRollover_ConcurrentReaders(t *testing.T) {
	byronGenesisJSON := `{
		"protocolConsts": {
			"k": 432,
			"protocolMagic": 2
		}
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"epochLength": 432000,
		"slotLength": 1,
		"protocolParams": {
			"protocolVersion": {"major": 2, "minor": 0},
			"decentralisationParam": 1,
			"maxBlockBodySize": 65536,
			"maxBlockHeaderSize": 1100,
			"maxTxSize": 16384,
			"minFeeA": 44,
			"minFeeB": 155381,
			"minUTxOValue": 1000000,
			"keyDeposit": 2000000,
			"poolDeposit": 500000000,
			"eMax": 18,
			"nOpt": 150,
			"a0": 0.3,
			"rho": 0.003,
			"tau": 0.2,
			"minPoolCost": 340000000
		},
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	require.NoError(
		t,
		cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON)),
	)
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    0,
			LengthInSlots: 0,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var wg sync.WaitGroup
	readCount := atomic.Int32{}
	txnStarted := make(chan struct{})
	txnDone := make(chan struct{})
	rolloverErr := make(chan error, 1)

	// Start the epoch rollover goroutine
	wg.Go(func() {

		// Capture snapshot
		ls.RLock()
		snapshotEra := ls.currentEra
		snapshotEpoch := ls.currentEpoch
		snapshotPParams := ls.currentPParams
		ls.RUnlock()

		// Signal that transaction is starting
		close(txnStarted)

		// Execute transaction (simulates DB work)
		var result *EpochRolloverResult
		txn := db.Transaction(true)
		err := txn.Do(func(txn *database.Txn) error {
			// Add a small delay to give readers time to run
			time.Sleep(50 * time.Millisecond)
			var err error
			result, err = ls.processEpochRollover(
				txn,
				snapshotEpoch,
				snapshotEra,
				snapshotPParams,
			)
			return err
		})
		if err != nil {
			rolloverErr <- err
			close(txnDone)
			return
		}

		// Apply results
		ls.Lock()
		if result != nil {
			ls.epochCache = result.NewEpochCache
			ls.currentEpoch = result.NewCurrentEpoch
		}
		ls.Unlock()

		close(txnDone)
	})

	// Start multiple reader goroutines that try to read during the transaction
	for range 5 {
		wg.Go(func() {

			// Wait for transaction to start
			<-txnStarted

			// Try to read multiple times during the transaction
			for range 10 {
				select {
				case <-txnDone:
					return
				default:
					ls.RLock()
					_ = ls.currentEra   // Read era
					_ = ls.currentEpoch // Read epoch
					readCount.Add(1)
					ls.RUnlock()
					time.Sleep(5 * time.Millisecond)
				}
			}
		})
	}

	// Wait for all goroutines with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - check for rollover error
		select {
		case err := <-rolloverErr:
			require.NoError(t, err)
		default:
		}
		assert.Greater(
			t,
			readCount.Load(),
			int32(0),
			"readers should have been able to read during transaction",
		)
	case <-time.After(10 * time.Second):
		t.Fatal("timeout - possible deadlock with concurrent readers")
	}
}

// TestTransitionToEra_ErrorHandling tests error conditions in transitionToEra
func TestTransitionToEra_ErrorHandling(t *testing.T) {
	t.Run("invalid era ID returns error", func(t *testing.T) {
		db, err := database.New(&database.Config{
			BlobPlugin:     "badger",
			MetadataPlugin: "sqlite",
			DataDir:        "",
		})
		require.NoError(t, err)
		defer db.Close()

		ls := &LedgerState{
			db:         db,
			currentEra: eras.ByronEraDesc,
			config: LedgerStateConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			},
		}

		txn := db.Transaction(true)
		err = txn.Do(func(txn *database.Txn) error {
			_, err := ls.transitionToEra(txn, 999, 0, 0, nil)
			return err
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown era ID 999")
	})
}

// makeTestBlock creates a test block with deterministic hash based on slot
func makeTestBlock(slot, id uint64) models.Block {
	// Create deterministic hash from slot
	slotBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(slotBytes, slot)
	hash := sha256.Sum256(slotBytes)
	return models.Block{
		ID:       id,
		Slot:     slot,
		Hash:     hash[:],
		Number:   id,
		Type:     1, // Shelley era type
		PrevHash: nil,
		Cbor:     []byte{0x80}, // minimal CBOR (empty array)
	}
}

// makeTestPoint creates a Point from a test block
func makeTestPoint(block models.Block) pcommon.Point {
	return pcommon.NewPoint(block.Slot, block.Hash)
}

// TestCleanupOrphanedBlobs_NoBlobStore tests that cleanup gracefully handles nil blob store
func TestCleanupOrphanedBlobs_NoBlobStore(t *testing.T) {
	ls := &LedgerState{
		db: nil, // No database
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Create a mock database that returns nil blob store
	mockDB, err := database.New(&database.Config{
		BlobPlugin:     "",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer mockDB.Close()

	ls.db = mockDB

	// Cleanup should return nil when there's no blob store
	err = ls.cleanupOrphanedBlobs(100)
	assert.NoError(t, err)
}

// TestCleanupOrphanedBlobs_NoOrphans tests cleanup when there are no orphaned blocks
func TestCleanupOrphanedBlobs_NoOrphans(t *testing.T) {
	// Create an in-memory database
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Store a few blocks at slots 1, 2, 3
	for slot := uint64(1); slot <= 3; slot++ {
		block := makeTestBlock(slot, slot)
		err = db.BlockCreate(block, nil)
		require.NoError(t, err)
	}

	// Cleanup with tip at slot 3 - no orphans expected
	err = ls.cleanupOrphanedBlobs(3)
	assert.NoError(t, err)

	// Verify all blocks still exist
	for slot := uint64(1); slot <= 3; slot++ {
		block := makeTestBlock(slot, slot)
		_, err := database.BlockByPoint(db, makeTestPoint(block))
		assert.NoError(t, err, "block at slot %d should still exist", slot)
	}
}

// TestCleanupOrphanedBlobs_WithOrphans tests cleanup when orphaned blocks exist
func TestCleanupOrphanedBlobs_WithOrphans(t *testing.T) {
	// Create an in-memory database
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Store blocks at slots 1-5
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		err = db.BlockCreate(block, nil)
		require.NoError(t, err)
	}

	// Cleanup with tip at slot 3 - blocks at slots 4 and 5 should be orphans
	err = ls.cleanupOrphanedBlobs(3)
	assert.NoError(t, err)

	// Verify blocks at slots 1-3 still exist
	for slot := uint64(1); slot <= 3; slot++ {
		block := makeTestBlock(slot, slot)
		_, err := database.BlockByPoint(db, makeTestPoint(block))
		assert.NoError(t, err, "block at slot %d should still exist", slot)
	}

	// Verify blocks at slots 4-5 were deleted
	for slot := uint64(4); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		_, err := database.BlockByPoint(db, makeTestPoint(block))
		assert.Error(t, err, "block at slot %d should be deleted", slot)
	}
}

// TestCleanupOrphanedBlobs_SlotZero tests cleanup behavior when tip is at slot 0
func TestCleanupOrphanedBlobs_SlotZero(t *testing.T) {
	// Create an in-memory database
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Store a block at slot 1 (would be orphan if tip is 0)
	block := makeTestBlock(1, 1)
	err = db.BlockCreate(block, nil)
	require.NoError(t, err)

	// Cleanup with tip at slot 0 - block at slot 1 should be deleted
	err = ls.cleanupOrphanedBlobs(0)
	assert.NoError(t, err)

	// Verify block at slot 1 was deleted
	_, err = database.BlockByPoint(db, makeTestPoint(block))
	assert.Error(t, err, "block at slot 1 should be deleted")
}

func TestIntersectPointsReturnsNoPointsWhenLedgerTipIsEmpty(
	t *testing.T,
) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	txn := db.BlobTxn(true)
	err = txn.Do(func(txn *database.Txn) error {
		return db.Blob().Set(
			txn.Blob(),
			dbtypes.BlockBlobIndexKey(1),
			[]byte("bad"),
		)
	})
	require.NoError(t, err)

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	assert.Nil(t, points)
}

func TestIntersectPointsUsesPrimaryChainWhenPrimaryChainIsAhead(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ledgerTipBlock := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(ledgerTipBlock),
		BlockNumber: ledgerTipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
	}
	ls.currentTip = ledgerTip

	points, err := ls.IntersectPoints(3)
	require.NoError(t, err)
	require.Len(t, points, 3)
	assert.Equal(t, blocks[4].Slot, points[0].Slot)
	assert.Equal(t, blocks[4].Hash, points[0].Hash)
	assert.Equal(t, blocks[3].Slot, points[1].Slot)
	assert.Equal(t, blocks[3].Hash, points[1].Hash)
	assert.Equal(t, blocks[2].Slot, points[2].Slot)
	assert.Equal(t, blocks[2].Hash, points[2].Hash)
}

func TestIntersectPointsUsesSparseLedgerTipSamples(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	blocks := make([]models.Block, 0, 256)
	for slot := uint64(1); slot <= 256; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	require.NotEmpty(t, blocks)
	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
	}

	points, err := ls.IntersectPoints(40)
	require.NoError(t, err)
	require.Greater(t, len(points), ledgerIntersectDenseCount)
	assert.Equal(t, ledgerTipBlock.Slot, points[0].Slot)
	assert.Equal(t, ledgerTipBlock.Hash, points[0].Hash)

	pointSlots := make(map[uint64]struct{}, len(points))
	for _, point := range points {
		pointSlots[point.Slot] = struct{}{}
	}
	for _, slot := range []uint64{224, 192, 128, 1} {
		_, ok := pointSlots[slot]
		assert.True(t, ok, "missing sparse intersect point at slot %d", slot)
	}
}

func TestIntersectPointsIncludesMithrilTrustBoundary(t *testing.T) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	blocks := make([]models.Block, 0, 256)
	for slot := uint64(1); slot <= 256; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	require.NotEmpty(t, blocks)
	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: 173,
	}

	points, err := ls.IntersectPoints(40)
	require.NoError(t, err)

	boundarySlot := uint64(173)
	var boundaryPoint *ocommon.Point
	for _, point := range points {
		if point.Slot == boundarySlot {
			point := point
			boundaryPoint = &point
			break
		}
	}
	require.NotNil(t, boundaryPoint)
	assert.Equal(t, blocks[boundarySlot-1].Hash, boundaryPoint.Hash)
}

func TestIntersectPointsSkipsZeroMithrilTrustBoundary(t *testing.T) {
	db := newTestDB(t)

	blocks := make([]models.Block, 0, 10)
	for slot := uint64(1); slot <= 10; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: 0,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	assertNoIntersectPointAtSlot(t, points, 0)
}

func TestIntersectPointsSkipsFutureMithrilTrustBoundary(t *testing.T) {
	db := newTestDB(t)

	blocks := make([]models.Block, 0, 10)
	for slot := uint64(1); slot <= 10; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	ledgerTipBlock := blocks[len(blocks)-1]
	boundarySlot := ledgerTipBlock.Slot + 1
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: boundarySlot,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	assertNoIntersectPointAtSlot(t, points, boundarySlot)
}

func TestIntersectPointsSkipsMissingMithrilTrustBoundaryBlock(
	t *testing.T,
) {
	db := newTestDB(t)

	var blocks []models.Block
	for slot := uint64(1); slot <= 10; slot++ {
		if slot == 5 {
			continue
		}
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	require.NotEmpty(t, blocks)
	ledgerTipBlock := blocks[len(blocks)-1]
	boundarySlot := uint64(5)
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: boundarySlot,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	assertNoIntersectPointAtSlot(t, points, boundarySlot)
}

func TestIntersectPointsSkipsMithrilTrustBoundaryOnLookupError(
	t *testing.T,
) {
	db := newTestDB(t)

	blocks := make([]models.Block, 0, 10)
	for slot := uint64(1); slot <= 10; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	boundarySlot := uint64(5)
	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return db.Blob().Set(
			txn.Blob(),
			dbtypes.BlockHashIndexKey(blocks[boundarySlot-1].Hash),
			[]byte("bad"),
		)
	}))

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: boundarySlot,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	assertNoIntersectPointAtSlot(t, points, boundarySlot)
}

func TestIntersectPointsUsesCanonicalMithrilTrustBoundary(t *testing.T) {
	db := newTestDB(t)

	blocks := make([]models.Block, 0, 64)
	for slot := uint64(1); slot <= 64; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	boundarySlot := uint64(20)
	canonicalBoundaryBlock := blocks[boundarySlot-1]
	nonCanonicalBoundaryBlock := makeTestBlock(boundarySlot, 1000)
	nonCanonicalBoundaryBlock.Hash = bytes.Repeat([]byte{0xff}, 32)
	nonCanonicalBoundaryBlock.PrevHash = append(
		[]byte(nil),
		blocks[boundarySlot-2].Hash...,
	)
	require.NoError(t, db.BlockCreate(nonCanonicalBoundaryBlock, nil))

	rawBoundaryBlock, err := database.BlockBeforeSlot(
		db,
		boundarySlot+1,
	)
	require.NoError(t, err)
	require.Equal(t, nonCanonicalBoundaryBlock.Hash, rawBoundaryBlock.Hash)

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: boundarySlot,
	}

	points, err := ls.IntersectPoints(40)
	require.NoError(t, err)

	var boundaryPoint *ocommon.Point
	for _, point := range points {
		if point.Slot == boundarySlot {
			point := point
			boundaryPoint = &point
			break
		}
	}
	require.NotNil(t, boundaryPoint)
	assert.Equal(t, canonicalBoundaryBlock.Hash, boundaryPoint.Hash)
	assert.NotEqual(t, nonCanonicalBoundaryBlock.Hash, boundaryPoint.Hash)
}

func TestAuthoritativeLedgerBlockAtSlotDoesNotRequireMonotonicBlockIDs(
	t *testing.T,
) {
	db := newTestDB(t)

	blocks := make([]models.Block, 0, 64)
	for slot := uint64(1); slot <= 64; slot++ {
		id := slot
		switch slot {
		case 20:
			id = 50
		case 50:
			id = 20
		}
		block := makeTestBlock(slot, id)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{db: db}

	block, err := ls.authoritativeLedgerBlockAtSlot(
		20,
		makeTestPoint(ledgerTipBlock),
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), block.Slot)
	assert.Equal(t, blocks[19].Hash, block.Hash)
}

func TestIntersectPointsKeepsMithrilTrustBoundaryWhenPointListIsFull(
	t *testing.T,
) {
	db := newTestDB(t)

	blocks := make([]models.Block, 0, 10)
	for slot := uint64(1); slot <= 10; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
		mithrilLedgerSlot: 5,
	}

	points, err := ls.IntersectPoints(4)
	require.NoError(t, err)
	require.Len(t, points, 4)
	assert.Equal(t, uint64(10), points[0].Slot)
	assert.Equal(t, uint64(9), points[1].Slot)
	assert.Equal(t, uint64(8), points[2].Slot)
	assert.Equal(t, uint64(5), points[3].Slot)
}

func assertNoIntersectPointAtSlot(
	t *testing.T,
	points []ocommon.Point,
	slot uint64,
) {
	t.Helper()
	for _, point := range points {
		assert.NotEqual(t, slot, point.Slot)
	}
}

func TestIntersectPointsSkipsMissingDenseBlockIndex(t *testing.T) {
	db := newTestDB(t)

	blocks := make([]models.Block, 0, 40)
	for slot := uint64(1); slot <= 40; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append(
				[]byte(nil),
				blocks[len(blocks)-1].Hash...,
			)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	blockBlobIndexKey := dbtypes.BlockBlobIndexKey(39)
	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		indexBytes, err := db.Blob().Get(txn.Blob(), blockBlobIndexKey)
		require.NoError(t, err)
		require.NotNil(t, indexBytes)
		return db.Blob().Delete(
			txn.Blob(),
			blockBlobIndexKey,
		)
	}))

	ledgerTipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db: db,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(ledgerTipBlock),
			BlockNumber: ledgerTipBlock.Number,
		},
	}

	points, err := ls.IntersectPoints(40)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(points), ledgerIntersectDenseCount)

	pointSlots := make(map[uint64]struct{}, len(points))
	for _, point := range points {
		pointSlots[point.Slot] = struct{}{}
	}
	_, hasMissingIndexSlot := pointSlots[39]
	assert.False(t, hasMissingIndexSlot)
	_, hasPreviousDenseSlot := pointSlots[38]
	assert.True(t, hasPreviousDenseSlot)
}

func TestChainDensityUsesCardanoNodeFragment(t *testing.T) {
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 3
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	blocks := []models.Block{
		makeTestBlock(10, 1),
		makeTestBlock(20, 2),
		makeTestBlock(100, 3),
		makeTestBlock(190, 4),
		makeTestBlock(210, 5),
	}
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}
	tipBlock := blocks[len(blocks)-1]
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentTip: ochainsync.Tip{
			Point:       makeTestPoint(tipBlock),
			BlockNumber: tipBlock.Number,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())

	density := ls.chainFragmentDensity(ls.currentTip, ls.SecurityParam())
	ls.Lock()
	ls.updateTipMetrics(density)
	ls.Unlock()

	// cardano-node computes density over the ChainDB fragment as:
	// (tip block - oldest fragment block) / (tip slot - oldest fragment slot).
	// With k=3 and tip block index 5, the oldest fragment block is index 2.
	assert.InDelta(
		t,
		3.0/190.0,
		promtestutil.ToFloat64(ls.metrics.density),
		1e-12,
	)
}

func TestLoadTipSeedsChainDensityFromPersistedFragment(t *testing.T) {
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 3
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)),
	)

	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	blocks := []models.Block{
		makeTestBlock(10, 1),
		makeTestBlock(20, 2),
		makeTestBlock(100, 3),
		makeTestBlock(190, 4),
		makeTestBlock(210, 5),
	}
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}
	tipBlock := blocks[len(blocks)-1]
	require.NoError(t, db.SetBlockNonce(tipBlock.Hash, tipBlock.Slot, []byte{1}, false, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       makeTestPoint(tipBlock),
		BlockNumber: tipBlock.Number,
	}, nil))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())

	require.NoError(t, ls.loadTip())

	assert.InDelta(
		t,
		3.0/190.0,
		promtestutil.ToFloat64(ls.metrics.density),
		1e-12,
	)
}

func TestFragmentDensityIgnoresByronEbbBlockNumber(t *testing.T) {
	assert.InDelta(t, 9.0/100.0, fragmentDensity(100, 10, 0, 0), 1e-12)
}

func TestReconcilePrimaryChainTipWithLedgerTipPreservesSelectedChain(
	t *testing.T,
) {
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	defer db.Close()

	blocks := make([]models.Block, 0, 5)
	for slot := uint64(1); slot <= 5; slot++ {
		block := makeTestBlock(slot, slot)
		if len(blocks) > 0 {
			block.PrevHash = append([]byte(nil), blocks[len(blocks)-1].Hash...)
		}
		blocks = append(blocks, block)
		require.NoError(t, db.BlockCreate(block, nil))
	}

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)

	ledgerTipBlock := blocks[2]
	ledgerTip := ochainsync.Tip{
		Point:       makeTestPoint(ledgerTipBlock),
		BlockNumber: ledgerTipBlock.Number,
	}
	require.NoError(t, db.SetTip(ledgerTip, nil))

	ls := &LedgerState{
		db:    db,
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			ChainManager: cm,
			Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.currentTip = ledgerTip
	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip())

	chainTip := cm.PrimaryChain().Tip()
	assert.Equal(t, blocks[len(blocks)-1].Slot, chainTip.Point.Slot)
	assert.Equal(t, blocks[len(blocks)-1].Number, chainTip.BlockNumber)
	assert.Equal(t, blocks[len(blocks)-1].Hash, chainTip.Point.Hash)
	assert.Equal(t, ledgerTip, ls.currentTip)

	for _, block := range blocks {
		_, err := database.BlockByPoint(db, makeTestPoint(block))
		assert.NoError(t, err, "block at slot %d should still exist", block.Slot)
	}
}

// ---------------------------------------------------------------------------
// applyEraTransition / transitionInfo clearing tests
// ---------------------------------------------------------------------------

// babbagePParams returns a minimal *babbage.BabbageProtocolParameters with
// the given protocol major version.  Used to construct era transitions without
// going through the full genesis-loading machinery.
func babbagePParams(major uint) *babbage.BabbageProtocolParameters {
	return &babbage.BabbageProtocolParameters{ProtocolMajor: major}
}

func TestNewLedgerStateHardForkTransitionUsesConfiguredEraList(t *testing.T) {
	tests := []struct {
		name           string
		enableDijkstra bool
		expected       bool
	}{
		{
			name:     "default era table gates off Dijkstra",
			expected: false,
		},
		{
			name:           "Dijkstra-enabled era table detects transition",
			enableDijkstra: true,
			expected:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTestDB(t)
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			ls, err := NewLedgerState(LedgerStateConfig{
				Database:       db,
				ChainManager:   cm,
				Logger:         slog.New(slog.NewJSONHandler(io.Discard, nil)),
				EnableDijkstra: tt.enableDijkstra,
			})
			require.NoError(t, err)

			got := ls.isHardForkTransition(
				ProtocolVersion{Major: 10},
				ProtocolVersion{Major: 12},
			)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// newTestEpoch is a convenience builder for models.Epoch.
func newTestEpoch(id, startSlot uint64, lengthInSlots uint, eraId uint) models.Epoch {
	return models.Epoch{
		EpochId:       id,
		StartSlot:     startSlot,
		LengthInSlots: lengthInSlots,
		EraId:         eraId,
		SlotLength:    1000,
	}
}

// ---------------------------------------------------------------------------
// evaluateTransitionImpossible tests
// ---------------------------------------------------------------------------

// TestEvaluateTransitionImpossible_SetWhenSafeZoneReachesEpochEnd verifies
// that TransitionImpossible is set when tipSlot + safeZone >= epochEndSlot.
//
// Using Shelley-era parameters from newTestEraHistoryCfg:
//
//	securityParam=432, activeSlotsCoeff=0.05
//	safeZone = ceil(3*432/0.05) = 25_920
//	epoch: startSlot=100_000, length=432_000, end=532_000
//	tipSlot = 532_000 - 25_920 = 506_080 → safeEnd = 532_000 = epochEnd → Impossible
func TestEvaluateTransitionImpossible_SetWhenSafeZoneReachesEpochEnd(t *testing.T) {
	const (
		epochStart = uint64(100_000)
		epochLen   = uint(432_000)
		epochEnd   = uint64(532_000)
		safeZone   = uint64(25_920)
		// tipSlot such that tipSlot + safeZone == epochEnd (boundary case)
		tipSlot = epochEnd - safeZone // 506_080
	)

	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra:   requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(500, epochStart, epochLen, eras.ConwayEraDesc.Id),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionImpossible, ls.transitionInfo.State,
		"when safeEndSlot == epochEndSlot, TransitionImpossible must be set")
}

// TestEvaluateTransitionImpossible_SetWhenSafeZoneExceedsEpochEnd verifies
// that TransitionImpossible is set when safeEndSlot > epochEndSlot.
func TestEvaluateTransitionImpossible_SetWhenSafeZoneExceedsEpochEnd(t *testing.T) {
	const (
		epochStart = uint64(100_000)
		epochLen   = uint(432_000)
		epochEnd   = uint64(532_000)
		// tipSlot well past the safe-zone boundary
		tipSlot = uint64(520_000)
	)

	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra:   requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(500, epochStart, epochLen, eras.ConwayEraDesc.Id),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionImpossible, ls.transitionInfo.State)
}

// TestEvaluateTransitionImpossible_NotSetWhenSafeZoneInsideEpoch verifies
// that TransitionImpossible is NOT set when safeEndSlot < epochEndSlot.
func TestEvaluateTransitionImpossible_NotSetWhenSafeZoneInsideEpoch(t *testing.T) {
	const (
		epochStart = uint64(100_000)
		epochLen   = uint(432_000)
		// tipSlot one slot before the boundary: safeEnd = epochEnd - 1
		tipSlot = uint64(506_079) // 532_000 - 25_920 - 1
	)

	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra:   requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(500, epochStart, epochLen, eras.ConwayEraDesc.Id),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"safeEndSlot < epochEndSlot: TransitionImpossible must NOT be set")
}

// TestEvaluateTransitionImpossible_NoOpWhenTransitionKnown verifies that
// evaluateTransitionImpossible does not override a confirmed TransitionKnown.
func TestEvaluateTransitionImpossible_NoOpWhenTransitionKnown(t *testing.T) {
	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra:   requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(500, 100_000, 432_000, eras.ConwayEraDesc.Id),
		currentTip: ochainsync.Tip{
			// tipSlot past the safe-zone boundary → would normally trigger Impossible
			Point: ocommon.NewPoint(520_000, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionKnown(501),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State,
		"evaluateTransitionImpossible must not override TransitionKnown")
	assert.Equal(t, uint64(501), ls.transitionInfo.KnownEpoch)
}

// TestEvaluateTransitionImpossible_NoOpAlreadyImpossible verifies that the
// call is idempotent when TransitionImpossible is already set.
func TestEvaluateTransitionImpossible_NoOpAlreadyImpossible(t *testing.T) {
	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra:   requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: newTestEpoch(500, 100_000, 432_000, eras.ConwayEraDesc.Id),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(520_000, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionImpossible(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionImpossible, ls.transitionInfo.State)
}

// TestEvaluateTransitionImpossible_NoOpWhenEpochLengthZero verifies that a
// zero LengthInSlots (uninitialized epoch) is skipped safely.
func TestEvaluateTransitionImpossible_NoOpWhenEpochLengthZero(t *testing.T) {
	cfg := newTestEraHistoryCfg(t)
	ls := &LedgerState{
		currentEra:   requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentEpoch: models.Epoch{EpochId: 0, LengthInSlots: 0},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(999_999, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.evaluateTransitionImpossible()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"zero-length epoch must not trigger TransitionImpossible")
}

// ---------------------------------------------------------------------------
// evaluateTriggerAtEpoch tests
// ---------------------------------------------------------------------------

// newTestLedgerStateWithTrigger builds a minimal LedgerState with the given
// currentEra / currentEpoch / initial transitionInfo, and the requested
// TestXHardForkAtEpoch override wired into the config (keyed on the
// successor era's lowercase name).
func newTestLedgerStateWithTrigger(
	t *testing.T,
	currentEraId uint,
	currentEpochId uint64,
	initialTI hardfork.TransitionInfo,
	nextEraLower string,
	overrideEpoch *uint64,
	experimentalEnabled bool,
) *LedgerState {
	t.Helper()
	cfg := newTestEraHistoryCfg(t)
	if experimentalEnabled {
		enabled := true
		cfg.ExperimentalHardForksEnabled = &enabled
	}
	switch nextEraLower {
	case "shelley":
		cfg.TestShelleyHardForkAtEpoch = overrideEpoch
	case "allegra":
		cfg.TestAllegraHardForkAtEpoch = overrideEpoch
	case "mary":
		cfg.TestMaryHardForkAtEpoch = overrideEpoch
	case "alonzo":
		cfg.TestAlonzoHardForkAtEpoch = overrideEpoch
	case "babbage":
		cfg.TestBabbageHardForkAtEpoch = overrideEpoch
	case "conway":
		cfg.TestConwayHardForkAtEpoch = overrideEpoch
	}
	return &LedgerState{
		currentEra:     requireEraDesc(t, currentEraId),
		currentEpoch:   newTestEpoch(currentEpochId, 0, 432_000, currentEraId),
		transitionInfo: initialTI,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
}

// Happy path: in Byron, with ExperimentalHardForksEnabled and
// TestShelleyHardForkAtEpoch=5, and the current epoch before 5, the
// TransitionInfo is surfaced as TransitionKnown(5).
func TestEvaluateTriggerAtEpoch_SetsTransitionKnown(t *testing.T) {
	target := uint64(5)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionUnknown(),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, target, ls.transitionInfo.KnownEpoch)
}

// Without ExperimentalHardForksEnabled, the override is inert.
func TestEvaluateTriggerAtEpoch_InertWithoutExperimentalFlag(t *testing.T) {
	target := uint64(5)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionUnknown(),
		"shelley", &target, false,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"override must be ignored without ExperimentalHardForksEnabled")
}

// When currentEpoch.EpochId >= target epoch, the trigger is not applied
// (the transition should have already occurred).
func TestEvaluateTriggerAtEpoch_NotSetWhenEpochReached(t *testing.T) {
	target := uint64(5)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 5,
		hardfork.NewTransitionUnknown(),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
}

// The last known era has no successor: the call is a no-op even if
// Test<Next>HardForkAtEpoch happens to be set (not meaningful).
func TestEvaluateTriggerAtEpoch_NoOpOnFinalEra(t *testing.T) {
	target := uint64(100)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ConwayEraDesc.Id, 3,
		hardfork.NewTransitionUnknown(),
		// This test uses the default active era table, where Dijkstra is
		// gated off and Conway has no successor.
		"", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
}

// AtEpoch override supersedes a prior TransitionImpossible: AtEpoch is
// authoritative info about a known upcoming transition and must override the
// safe-zone-derived "no transition in this epoch" verdict.
func TestEvaluateTriggerAtEpoch_OverridesTransitionImpossible(t *testing.T) {
	target := uint64(10)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionImpossible(),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, target, ls.transitionInfo.KnownEpoch)
}

// AtEpoch override replaces a TransitionKnown set for a different epoch.
// Mirrors Haskell's shelleyTriggerHardFork short-circuit: the AtEpoch config
// is the truth and bypasses pparams-vote inspection entirely.
func TestEvaluateTriggerAtEpoch_ReplacesDifferentKnownEpoch(t *testing.T) {
	target := uint64(10)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionKnown(4),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, target, ls.transitionInfo.KnownEpoch,
		"AtEpoch override must replace a stale TransitionKnown(other)")
}

// Idempotent when already TransitionKnown at the same epoch.
func TestEvaluateTriggerAtEpoch_IdempotentOnSameEpoch(t *testing.T) {
	target := uint64(10)
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionKnown(target),
		"shelley", &target, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State)
	assert.Equal(t, target, ls.transitionInfo.KnownEpoch)
}

// No override configured at all: evaluateTriggerAtEpoch is a no-op.
func TestEvaluateTriggerAtEpoch_NoOpWithoutOverride(t *testing.T) {
	ls := newTestLedgerStateWithTrigger(
		t,
		eras.ByronEraDesc.Id, 3,
		hardfork.NewTransitionUnknown(),
		"", nil, true,
	)
	ls.evaluateTriggerAtEpoch()
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
}

// TestRolloverCommit_ResetsTransitionImpossible verifies that a plain epoch
// rollover (no HardFork, no era transition) resets TransitionImpossible to
// TransitionUnknown so the new epoch starts fresh.
func TestRolloverCommit_ResetsTransitionImpossible(t *testing.T) {
	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentPParams: babbagePParams(9),
		// Simulate state at end of epoch 500: TransitionImpossible was set
		// because the tip's safe zone reached the epoch end.
		transitionInfo: hardfork.NewTransitionImpossible(),
	}

	var eraTransitions []*EraTransitionResult
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch:   models.Epoch{EpochId: 501, StartSlot: 532_000, LengthInSlots: 432_000},
		NewCurrentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewCurrentPParams: babbagePParams(9),
		NewEpochCache:     []models.Epoch{{EpochId: 501}},
		HardFork:          nil,
	}

	ls.Lock()
	for _, eraResult := range eraTransitions {
		ls.applyEraTransition(eraResult)
	}
	if rolloverResult != nil {
		ls.epochCache = rolloverResult.NewEpochCache
		ls.currentEpoch = rolloverResult.NewCurrentEpoch
		ls.currentEra = rolloverResult.NewCurrentEra
		ls.currentPParams = rolloverResult.NewCurrentPParams
		if len(eraTransitions) == 0 {
			ls.transitionInfo = hardfork.NewTransitionUnknown()
		}
	}
	if len(eraTransitions) == 0 && rolloverResult != nil && rolloverResult.HardFork != nil {
		ls.transitionInfo = hardfork.NewTransitionKnown(rolloverResult.NewCurrentEpoch.EpochId)
	}
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"plain epoch rollover must reset TransitionImpossible to TransitionUnknown")
}

// TestApplyEraTransition_ClearsTransitionKnown verifies that
// applyEraTransition unconditionally clears a pending TransitionKnown, even
// when called outside of any epoch-rollover context (the "standalone
// era-transition block" case).
func TestApplyEraTransition_ClearsTransitionKnown(t *testing.T) {
	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: babbagePParams(8),
		transitionInfo: hardfork.NewTransitionKnown(500),
	}

	result := &EraTransitionResult{
		NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewPParams: babbagePParams(9),
	}

	// Simulate a standalone era-transition path: apply under the lock,
	// no epoch rollover involved.
	ls.Lock()
	ls.applyEraTransition(result)
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"TransitionKnown must be cleared when the new era becomes active")
	assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
}

// TestApplyEraTransition_ClearsTransitionUnknown confirms that calling
// applyEraTransition when transitionInfo is already TransitionUnknown is a
// no-op for the State field (still TransitionUnknown).
func TestApplyEraTransition_ClearsTransitionUnknown(t *testing.T) {
	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: babbagePParams(8),
		transitionInfo: hardfork.NewTransitionUnknown(),
	}

	result := &EraTransitionResult{
		NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewPParams: babbagePParams(9),
	}

	ls.Lock()
	ls.applyEraTransition(result)
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
}

// TestApplyEraTransition_PreservesAndUpdatesFields verifies that
// applyEraTransition correctly rotates currentPParams → prevEraPParams
// and installs result.NewPParams / result.NewEra.
func TestApplyEraTransition_PreservesAndUpdatesFields(t *testing.T) {
	oldPParams := babbagePParams(8)
	newPParams := babbagePParams(9)

	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: lcommon.ProtocolParameters(oldPParams),
		transitionInfo: hardfork.NewTransitionKnown(500),
	}

	result := &EraTransitionResult{
		NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewPParams: lcommon.ProtocolParameters(newPParams),
	}

	ls.Lock()
	ls.applyEraTransition(result)
	ls.Unlock()

	assert.Equal(t, lcommon.ProtocolParameters(oldPParams), ls.prevEraPParams,
		"old pparams must be preserved as prevEraPParams")
	assert.Equal(t, lcommon.ProtocolParameters(newPParams), ls.currentPParams,
		"new pparams must become currentPParams")
	assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id,
		"currentEra must be updated to the new era")
	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"transitionInfo must be cleared")
}

// TestApplyEraTransition_MultipleSteps_AllCleared verifies the chained-
// transition case (e.g. jumping two eras at once): each step clears
// transitionInfo, and the final state is TransitionUnknown.
func TestApplyEraTransition_MultipleSteps_AllCleared(t *testing.T) {
	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.AlonzoEraDesc.Id),
		currentPParams: babbagePParams(6),
		transitionInfo: hardfork.NewTransitionKnown(300),
	}

	steps := []*EraTransitionResult{
		{
			NewEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
			NewPParams: babbagePParams(8),
		},
		{
			NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
			NewPParams: babbagePParams(9),
		},
	}

	ls.Lock()
	for _, step := range steps {
		ls.applyEraTransition(step)
	}
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State)
	assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
}

// TestRolloverCommit_EraTransitionClearsTransitionInfo exercises the
// in-memory state update block (the rollover-commit path) with both
// eraTransitions and a rolloverResult to confirm that eraTransitions take
// precedence: TransitionKnown is cleared even when rolloverResult.HardFork
// is also set (should not happen in practice, but the logic must be safe).
func TestRolloverCommit_EraTransitionClearsTransitionInfo(t *testing.T) {
	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: babbagePParams(8),
		transitionInfo: hardfork.NewTransitionKnown(499),
	}

	eraTransitions := []*EraTransitionResult{
		{
			NewEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
			NewPParams: babbagePParams(9),
		},
	}
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch:   models.Epoch{EpochId: 500},
		NewCurrentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewCurrentPParams: babbagePParams(9),
		NewEpochCache:     []models.Epoch{{EpochId: 500}},
		HardFork: &HardForkInfo{
			OldVersion: ProtocolVersion{Major: 8},
			NewVersion: ProtocolVersion{Major: 9},
		},
	}

	// Replicate the rollover-commit block logic directly.
	ls.Lock()
	for _, eraResult := range eraTransitions {
		ls.applyEraTransition(eraResult)
	}
	ls.epochCache = rolloverResult.NewEpochCache
	ls.currentEpoch = rolloverResult.NewCurrentEpoch
	ls.currentEra = rolloverResult.NewCurrentEra
	ls.currentPParams = rolloverResult.NewCurrentPParams
	if len(eraTransitions) == 0 && rolloverResult.HardFork != nil {
		ls.transitionInfo = hardfork.NewTransitionKnown(rolloverResult.NewCurrentEpoch.EpochId)
	}
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"era transition must clear transitionInfo even when rolloverResult.HardFork is set")
}

// TestRolloverCommit_HardForkWithoutEraTransition verifies that
// TransitionKnown is set when rolloverResult.HardFork is non-nil and no era
// transition happened (the normal epoch-boundary version-bump window).
func TestRolloverCommit_HardForkWithoutEraTransition(t *testing.T) {
	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		currentPParams: babbagePParams(8),
		transitionInfo: hardfork.NewTransitionUnknown(),
	}

	var eraTransitions []*EraTransitionResult // empty — no standalone transition
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch:   models.Epoch{EpochId: 500},
		NewCurrentEra:     requireEraDesc(t, eras.BabbageEraDesc.Id),
		NewCurrentPParams: babbagePParams(9),
		NewEpochCache:     []models.Epoch{{EpochId: 500}},
		HardFork: &HardForkInfo{
			OldVersion: ProtocolVersion{Major: 8},
			NewVersion: ProtocolVersion{Major: 9},
		},
	}

	ls.Lock()
	for _, eraResult := range eraTransitions {
		ls.applyEraTransition(eraResult)
	}
	ls.epochCache = rolloverResult.NewEpochCache
	ls.currentEpoch = rolloverResult.NewCurrentEpoch
	ls.currentEra = rolloverResult.NewCurrentEra
	ls.currentPParams = rolloverResult.NewCurrentPParams
	if len(eraTransitions) == 0 && rolloverResult.HardFork != nil {
		ls.transitionInfo = hardfork.NewTransitionKnown(rolloverResult.NewCurrentEpoch.EpochId)
	}
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionKnown, ls.transitionInfo.State,
		"version bump at epoch boundary without era transition must set TransitionKnown")
	assert.Equal(t, uint64(500), ls.transitionInfo.KnownEpoch)
}

// TestRolloverCommit_NoHardFork_TransitionInfoUnchanged verifies that a plain
// epoch rollover (no HardFork, no era transition) leaves transitionInfo alone.
func TestRolloverCommit_NoHardFork_TransitionInfoUnchanged(t *testing.T) {
	ls := &LedgerState{
		currentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		currentPParams: babbagePParams(9),
		transitionInfo: hardfork.NewTransitionUnknown(),
	}

	var eraTransitions []*EraTransitionResult
	rolloverResult := &EpochRolloverResult{
		NewCurrentEpoch:   models.Epoch{EpochId: 501},
		NewCurrentEra:     requireEraDesc(t, eras.ConwayEraDesc.Id),
		NewCurrentPParams: babbagePParams(9),
		NewEpochCache:     []models.Epoch{{EpochId: 501}},
		HardFork:          nil,
	}

	ls.Lock()
	for _, eraResult := range eraTransitions {
		ls.applyEraTransition(eraResult)
	}
	ls.epochCache = rolloverResult.NewEpochCache
	ls.currentEpoch = rolloverResult.NewCurrentEpoch
	ls.currentEra = rolloverResult.NewCurrentEra
	ls.currentPParams = rolloverResult.NewCurrentPParams
	if len(eraTransitions) == 0 && rolloverResult.HardFork != nil {
		ls.transitionInfo = hardfork.NewTransitionKnown(rolloverResult.NewCurrentEpoch.EpochId)
	}
	ls.Unlock()

	assert.Equal(t, hardfork.TransitionUnknown, ls.transitionInfo.State,
		"plain epoch rollover must not change transitionInfo")
}

func TestLatestOpCertSequenceTracksHighestObservedAndRollback(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}

	var poolID [28]byte
	for i := range poolID {
		poolID[i] = byte(i + 1)
	}
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolID[:]))
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
		},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	sequence, found, err := ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, uint64(0), sequence)

	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 3, 10, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 7, 20, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 5, 30, nil))

	sequence, found, err = ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(7), sequence)

	require.NoError(t, db.RestorePoolStateAtSlot(15, nil))
	sequence, found, err = ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(3), sequence)
}

func TestLedgerProcessBlockTracksOpCertSequenceByIssuerVkeyHash(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}

	var issuerVkey lcommon.IssuerVkey
	for i := range issuerVkey {
		issuerVkey[i] = byte(i + 1)
	}
	pkh := lcommon.PoolKeyHash(issuerVkey.Hash())
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
		},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	block := &babbage.BabbageBlock{
		BlockHeader: &babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				Slot:       10,
				IssuerVkey: issuerVkey,
				OpCert: babbage.BabbageOpCert{
					SequenceNumber: 4,
				},
			},
		},
	}

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, err := ls.ledgerProcessBlock(
			txn,
			ocommon.Point{Slot: 10},
			block,
			false,
			false,
			nil,
			nil,
			eras.BabbageEraDesc,
			nil,
			nil,
		)
		return err
	}))

	var poolID [28]byte
	copy(poolID[:], pkh.Bytes())
	sequence, found, err := ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(4), sequence)
}
