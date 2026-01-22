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
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	for i := 0; i < 5; i++ {
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

	// Give operations time to queue
	time.Sleep(20 * time.Millisecond)

	// Shutdown the pool - this should wait for all operations to complete
	pool.Shutdown()

	// Wait for all result handlers
	wg.Wait()

	// Verify all operations completed
	assert.Equal(t, int32(5), completedCount.Load(), "not all operations completed before shutdown returned")
}

// TestDatabaseWorkerPoolShutdownWithErrors tests error handling during shutdown
func TestDatabaseWorkerPoolShutdownWithErrors(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 2
	config.TaskQueueSize = 10

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32

	// Submit operations, some will error
	for i := 0; i < 3; i++ {
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
	assert.Equal(t, int32(3), completedCount.Load(), "not all operations completed")
}

// TestDatabaseWorkerPoolQueueFull tests behavior when queue is full
func TestDatabaseWorkerPoolQueueFull(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 1
	config.TaskQueueSize = 1 // Very small queue

	pool := NewDatabaseWorkerPool(nil, config)

	// Submit some operations
	for i := 0; i < 3; i++ {
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

// TestDatabaseWorkerPoolConcurrency tests the pool under concurrent load
func TestDatabaseWorkerPoolConcurrency(t *testing.T) {
	config := DefaultDatabaseWorkerPoolConfig()
	config.WorkerPoolSize = 5
	config.TaskQueueSize = 50

	pool := NewDatabaseWorkerPool(nil, config)

	var completedCount atomic.Int32

	// Submit many operations
	numOperations := 20
	for i := 0; i < numOperations; i++ {
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
	for i := 0; i < 3; i++ {
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
