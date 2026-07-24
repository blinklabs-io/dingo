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

package koiosparity

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"
)

// CheckConfig holds parameters for a parity check run.
type CheckConfig struct {
	Network      string
	DingoDB      DingoDBConfig // which backend to read Dingo reward state from
	CachePath    string
	Workers      int
	All          bool   // re-check all cached epochs, not just unchecked/stale ones
	FromEpoch    uint64 // 0 = all unchecked/stale
	ThroughEpoch uint64 // 0 = no upper bound
	GraceHours   int    // pools/epochs missing from Dingo within this window → reference_lag, not FAIL
}

// CheckResult summarises a completed check run.
type CheckResult struct {
	EpochsChecked int
	PoolsChecked  int
	MismatchCount int
	FailEpochs    []uint64
	ErrorEpochs   []uint64
}

// Check compares the Koios reference cache against Dingo's metadata database
// for unchecked or stale epochs. It reads reward_pool_input and epoch_summary
// directly — no Blockfrost or other HTTP endpoints are contacted.
func Check(ctx context.Context, cfg CheckConfig, logger *slog.Logger) (*CheckResult, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
	}

	cache, err := OpenCache(cfg.CachePath, logger)
	if err != nil {
		return nil, fmt.Errorf("open cache: %w", err)
	}
	defer cache.Close() //nolint:errcheck

	dingo, err := OpenDingoDB(cfg.DingoDB)
	if err != nil {
		return nil, fmt.Errorf("open dingo db: %w", err)
	}
	defer dingo.Close() //nolint:errcheck

	// Determine which epochs to check.
	var epochs []uint64
	if cfg.All {
		epochs, err = cache.GetAllFetchedEpochs(cfg.Network)
	} else {
		epochs, err = cache.GetEpochsNeedingCheck(cfg.Network)
	}
	if err != nil {
		return nil, fmt.Errorf("get epochs to check: %w", err)
	}

	// Apply from/through bounds.
	filtered := make([]uint64, 0, len(epochs))
	for _, e := range epochs {
		if cfg.FromEpoch > 0 && e < cfg.FromEpoch {
			continue
		}
		if cfg.ThroughEpoch > 0 && e > cfg.ThroughEpoch {
			continue
		}
		filtered = append(filtered, e)
	}
	epochs = filtered

	if len(epochs) == 0 {
		logger.Info("koiosparity: nothing to check", "network", cfg.Network)
		return &CheckResult{}, nil
	}

	logger.Info("koiosparity: checking epochs",
		"network", cfg.Network,
		"count", len(epochs),
		"workers", cfg.Workers,
	)

	result := &CheckResult{}
	var mu sync.Mutex
	sem := make(chan struct{}, cfg.Workers)
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

loop:
	for _, epoch := range epochs {
		select {
		case <-ctx.Done():
			break loop
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(epoch uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			res, checkErr := checkEpoch(ctx, cache, dingo, cfg.Network, epoch, cfg.GraceHours, logger)
			if checkErr != nil {
				select {
				case errCh <- fmt.Errorf("epoch %d: %w", epoch, checkErr):
				default:
				}
				return
			}

			mu.Lock()
			result.EpochsChecked++
			result.PoolsChecked += res.KoiosPoolCount
			result.MismatchCount += len(res.Mismatches)
			switch res.Status {
			case StatusFail:
				result.FailEpochs = append(result.FailEpochs, epoch)
			case StatusError:
				result.ErrorEpochs = append(result.ErrorEpochs, epoch)
			}
			mu.Unlock()
		}(epoch)
	}

	wg.Wait()

	// Check cancellation before consuming errCh so a clean shutdown returns
	// ctx.Err() rather than a mid-flight epoch error.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	logger.Info("koiosparity: check complete",
		"network", cfg.Network,
		"epochs", result.EpochsChecked,
		"mismatches", result.MismatchCount,
		"fail_epochs", len(result.FailEpochs),
	)
	return result, nil
}

// checkEpoch performs the full comparison for one epoch and persists results.
//
// Epoch aggregates (epoch_summary, reward_ada_pots) are compared against the
// Koios epoch_info row. Per-pool reward inputs (reward_pool_input) are compared
// against Koios pool_history rows using a single bulk DB query per epoch.
// Koios defines epoch pool membership; pools in Dingo but not in Koios are
// flagged as pool_only_dingo.
func checkEpoch(
	ctx context.Context,
	cache *Cache,
	dingo *DingoDB,
	network string,
	epoch uint64,
	graceHours int,
	logger *slog.Logger,
) (*EpochCompareResult, error) {
	_ = ctx // reserved for future cancellation within DB calls
	now := time.Now()

	// Load Koios reference data.
	koiosEpoch, err := cache.GetEpochInfo(network, epoch)
	if err != nil {
		return nil, fmt.Errorf("get koios epoch info: %w", err)
	}
	koiosPools, err := cache.GetAllPoolsForEpoch(network, epoch)
	if err != nil {
		return nil, fmt.Errorf("get koios pool epoch: %w", err)
	}

	// Clear previous mismatch records for this epoch before writing new ones.
	if err := cache.DeleteEpochMismatches(network, epoch); err != nil {
		return nil, fmt.Errorf("delete old mismatches: %w", err)
	}

	// Pre-staking epochs (Koios active_stake=null) have no reference data to
	// compare against — record PASS with zero mismatches rather than running
	// comparisons against an empty pool set, which would spuriously flag every
	// Dingo-side pool as pool_only_dingo.
	if koiosEpoch.PreStaking {
		if err := cache.UpsertCheckEpochStatus(CheckEpochStatus{
			Network:       network,
			Epoch:         epoch,
			LastCheckedAt: now,
			Status:        StatusPass,
		}); err != nil {
			return nil, fmt.Errorf("upsert check status: %w", err)
		}
		logger.Debug("koiosparity: epoch predates staking, skipping comparison",
			"network", network,
			"epoch", epoch,
		)
		return &EpochCompareResult{
			Network: network,
			Epoch:   epoch,
			Status:  StatusPass,
		}, nil
	}

	var allMismatches []CheckMismatch

	// 1. Compare epoch-level aggregates.
	dingoEpoch, epochErr := dingo.GetEpochData(epoch)
	allMismatches = append(allMismatches,
		CompareEpochAggregates(network, epoch, koiosEpoch, dingoEpoch, epochErr, now, graceHours)...,
	)

	// 2. Bulk-load all pool reward inputs for this epoch from Dingo's DB.
	// This is a single query regardless of how many pools Koios knows about.
	dingoPoolMap, dingoPoolErr := dingo.GetPoolEpochDataMap(epoch)
	if dingoPoolErr != nil {
		// Record the DB failure and skip all per-pool comparisons.
		// Continuing with dingoPoolMap == nil would make every Koios pool appear
		// as pool_only_koios (FAIL), masking the real ERROR cause.
		allMismatches = append(allMismatches, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "reward_pool_input",
			DingoValue: fmt.Sprintf("error: %v", dingoPoolErr),
			KoiosValue: "",
			Category:   CategoryDBError,
			CheckedAt:  now,
		})
	}

	// 3. Build a set of pool-key-hash → bech32 from Koios pools so we can
	// detect pools present in Dingo but absent from Koios.
	// Skipped entirely when the bulk DB query failed — the dingo_db_error above covers it.
	epochEndTime := koiosEpoch.EpochEndTime // zero for old cache rows; grace window skips if zero
	var koiosKeySet map[string]struct{}
	var onlyKoios []string
	dingoFound := 0

	if dingoPoolErr == nil {
		koiosKeySet = make(map[string]struct{}, len(koiosPools))
		for i := range koiosPools {
			koiosPool := &koiosPools[i]
			keyHex, decErr := PoolKeyHashHex(koiosPool.PoolBech32)
			if decErr != nil {
				// Bad bech32 in our cache — surface as ERROR so PASS is never silently wrong.
				logger.Warn("koiosparity: failed to decode pool bech32",
					"pool", koiosPool.PoolBech32, "error", decErr)
				allMismatches = append(allMismatches, CheckMismatch{
					Network:    network,
					Epoch:      epoch,
					PoolBech32: koiosPool.PoolBech32,
					Field:      "pool_bech32_decode",
					DingoValue: fmt.Sprintf("error: %v", decErr),
					KoiosValue: koiosPool.PoolBech32,
					Category:   CategoryDBError,
					CheckedAt:  now,
				})
				onlyKoios = append(onlyKoios, koiosPool.PoolBech32)
				continue
			}
			koiosKeySet[keyHex] = struct{}{}

			dingoPool := dingoPoolMap[keyHex]
			poolMismatches := ComparePoolEpoch(network, epoch, koiosPool, dingoPool, now, graceHours, epochEndTime)
			allMismatches = append(allMismatches, poolMismatches...)

			if dingoPool == nil {
				onlyKoios = append(onlyKoios, koiosPool.PoolBech32)
			} else {
				dingoFound++
			}
		}
	}

	// 4. Detect pools that Dingo computed rewards for but Koios doesn't list.
	// Skipped when bulk DB query failed (dingoPoolMap is nil/invalid).
	// Convert key-hash hex back to bech32 so PoolBech32 is consistently formatted.
	var onlyDingo []string
	if dingoPoolErr == nil {
		for keyHex := range dingoPoolMap {
			if _, inKoios := koiosKeySet[keyHex]; !inKoios {
				poolBech32, bechErr := PoolKeyHashHexToBech32(keyHex)
				if bechErr != nil {
					poolBech32 = keyHex // unreachable: hex from GetPoolEpochDataMap is always valid
				}
				onlyDingo = append(onlyDingo, poolBech32)
				allMismatches = append(allMismatches, CheckMismatch{
					Network:    network,
					Epoch:      epoch,
					PoolBech32: poolBech32,
					Field:      "pool_presence",
					DingoValue: "present",
					KoiosValue: "",
					Category:   CategoryPoolOnlyDingo,
					CheckedAt:  now,
				})
			}
		}
	}

	status := DetermineStatus(allMismatches)

	if err := cache.InsertMismatches(allMismatches); err != nil {
		return nil, fmt.Errorf("insert mismatches: %w", err)
	}

	if err := cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:        network,
		Epoch:          epoch,
		LastCheckedAt:  now,
		Status:         status,
		MismatchCount:  len(allMismatches),
		DingoPoolCount: dingoFound,
		KoiosPoolCount: len(koiosPools),
		OnlyDingoPools: MarshalPoolList(onlyDingo),
		OnlyKoiosPools: MarshalPoolList(onlyKoios),
	}); err != nil {
		return nil, fmt.Errorf("upsert check status: %w", err)
	}

	logger.Debug("koiosparity: epoch checked",
		"network", network,
		"epoch", epoch,
		"status", status,
		"mismatches", len(allMismatches),
		"dingo_found", dingoFound,
		"koios_pools", len(koiosPools),
		"only_dingo", len(onlyDingo),
	)

	return &EpochCompareResult{
		Network:        network,
		Epoch:          epoch,
		Status:         status,
		Mismatches:     allMismatches,
		DingoPoolCount: dingoFound,
		KoiosPoolCount: len(koiosPools),
		OnlyDingo:      onlyDingo,
		OnlyKoios:      onlyKoios,
	}, nil
}
