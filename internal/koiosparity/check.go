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
	DingoAPIURL  string
	CachePath    string
	Workers      int
	All          bool   // re-check all cached epochs
	FromEpoch    uint64 // 0 = all unchecked
	ThroughEpoch uint64 // 0 = no upper bound
	GraceHours   int    // pools-only-in-dingo in recently-fetched epochs → reference_lag
}

// CheckResult summarises a completed check run.
type CheckResult struct {
	EpochsChecked int
	PoolsChecked  int
	MismatchCount int
	FailEpochs    []uint64
	ErrorEpochs   []uint64
}

// Check compares Koios cache against Dingo's Blockfrost API for unchecked epochs.
func Check(ctx context.Context, cfg CheckConfig, logger *slog.Logger) (*CheckResult, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.NumCPU()
	}
	if cfg.GraceHours <= 0 {
		cfg.GraceHours = 24
	}

	cache, err := OpenCache(cfg.CachePath, logger)
	if err != nil {
		return nil, fmt.Errorf("open cache: %w", err)
	}

	dingo := NewBlockfrostClient(cfg.DingoAPIURL)

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
		"grace_hours", cfg.GraceHours,
	)

	result := &CheckResult{}
	var mu sync.Mutex
	sem := make(chan struct{}, cfg.Workers)
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	// Pre-fetch pool list from Dingo once (shared across epoch checks).
	dingoPools, dingoPoolsErr := dingo.GetPoolsExtended(ctx)
	if dingoPoolsErr != nil && dingoPoolsErr != ErrAPINotFound {
		logger.Warn("koiosparity: could not fetch Dingo pool list",
			"error", dingoPoolsErr,
		)
	}
	dingoPoolSet := make(map[string]bool, len(dingoPools))
	for _, p := range dingoPools {
		dingoPoolSet[p.PoolID] = true
	}

	grace := time.Duration(cfg.GraceHours) * time.Hour

	for _, epoch := range epochs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(epoch uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			res, checkErr := checkEpoch(
				ctx, cache, dingo, cfg.Network, epoch,
				dingoPoolSet, dingoPoolsErr, grace, logger,
			)
			if checkErr != nil {
				select {
				case errCh <- fmt.Errorf("epoch %d: %w", epoch, checkErr):
				default:
				}
				return
			}

			mu.Lock()
			result.EpochsChecked++
			result.PoolsChecked += res.DingoPoolCount + res.KoiosPoolCount
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

// checkEpoch performs the comparison for a single epoch and persists results.
func checkEpoch(
	ctx context.Context,
	cache *Cache,
	dingo *BlockfrostClient,
	network string,
	epoch uint64,
	dingoPoolSet map[string]bool,
	dingoPoolsErr error,
	grace time.Duration,
	logger *slog.Logger,
) (*EpochCompareResult, error) {
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

	// If Koios data was fetched within the grace window the epoch may still be
	// incomplete on Koios's side — pools absent from Koios get reference_lag.
	recentFetch := time.Since(koiosEpoch.FetchedAt) < grace

	// Build a map of Koios pools by ID.
	koiosPoolMap := make(map[string]*KoiosPoolEpoch, len(koiosPools))
	for i := range koiosPools {
		koiosPoolMap[koiosPools[i].PoolBech32] = &koiosPools[i]
	}

	// Clear previous mismatch records for this epoch.
	if err := cache.DeleteEpochMismatches(network, epoch); err != nil {
		return nil, fmt.Errorf("delete old mismatches: %w", err)
	}

	var allMismatches []CheckMismatch

	// Compare epoch aggregates.
	dingoEpoch, epochFetchErr := dingo.GetEpoch(ctx, epoch)
	aggMismatches := CompareEpochAggregates(
		network, epoch, koiosEpoch, dingoEpoch, epochFetchErr, now,
	)
	allMismatches = append(allMismatches, aggMismatches...)

	// Per-pool comparison: build union of pool IDs from both sides.
	allPoolIDs := make(map[string]bool, len(koiosPoolMap)+len(dingoPoolSet))
	for id := range koiosPoolMap {
		allPoolIDs[id] = true
	}
	for id := range dingoPoolSet {
		allPoolIDs[id] = true
	}

	var onlyDingo []string
	var onlyKoios []string

	for poolID := range allPoolIDs {
		koiosPool, inKoios := koiosPoolMap[poolID]
		_, inDingo := dingoPoolSet[poolID]

		if inDingo && !inKoios {
			// Pool is in Dingo but absent from Koios.
			// If the Koios data is recent, Koios may simply not have indexed it yet.
			cat := CategoryPoolOnlyDingo
			if recentFetch {
				cat = CategoryReferenceLag
			}
			onlyDingo = append(onlyDingo, poolID)
			allMismatches = append(allMismatches, CheckMismatch{
				Network:    network,
				Epoch:      epoch,
				PoolBech32: poolID,
				Field:      "pool_presence",
				DingoValue: "present",
				KoiosValue: "",
				Category:   cat,
				CheckedAt:  now,
			})
			continue
		}

		if !inDingo && inKoios {
			onlyKoios = append(onlyKoios, poolID)
			poolMismatches := ComparePoolEpoch(
				network, epoch, koiosPool, nil, nil, now,
			)
			allMismatches = append(allMismatches, poolMismatches...)
			continue
		}

		// In both — fetch Dingo pool history for this epoch.
		dingoItem, histErr := dingo.GetPoolHistoryForEpoch(ctx, poolID, epoch)
		poolMismatches := ComparePoolEpoch(
			network, epoch, koiosPool, dingoItem, histErr, now,
		)
		allMismatches = append(allMismatches, poolMismatches...)
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
		DingoPoolCount: len(dingoPoolSet),
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
		"recent_fetch", recentFetch,
	)

	return &EpochCompareResult{
		Network:        network,
		Epoch:          epoch,
		Status:         status,
		Mismatches:     allMismatches,
		DingoPoolCount: len(dingoPoolSet),
		KoiosPoolCount: len(koiosPools),
		OnlyDingo:      onlyDingo,
		OnlyKoios:      onlyKoios,
	}, nil
}
