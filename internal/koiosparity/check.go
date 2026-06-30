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
	GraceHours   int    // retained for config compatibility; not currently used
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
	)

	result := &CheckResult{}
	var mu sync.Mutex
	sem := make(chan struct{}, cfg.Workers)
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

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

			res, checkErr := checkEpoch(ctx, cache, dingo, cfg.Network, epoch, logger)
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
// Koios pool membership is authoritative: only pools Koios knows about for
// this epoch are compared, avoiding false pool_only_dingo mismatches from the
// global /pools/extended registry.
func checkEpoch(
	ctx context.Context,
	cache *Cache,
	dingo *BlockfrostClient,
	network string,
	epoch uint64,
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

	// Per-pool comparison: iterate Koios pools only.
	// Using the global /pools/extended as the Dingo side would flag pools that
	// exist in Dingo but were inactive this epoch as false pool_only_dingo hits.
	var onlyKoios []string
	dingoFound := 0

	for i := range koiosPools {
		koiosPool := &koiosPools[i]
		dingoItem, histErr := dingo.GetPoolHistoryForEpoch(ctx, koiosPool.PoolBech32, epoch)
		poolMismatches := ComparePoolEpoch(
			network, epoch, koiosPool, dingoItem, histErr, now,
		)
		if dingoItem == nil && histErr == nil {
			onlyKoios = append(onlyKoios, koiosPool.PoolBech32)
		} else if dingoItem != nil {
			dingoFound++
		}
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
		DingoPoolCount: dingoFound,
		KoiosPoolCount: len(koiosPools),
		OnlyDingoPools: MarshalPoolList(nil),
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
	)

	return &EpochCompareResult{
		Network:        network,
		Epoch:          epoch,
		Status:         status,
		Mismatches:     allMismatches,
		DingoPoolCount: dingoFound,
		KoiosPoolCount: len(koiosPools),
		OnlyDingo:      nil,
		OnlyKoios:      onlyKoios,
	}, nil
}
