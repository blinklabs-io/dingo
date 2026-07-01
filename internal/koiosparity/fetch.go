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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// FetchConfig holds parameters for a Koios fetch run.
type FetchConfig struct {
	Network      string
	APIKey       string
	CachePath    string
	Concurrency  int
	FromEpoch    uint64 // 0 = resume from last cached + 1
	ThroughEpoch uint64 // 0 = tip - 1
}

// FetchResult summarises a completed fetch run.
type FetchResult struct {
	EpochsFetched int
	PoolsFetched  int
	FromEpoch     uint64
	ThroughEpoch  uint64
}

// Fetch pulls Koios data into the cache, resuming from the last cached epoch.
func Fetch(ctx context.Context, cfg FetchConfig, logger *slog.Logger) (*FetchResult, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}

	cache, err := OpenCache(cfg.CachePath, logger)
	if err != nil {
		return nil, fmt.Errorf("open cache: %w", err)
	}

	koios, err := NewKoiosClient(cfg.Network, cfg.APIKey)
	if err != nil {
		return nil, err
	}

	// Determine epoch range.
	tipEpoch, err := koios.GetTipEpoch(ctx)
	if err != nil {
		return nil, fmt.Errorf("get tip epoch: %w", err)
	}
	if tipEpoch == 0 {
		return nil, errors.New("koios tip epoch is 0: no closed epochs to fetch")
	}
	// We only compare closed epochs: tip - 1.
	throughEpoch := tipEpoch - 1
	if cfg.ThroughEpoch > 0 && cfg.ThroughEpoch < throughEpoch {
		throughEpoch = cfg.ThroughEpoch
	}

	// fromEpoch = 0 means start from genesis; GetUncachedEpochs will skip
	// whatever is already cached, so no resume logic is needed here.
	fromEpoch := cfg.FromEpoch

	if fromEpoch > throughEpoch {
		logger.Info("koiosparity: fetch cache is up-to-date",
			"network", cfg.Network,
			"last_epoch", throughEpoch,
		)
		return &FetchResult{FromEpoch: fromEpoch, ThroughEpoch: throughEpoch}, nil
	}

	// Collect every pool that has ever been registered on chain, including
	// retired pools. /pool_registrations is the authoritative certificate
	// history; deduplicating its pool_id_bech32 column gives the complete set.
	// We hoist this once per Fetch run rather than once per epoch because the
	// list grows monotonically and fetching it once is far cheaper on wide
	// backfills.
	poolIDs, err := koios.GetAllHistoricalPoolIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("get historical pool IDs: %w", err)
	}

	// Build list of epochs to fetch: only those NOT already in the cache.
	// GetUncachedEpochs fills holes left by prior failed or interrupted runs
	// rather than naively resuming from max(fetched)+1, which would skip gaps.
	epochs, err := cache.GetUncachedEpochs(cfg.Network, fromEpoch, throughEpoch)
	if err != nil {
		return nil, fmt.Errorf("get uncached epochs: %w", err)
	}

	if len(epochs) == 0 {
		logger.Info("koiosparity: fetch cache is up-to-date",
			"network", cfg.Network,
			"last_epoch", throughEpoch,
		)
		return &FetchResult{FromEpoch: fromEpoch, ThroughEpoch: throughEpoch}, nil
	}

	logger.Info("koiosparity: fetching epochs from Koios",
		"network", cfg.Network,
		"from", fromEpoch,
		"through", throughEpoch,
		"count", len(epochs),
		"pools", len(poolIDs),
		"concurrency", cfg.Concurrency,
	)

	result := &FetchResult{FromEpoch: fromEpoch, ThroughEpoch: throughEpoch}
	var mu sync.Mutex

	sem := make(chan struct{}, cfg.Concurrency)
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

			cnt, fetchErr := fetchEpoch(ctx, koios, cache, cfg.Network, epoch, poolIDs, logger)
			if fetchErr != nil {
				select {
				case errCh <- fmt.Errorf("epoch %d: %w", epoch, fetchErr):
				default:
				}
				return
			}
			mu.Lock()
			result.EpochsFetched++
			result.PoolsFetched += cnt
			mu.Unlock()
		}(epoch)
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	logger.Info("koiosparity: fetch complete",
		"network", cfg.Network,
		"epochs", result.EpochsFetched,
		"pools", result.PoolsFetched,
	)
	return result, nil
}

// fetchEpoch fetches and caches one epoch's worth of Koios data.
//
// Pool rows are written before epoch info so the resume cursor
// (koios_epoch_info presence) is never advanced for a partially-cached epoch.
// Pool history errors are propagated rather than silently skipped so incomplete
// cache rows are never treated as valid reference data.
func fetchEpoch(
	ctx context.Context,
	koios *KoiosClient,
	cache *Cache,
	network string,
	epoch uint64,
	poolIDs []string,
	logger *slog.Logger,
) (int, error) {
	// 1. Fetch epoch info (not written yet).
	info, err := koios.GetEpochInfo(ctx, epoch)
	if err != nil {
		return 0, fmt.Errorf("get epoch info: %w", err)
	}

	now := time.Now()

	// 2. Fetch per-pool epoch history rows in parallel.
	// _pool_bech32 is a required Koios parameter; _epoch_no filters server-side
	// so each call returns at most one row instead of the full pool history.
	poolCount := 0
	poolSem := make(chan struct{}, 5)
	var poolWg sync.WaitGroup
	var poolMu sync.Mutex
	poolErrCh := make(chan error, 1)

	// Use a labeled break so that on cancellation we stop spawning new workers
	// but still drain already-started goroutines via poolWg.Wait() below.
	// An early return here would let running goroutines race the cache after
	// the caller proceeds.
outer:
	for _, poolID := range poolIDs {
		select {
		case <-ctx.Done():
			break outer
		case poolSem <- struct{}{}:
		}

		poolWg.Add(1)
		go func(id string) {
			defer poolWg.Done()
			defer func() { <-poolSem }()

			item, histErr := koios.GetPoolEpochHistory(ctx, id, epoch)
			if histErr != nil {
				select {
				case poolErrCh <- fmt.Errorf("pool %s history: %w", id, histErr):
				default:
				}
				return
			}
			if item == nil {
				return // Pool wasn't active this epoch.
			}

			if upsertErr := cache.UpsertPoolEpoch(KoiosPoolEpoch{
				Network:     network,
				Epoch:       epoch,
				PoolBech32:  id,
				ActiveStake: item.ActiveStake,
				BlockCnt:    item.BlockCnt,
				Delegators:  item.DelegatorCnt,
				FetchedAt:   now,
			}); upsertErr != nil {
				select {
				case poolErrCh <- fmt.Errorf("upsert pool %s: %w", id, upsertErr):
				default:
				}
				return
			}
			poolMu.Lock()
			poolCount++
			poolMu.Unlock()
		}(poolID)
	}

	poolWg.Wait() // always drain started goroutines before returning

	// If context was cancelled, report that rather than any pool error.
	if ctx.Err() != nil {
		return poolCount, ctx.Err()
	}

	select {
	case err := <-poolErrCh:
		return poolCount, err
	default:
	}

	// 3. Write epoch info only after all pool rows have succeeded.
	//
	// Reject epochs where Koios returned no end_time. All closed epochs should
	// have a valid Unix timestamp; a zero means the response is incomplete.
	// Rather than cache broken data that permanently disables the grace-window
	// logic, return an error so the epoch stays uncached and is retried on the
	// next fetch run.
	if info.EndTime == 0 {
		return 0, fmt.Errorf("epoch %d: koios returned end_time=0 — epoch may not be fully closed yet", epoch)
	}
	epochEndTime := time.Unix(info.EndTime, 0).UTC()

	if err := cache.UpsertEpochInfo(KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  info.ActiveStake,
		PoolCnt:      info.PoolCnt,
		DelegatorCnt: info.DelegatorCnt,
		Fees:         info.Fees,
		TotalRewards: info.TotalRewards,
		EpochEndTime: epochEndTime,
		FetchedAt:    now,
	}); err != nil {
		return 0, fmt.Errorf("upsert epoch info: %w", err)
	}

	logger.Debug("koiosparity: epoch fetched",
		"network", network,
		"epoch", epoch,
		"pools", poolCount,
	)
	return poolCount, nil
}
