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
	// We only compare closed epochs: tip - 1.
	throughEpoch := tipEpoch - 1
	if cfg.ThroughEpoch > 0 && cfg.ThroughEpoch < throughEpoch {
		throughEpoch = cfg.ThroughEpoch
	}

	fromEpoch := cfg.FromEpoch
	if fromEpoch == 0 {
		// Resume: start from last cached epoch + 1.
		_, lastCached, rangeErr := cache.GetFetchedEpochRange(cfg.Network)
		if rangeErr == nil && lastCached > 0 {
			fromEpoch = lastCached + 1
		} else {
			fromEpoch = 0 // start from genesis
		}
	}

	if fromEpoch > throughEpoch {
		logger.Info("koiosparity: fetch cache is up-to-date",
			"network", cfg.Network,
			"last_epoch", throughEpoch,
		)
		return &FetchResult{FromEpoch: fromEpoch, ThroughEpoch: throughEpoch}, nil
	}

	// Build list of epochs to fetch.
	epochs := make([]uint64, 0, int(throughEpoch-fromEpoch+1))
	for e := fromEpoch; e <= throughEpoch; e++ {
		epochs = append(epochs, e)
	}

	logger.Info("koiosparity: fetching epochs from Koios",
		"network", cfg.Network,
		"from", fromEpoch,
		"through", throughEpoch,
		"count", len(epochs),
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

			pools, fetchErr := fetchEpoch(ctx, koios, cache, cfg.Network, epoch, logger)
			if fetchErr != nil {
				select {
				case errCh <- fmt.Errorf("epoch %d: %w", epoch, fetchErr):
				default:
				}
				return
			}
			mu.Lock()
			result.EpochsFetched++
			result.PoolsFetched += pools
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
// Returns the number of pool rows upserted.
func fetchEpoch(
	ctx context.Context,
	koios *KoiosClient,
	cache *Cache,
	network string,
	epoch uint64,
	logger *slog.Logger,
) (int, error) {
	// 1. Epoch info.
	info, err := koios.GetEpochInfo(ctx, epoch)
	if err != nil {
		return 0, fmt.Errorf("get epoch info: %w", err)
	}

	now := time.Now()
	if err := cache.UpsertEpochInfo(KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  info.ActiveStake,
		PoolCnt:      info.PoolCnt,
		DelegatorCnt: info.DelegatorCnt,
		Fees:         info.Fees,
		TotalRewards: info.TotalRewards,
		FetchedAt:    now,
	}); err != nil {
		return 0, fmt.Errorf("upsert epoch info: %w", err)
	}

	// 2. Discover all pools active in this epoch via pool_list.
	pools, err := koios.GetPoolList(ctx)
	if err != nil {
		return 0, fmt.Errorf("get pool list: %w", err)
	}

	// 3. For each pool, fetch history for this epoch and store if present.
	poolCount := 0
	poolSem := make(chan struct{}, 5)
	var poolWg sync.WaitGroup
	var poolMu sync.Mutex
	poolErrCh := make(chan error, 1)

	for _, pool := range pools {
		select {
		case <-ctx.Done():
			return poolCount, ctx.Err()
		case poolSem <- struct{}{}:
		}

		poolWg.Add(1)
		go func(p KoiosPoolListItem) {
			defer poolWg.Done()
			defer func() { <-poolSem }()

			item, histErr := koios.GetPoolEpochHistory(ctx, p.PoolIDBech32, epoch)
			if histErr != nil {
				logger.Debug("koiosparity: skip pool history",
					"pool", p.PoolIDBech32,
					"epoch", epoch,
					"error", histErr,
				)
				return
			}
			if item == nil {
				return // Pool wasn't active this epoch.
			}

			if upsertErr := cache.UpsertPoolEpoch(KoiosPoolEpoch{
				Network:     network,
				Epoch:       epoch,
				PoolBech32:  p.PoolIDBech32,
				ActiveStake: item.ActiveStake,
				BlockCnt:    item.BlockCnt,
				Delegators:  item.DelegatorCnt,
				FetchedAt:   now,
			}); upsertErr != nil {
				select {
				case poolErrCh <- fmt.Errorf("upsert pool %s: %w", p.PoolIDBech32, upsertErr):
				default:
				}
				return
			}
			poolMu.Lock()
			poolCount++
			poolMu.Unlock()
		}(pool)
	}

	poolWg.Wait()
	select {
	case err := <-poolErrCh:
		return poolCount, err
	default:
	}

	logger.Debug("koiosparity: epoch fetched",
		"network", network,
		"epoch", epoch,
		"pools", poolCount,
	)
	return poolCount, nil
}
