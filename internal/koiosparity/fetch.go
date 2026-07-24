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
	"strconv"
	"sync"
	"sync/atomic"
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
	ForceRefresh bool   // re-fetch epochs already in cache (overwrite); implies FromEpoch is a hard start
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
	defer cache.Close() //nolint:errcheck

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
	// retired pools. /pool_list is the authoritative source; we hoist this once
	// per Fetch run rather than once per epoch because the list grows
	// monotonically and fetching it once is far cheaper on wide backfills.
	poolIDs, err := koios.GetAllHistoricalPoolIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("get historical pool IDs: %w", err)
	}

	// Build list of epochs to fetch.
	// Normal mode: only epochs NOT already in the cache (fills holes from prior
	// failed/interrupted runs rather than naively resuming from max+1).
	// ForceRefresh mode: fetch the full range and overwrite cached rows, used
	// when the user suspects stale or corrupt cached data in [fromEpoch, through].
	var epochs []uint64
	if cfg.ForceRefresh {
		for e := fromEpoch; e <= throughEpoch; e++ {
			epochs = append(epochs, e)
		}
	} else {
		epochs, err = cache.GetUncachedEpochs(cfg.Network, fromEpoch, throughEpoch)
		if err != nil {
			return nil, fmt.Errorf("get uncached epochs: %w", err)
		}
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

	// fetchCtx is cancelled the moment any epoch hits a real (non-pre-staking)
	// error, so a failure surfaces immediately instead of waiting out the rest
	// of a batch that can run for hours. The final ctx.Err() check below still
	// inspects the original, un-derived ctx so a genuine caller cancellation is
	// never confused with this internal fail-fast cancellation.
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	totalEpochs := len(epochs)
	var epochsDone atomic.Int64
	start := time.Now()

	// Periodic progress ticker so long backfills (hundreds of epochs, each
	// with up to thousands of per-pool requests) still surface liveness in
	// the logs even if no single epoch has completed since the last tick.
	progressDone := make(chan struct{})
	var progressWg sync.WaitGroup
	progressWg.Go(func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				done := epochsDone.Load()
				elapsed := time.Since(start)
				var eta time.Duration
				if done > 0 {
					eta = time.Duration(int64(elapsed) / done * int64(totalEpochs-int(done)))
				}
				logger.Info("koiosparity: fetch progress",
					"network", cfg.Network,
					"epochs_done", done,
					"epochs_total", totalEpochs,
					"percent", fmt.Sprintf("%.1f", float64(done)/float64(totalEpochs)*100),
					"elapsed", elapsed.Round(time.Second),
					"eta", eta.Round(time.Second),
				)
			case <-progressDone:
				return
			}
		}
	})

loop:
	for _, epoch := range epochs {
		select {
		case <-fetchCtx.Done():
			break loop
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(epoch uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			cnt, fetchErr := fetchEpoch(fetchCtx, koios, cache, cfg.Network, epoch, poolIDs, logger)
			if fetchErr != nil {
				select {
				case errCh <- fmt.Errorf("epoch %d: %w", epoch, fetchErr):
				default:
				}
				// Stop dispatching/running further epochs immediately rather
				// than grinding through the rest of a potentially hours-long
				// batch once we already know this run will fail. Epochs that
				// don't finish committing just stay uncached and are retried
				// (along with any not-yet-attempted epochs) on the next run.
				cancel()
				return
			}
			mu.Lock()
			result.EpochsFetched++
			result.PoolsFetched += cnt
			mu.Unlock()
			done := epochsDone.Add(1)
			logger.Info("koiosparity: epoch fetched",
				"network", cfg.Network,
				"epoch", epoch,
				"pools", cnt,
				"epochs_done", done,
				"epochs_total", totalEpochs,
			)
		}(epoch)
	}

	wg.Wait()
	close(progressDone)
	progressWg.Wait()

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

	// Validate all rejection conditions before any DB writes so an incomplete
	// or pre-staking epoch response never partially modifies the cache.

	// Epochs 0 and 1 predate a valid "go" stake snapshot and can never have a
	// comparable active_stake — not a heuristic, but exact Shelley ledger
	// mechanics (formal-ledger-specifications, Ledger.Conway.Specification.Rewards):
	// a stake snapshot taken as "mark" at the boundary into epoch N+1 becomes
	// "set" at the boundary into epoch N+2, then "go" (the one used for reward /
	// active-stake calculation) at the boundary into epoch N+3. So the go
	// snapshot used for epoch E's active_stake was captured at the boundary
	// into epoch E-2, which only exists once E >= 2 (both preview and preprod
	// start their own epoch numbering at 0, so this is network-independent).
	// Koios returns active_stake=null here permanently — commit a PreStaking
	// marker so GetUncachedEpochs stops proposing these two epochs on every
	// future fetch run.
	//
	// A null active_stake on any OTHER epoch is not this condition — it means
	// Koios's backend hasn't finished processing a just-closed epoch yet (see
	// the end_time==0 check below) or something is genuinely wrong upstream.
	// Treating that as permanent pre-staking would silently and permanently
	// stop comparing a real epoch, so it is rejected as a retryable error
	// instead of being cached.
	const preStakingThroughEpoch = 1
	if info.ActiveStake == nil && epoch <= preStakingThroughEpoch {
		var epochEndTime time.Time
		if info.EndTime != 0 {
			epochEndTime = time.Unix(info.EndTime, 0).UTC()
		}
		if err := cache.CommitEpochData(KoiosEpochInfo{
			Network:      network,
			Epoch:        epoch,
			PreStaking:   true,
			EpochEndTime: epochEndTime,
			FetchedAt:    time.Now(),
		}, nil); err != nil {
			return 0, fmt.Errorf("commit pre-staking marker: %w", err)
		}
		logger.Info("koiosparity: epoch predates staking, marking permanently unfetchable",
			"network", network,
			"epoch", epoch,
		)
		return 0, nil
	}
	if info.ActiveStake == nil {
		return 0, fmt.Errorf(
			"epoch %d: koios returned null active_stake unexpectedly (only epochs <= %d predate a valid stake snapshot)",
			epoch, preStakingThroughEpoch,
		)
	}
	activeStake := *info.ActiveStake

	// end_time 0 means the epoch is not yet fully closed in Koios. Reject now
	// rather than after pool rows have been written to the cache.
	if info.EndTime == 0 {
		return 0, fmt.Errorf("epoch %d: koios returned end_time=0 — epoch may not be fully closed yet", epoch)
	}
	epochEndTime := time.Unix(info.EndTime, 0).UTC()

	// fees and total_rewards may also be null for early epochs; store as ""
	// so the cache constraint is satisfied. The comparer skips fees comparison
	// when koios.Fees is "".
	var fees, totalRewards string
	if info.Fees != nil {
		fees = *info.Fees
	}
	if info.TotalRewards != nil {
		totalRewards = *info.TotalRewards
	}

	now := time.Now()

	// 2. Fetch per-pool epoch history rows in parallel.
	// Pool rows are accumulated in memory and written atomically at the end so
	// that a force-refresh or partial failure cannot leave a mixed old+new set
	// in the cache for this epoch.
	var poolRows []KoiosPoolEpoch
	poolSem := make(chan struct{}, 5)
	var poolWg sync.WaitGroup
	var poolMu sync.Mutex
	poolErrCh := make(chan error, 1)
	var poolsDone atomic.Int64
	poolTotal := len(poolIDs)

	// A single epoch can require a request per pool (up to ~1200 on preview),
	// so surface progress mid-epoch rather than only after it fully commits.
	poolProgressDone := make(chan struct{})
	var poolProgressWg sync.WaitGroup
	poolProgressWg.Go(func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				logger.Info("koiosparity: epoch pool fetch progress",
					"network", network,
					"epoch", epoch,
					"pools_done", poolsDone.Load(),
					"pools_total", poolTotal,
				)
			case <-poolProgressDone:
				return
			}
		}
	})

	// Use a labeled break so that on cancellation we stop spawning new workers
	// but still drain already-started goroutines via poolWg.Wait() below.
	// An early return here would let running goroutines race poolRows after
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
			defer poolsDone.Add(1)

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

			var margin, memberRewards string
			if item.Margin != nil {
				// Format without trailing zeros so "0.100" and "0.1" compare
				// equally after Rat normalisation in ComparePoolEpoch.
				margin = strconv.FormatFloat(*item.Margin, 'g', -1, 64)
			}
			if item.MemberRewards != nil {
				memberRewards = *item.MemberRewards
			}

			poolMu.Lock()
			poolRows = append(poolRows, KoiosPoolEpoch{
				Network:       network,
				Epoch:         epoch,
				PoolBech32:    id,
				ActiveStake:   item.ActiveStake,
				BlockCnt:      item.BlockCnt,
				Delegators:    item.DelegatorCnt,
				Margin:        margin,
				FixedCost:     item.FixedCost,
				PoolFees:      item.PoolFees,
				DelegRewards:  item.DelegRewards,
				MemberRewards: memberRewards,
				FetchedAt:     now,
			})
			poolMu.Unlock()
		}(poolID)
	}

	poolWg.Wait() // always drain started goroutines before returning
	close(poolProgressDone)
	poolProgressWg.Wait()

	// If context was cancelled, report that rather than any pool error.
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	select {
	case err := <-poolErrCh:
		return 0, err
	default:
	}

	// 3. Commit pool rows and epoch info atomically.
	//
	// CommitEpochData deletes the old pool set, batch-inserts the new one,
	// and upserts epoch info — all in one transaction. This ensures the
	// freshness marker (fetched_at) is never left stale relative to the
	// pool rows, which would suppress the automatic recheck.
	if err := cache.CommitEpochData(KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  activeStake,
		Fees:         fees,
		TotalRewards: totalRewards,
		EpochEndTime: epochEndTime,
		FetchedAt:    now,
	}, poolRows); err != nil {
		return 0, fmt.Errorf("commit epoch: %w", err)
	}

	return len(poolRows), nil
}
