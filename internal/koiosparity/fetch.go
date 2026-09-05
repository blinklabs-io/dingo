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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
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
	// AccountsEnabled additionally fetches #3097's per-account Koios
	// reference data (FetchAccountRewardsForEpoch) for every epoch this run
	// fetches. False by default: per-account fetching issues far more Koios
	// requests than pool-level fetching (one chunked request set per epoch
	// covering the full address universe, versus one request per pool), so
	// this is opt-in for the standalone CLI. See ObserverConfig.AccountsEnabled
	// for the in-process observer's default.
	AccountsEnabled bool
	// AccountsSource supplies Dingo's own known reward-account addresses to
	// union with Koios's full account list (see BuildAccountAddressUniverse)
	// when AccountsEnabled is true. nil is valid (e.g. the standalone CLI's
	// `fetch` command run without Dingo database access configured for
	// accounts) — the address universe then falls back to Koios's list
	// alone, which still checks every Koios-known account, just cannot also
	// surface a Dingo-only account Koios has never indexed.
	AccountsSource RewardParitySource
	// GraceHours is forwarded to FetchAccountRewardsForEpoch's zero-row/lag
	// gate (see its doc comment): a just-closed epoch (within this many
	// hours of EpochEndTime) whose #3097 account fetch returns zero rows
	// across the whole address universe is left with koios_account_coverage
	// incomplete rather than permanently accepted as "zero accounts earned
	// rewards", since Koios's own /account_reward_history publishing lag is
	// a far more likely explanation. 0 disables the gate (every zero-row
	// result is accepted as final immediately). Unused when AccountsEnabled
	// is false.
	GraceHours int
	// AccountChunkSize/AccountChunkMaxBytes (dingo #3099) bound each
	// /account_reward_history request by both address count and encoded
	// body size (see chunkAddressesByCountAndSize). <=0 means "use the
	// package default" (koiosAccountChunkSize/koiosAccountChunkMaxBytesDefault).
	// Unused when AccountsEnabled is false.
	AccountChunkSize     int
	AccountChunkMaxBytes int
}

// FetchResult summarises a completed fetch run.
type FetchResult struct {
	EpochsFetched int
	PoolsFetched  int
	FromEpoch     uint64
	ThroughEpoch  uint64
	// FailedEpochs lists epochs that hit a transient, isolated fetch failure
	// (see transientEpochFetchErr) and were skipped rather than aborting the
	// rest of the run. They remain uncached and are retried by a future
	// fetch run via GetUncachedEpochs.
	FailedEpochs []uint64
}

// transientEpochFetchErr marks a per-epoch HTTP-level fetch failure (epoch
// info, totals, or a single pool's history request exhausting its retries)
// as isolated to that one epoch rather than systematic. Unlike a validation
// error (e.g. an unexpected null active_stake, which recurs on every
// subsequent epoch and is worth stopping the whole batch for immediately),
// a transient failure — most often a brief Koios backend blip on one request
// among many thousands — has no reason to affect any other epoch, so the
// dispatcher in Fetch skips just this epoch instead of cancelling every
// other in-flight epoch in the batch.
type transientEpochFetchErr struct {
	err error
}

func (e *transientEpochFetchErr) Error() string { return e.err.Error() }
func (e *transientEpochFetchErr) Unwrap() error { return e.err }

func transientErr(err error) error {
	return &transientEpochFetchErr{err: err}
}

// classifyFetchErr wraps a Koios client error as either permanent (returned
// as-is, which the caller then propagates as a hard Fetch error that aborts
// the whole run) or transient (wrapped in transientEpochFetchErr, isolated to
// this one epoch and retried on a future Fetch run), based on whether err
// wraps the ErrKoiosPermanent sentinel — daily-quota exhaustion, auth
// failures, and other hard 4xx the client already identifies as non-retryable.
func classifyFetchErr(err error) error {
	if errors.Is(err, ErrKoiosPermanent) {
		return err
	}
	return transientErr(err)
}

// Fetch pulls Koios data into the cache, resuming from the last cached epoch.
func Fetch(
	ctx context.Context,
	cfg FetchConfig,
	logger *slog.Logger,
) (*FetchResult, error) {
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
		return nil, errors.New(
			"koios tip epoch is 0: no closed epochs to fetch",
		)
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
		return &FetchResult{
			FromEpoch:    fromEpoch,
			ThroughEpoch: throughEpoch,
		}, nil
	}

	// Collect every pool that has ever been registered on chain, including
	// retired pools. /pool_list is the authoritative source; we hoist this once
	// per Fetch run rather than once per epoch because the list grows
	// monotonically and fetching it once is far cheaper on wide backfills.
	poolIDs, err := koios.GetAllHistoricalPoolIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("get historical pool IDs: %w", err)
	}

	// Each pool's true first-active epoch, used to skip /pool_history calls
	// for epochs before a pool could possibly have any data — see
	// GetPoolFirstActiveEpochs for why this must come from /pool_updates and
	// not /pool_list's active_epoch_no. Also hoisted once per Fetch run.
	firstActiveEpochs, err := koios.GetPoolFirstActiveEpochs(ctx)
	if err != nil {
		return nil, fmt.Errorf("get pool first-active epochs: %w", err)
	}

	// Koios's full historical account list (#3097) — hoisted once per Fetch
	// run for the same reason poolIDs is, when account fetching is enabled.
	var koiosAccountAddrs []string
	if cfg.AccountsEnabled {
		koiosAccountAddrs, err = ResolveKoiosAccountUniverse(ctx, koios)
		if err != nil {
			return nil, fmt.Errorf("get all koios account addresses: %w", err)
		}
	}

	// Build list of epochs to fetch.
	// Normal mode: epochs NOT already in the cache (fills holes from prior
	// failed/interrupted runs rather than naively resuming from max+1),
	// UNIONED — when cfg.AccountsEnabled — with epochs that already have
	// fresh pool-level Koios data but are still missing #3097's per-account
	// coverage (accountOnlyEpochs below). Without this union, an epoch fetched
	// before per-account fetching existed (or before AccountsEnabled was
	// turned on) would look "already fetched" to GetUncachedEpochs forever and
	// never get a per-account backfill — see GetEpochsMissingAccountCoverage's
	// doc comment and ARCHITECTURE.md's Koios Parity Tracker "Per-account
	// exact parity" subsection.
	// ForceRefresh mode: fetch the full range and overwrite cached rows, used
	// when the user suspects stale or corrupt cached data in [fromEpoch, through].
	var epochs []uint64
	// accountOnlyEpochs marks epochs that only need the #3097 account-level
	// backfill below — their pool-level Koios data is already fresh, so the
	// per-epoch worker skips the redundant pool/epoch_info/totals fetchEpoch
	// call for these and fetches only account rewards.
	accountOnlyEpochs := make(map[uint64]bool)
	if cfg.ForceRefresh {
		for e := fromEpoch; e <= throughEpoch; e++ {
			epochs = append(epochs, e)
		}
	} else {
		epochs, err = cache.GetUncachedEpochs(cfg.Network, fromEpoch, throughEpoch)
		if err != nil {
			return nil, fmt.Errorf("get uncached epochs: %w", err)
		}
		if cfg.AccountsEnabled {
			have := make(map[uint64]bool, len(epochs))
			for _, e := range epochs {
				have[e] = true
			}
			missingAccounts, err := cache.GetEpochsMissingAccountCoverage(
				cfg.Network,
				fromEpoch,
				throughEpoch,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"get epochs missing account coverage: %w",
					err,
				)
			}
			for _, e := range missingAccounts {
				if !have[e] {
					accountOnlyEpochs[e] = true
					epochs = append(epochs, e)
				}
			}
			slices.Sort(epochs)
		}
	}

	if len(epochs) == 0 {
		logger.Info("koiosparity: fetch cache is up-to-date",
			"network", cfg.Network,
			"last_epoch", throughEpoch,
		)
		return &FetchResult{
			FromEpoch:    fromEpoch,
			ThroughEpoch: throughEpoch,
		}, nil
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
					eta = time.Duration(
						int64(elapsed) / done * int64(totalEpochs-int(done)),
					)
				}
				logger.Info(
					"koiosparity: fetch progress",
					"network",
					cfg.Network,
					"epochs_done",
					done,
					"epochs_total",
					totalEpochs,
					"percent",
					fmt.Sprintf("%.1f", float64(done)/float64(totalEpochs)*100),
					"elapsed",
					elapsed.Round(time.Second),
					"eta",
					eta.Round(time.Second),
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

			// handleEpochFetchErr applies the shared "transient isolates to
			// this epoch, anything else aborts the whole run" classification
			// to both the pool-level fetch below and the optional #3097
			// account-level fetch that follows it, so a permanent/other
			// error from either phase cancels the run the same way and a
			// transient error from either phase lands this epoch in
			// FailedEpochs the same way — the two phases must not diverge in
			// how they report failure.
			handleEpochFetchErr := func(phase string, fetchErr error) {
				if transient, ok := errors.AsType[*transientEpochFetchErr](fetchErr); ok {
					// Isolated to this epoch (e.g. one flaky Koios 5xx among
					// thousands of requests this run makes) — skip it rather
					// than cancelling every other in-flight epoch. It stays
					// uncached and is retried by a future fetch run.
					mu.Lock()
					result.FailedEpochs = append(result.FailedEpochs, epoch)
					mu.Unlock()
					logger.Warn("koiosparity: epoch fetch failed transiently, skipping (will retry on next run)",
						"network", cfg.Network,
						"epoch", epoch,
						"phase", phase,
						"error", transient,
					)
					epochsDone.Add(1)
					return
				}
				select {
				case errCh <- fmt.Errorf("epoch %d (%s): %w", epoch, phase, fetchErr):
				default:
				}
				// Stop dispatching/running further epochs immediately rather
				// than grinding through the rest of a potentially hours-long
				// batch once we already know this run will fail. Epochs that
				// don't finish committing just stay uncached and are retried
				// (along with any not-yet-attempted epochs) on the next run.
				cancel()
			}

			var cnt int
			if accountOnlyEpochs[epoch] {
				// Pool-level Koios data for this epoch is already fresh —
				// only #3097's per-account coverage is missing/incomplete —
				// so skip the redundant pool/epoch_info/totals fetchEpoch
				// call entirely and go straight to the account backfill
				// below, rather than re-fetching thousands of already-fresh
				// pool-history rows just to reach it.
				logger.Debug(
					"koiosparity: epoch pool data already fresh, backfilling accounts only",
					"network", cfg.Network,
					"epoch", epoch,
				)
			} else {
				var fetchErr error
				cnt, fetchErr = fetchEpoch(fetchCtx, koios, cache, cfg.Network, epoch, poolIDs, firstActiveEpochs, logger)
				if fetchErr != nil {
					handleEpochFetchErr("pools", fetchErr)
					return
				}
			}

			if cfg.AccountsEnabled {
				if _, acctErr := FetchEpochAccountsWithAddrs(
					fetchCtx, koios, cache, cfg.Network, epoch,
					cfg.AccountsSource, koiosAccountAddrs, cfg.GraceHours,
					cfg.AccountChunkSize, cfg.AccountChunkMaxBytes,
					cfg.ForceRefresh, logger,
				); acctErr != nil {
					handleEpochFetchErr("accounts", acctErr)
					return
				}
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

	if len(result.FailedEpochs) > 0 {
		slices.Sort(result.FailedEpochs)
		logger.Warn("koiosparity: fetch complete with transient failures",
			"network", cfg.Network,
			"epochs", result.EpochsFetched,
			"pools", result.PoolsFetched,
			"failed_epochs", result.FailedEpochs,
		)
		return result, nil
	}

	logger.Info("koiosparity: fetch complete",
		"network", cfg.Network,
		"epochs", result.EpochsFetched,
		"pools", result.PoolsFetched,
	)
	return result, nil
}

// FetchEpochWithClient fetches and caches Koios reference data for exactly
// one epoch using an already-open cache and Koios client, without reopening
// the cache database or reconstructing a Koios client — the primitive Fetch's
// per-epoch worker pool uses internally (fetchEpoch), exported so a
// long-lived caller (the dingo #3098 in-process epoch observer, which fetches
// one newly closed epoch at a time as epoch.transition events arrive rather
// than batching a whole historical range) can reuse one cache handle and one
// Koios client across many calls.
//
// Every request Koios needs beyond the target epoch's own history — the
// full historical pool ID list and each pool's first-active epoch — is
// re-resolved on every call. A caller that will fetch the same epoch more
// than once in quick succession (e.g. a bounded retry loop riding out a
// transient failure) should resolve these once with
// GetAllHistoricalPoolIDs/GetPoolFirstActiveEpochs and call
// FetchEpochWithPools instead, to avoid repeating both full Koios scans on
// every attempt. Efficient reuse/pagination across many distinct epochs is
// #3099's chunked/resumable fetch scope, not this function's.
func FetchEpochWithClient(
	ctx context.Context,
	koios *KoiosClient,
	cache *Cache,
	network string,
	epoch uint64,
	logger *slog.Logger,
) (int, error) {
	poolIDs, firstActiveEpochs, err := resolvePoolUniverse(ctx, koios)
	if err != nil {
		return 0, err
	}
	return fetchEpoch(
		ctx,
		koios,
		cache,
		network,
		epoch,
		poolIDs,
		firstActiveEpochs,
		logger,
	)
}

// FetchEpochWithPools is FetchEpochWithClient with the pool universe
// (poolIDs/firstActiveEpochs) already resolved by the caller — see
// resolvePoolUniverse. Intended for a caller that fetches the same epoch
// across multiple attempts (a retry loop) or fetches several epochs in one
// batch: resolving the pool universe once, up front, and passing it to every
// call avoids repeating /pool_list and /pool_updates's full scans on every
// attempt/epoch, which matters for rate-limit budget on wide backfills or
// long retry sequences near Koios's daily quota.
func FetchEpochWithPools(
	ctx context.Context,
	koios *KoiosClient,
	cache *Cache,
	network string,
	epoch uint64,
	poolIDs []string,
	firstActiveEpochs map[string]uint64,
	logger *slog.Logger,
) (int, error) {
	return fetchEpoch(
		ctx,
		koios,
		cache,
		network,
		epoch,
		poolIDs,
		firstActiveEpochs,
		logger,
	)
}

// resolvePoolUniverse fetches the full historical pool ID list and each
// pool's true first-active epoch — the two requests every fetchEpoch call
// needs beyond the target epoch's own history. Factored out so callers that
// fetch more than one epoch (or retry the same epoch) can resolve it once
// and reuse it via FetchEpochWithPools, instead of paying for both full
// Koios scans again on every call the way FetchEpochWithClient does.
func resolvePoolUniverse(
	ctx context.Context,
	koios *KoiosClient,
) (poolIDs []string, firstActiveEpochs map[string]uint64, err error) {
	poolIDs, err = koios.GetAllHistoricalPoolIDs(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get historical pool IDs: %w", err)
	}
	firstActiveEpochs, err = koios.GetPoolFirstActiveEpochs(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get pool first-active epochs: %w", err)
	}
	return poolIDs, firstActiveEpochs, nil
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
	firstActiveEpochs map[string]uint64,
	logger *slog.Logger,
) (int, error) {
	// 1. Fetch epoch info (not written yet).
	info, err := koios.GetEpochInfo(ctx, epoch)
	if err != nil {
		return 0, classifyFetchErr(fmt.Errorf("get epoch info: %w", err))
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
	if info.ActiveStake == nil && epoch <= preStakingThroughEpoch {
		if err := cache.CommitEpochData(KoiosEpochInfo{
			Network:        network,
			Epoch:          epoch,
			PreStaking:     true,
			EpochEndTime:   unixTime(info.EndTime),
			Era:            info.Era,
			OutSum:         strOrEmpty(info.OutSum),
			TxCount:        info.TxCount,
			BlkCount:       info.BlkCount,
			EpochStartTime: unixTime(info.StartTime),
			FirstBlockTime: unixTime(info.FirstBlockTime),
			LastBlockTime:  unixTime(info.LastBlockTime),
			AvgBlkReward:   strOrEmpty(info.AvgBlkReward),
			FetchedAt:      time.Now(),
		}, nil, nil); err != nil {
			return 0, fmt.Errorf("commit pre-staking marker: %w", err)
		}
		logger.Info(
			"koiosparity: epoch predates staking, marking permanently unfetchable",
			"network",
			network,
			"epoch",
			epoch,
		)
		return 0, nil
	}
	if info.ActiveStake == nil {
		return 0, fmt.Errorf(
			"epoch %d: koios returned null active_stake unexpectedly (only epochs <= %d predate a valid stake snapshot)",
			epoch,
			preStakingThroughEpoch,
		)
	}
	activeStake := *info.ActiveStake

	// end_time 0 means the epoch is not yet fully closed in Koios. Reject now
	// rather than after pool rows have been written to the cache.
	if info.EndTime == 0 {
		return 0, fmt.Errorf(
			"epoch %d: koios returned end_time=0 — epoch may not be fully closed yet",
			epoch,
		)
	}
	epochEndTime := unixTime(info.EndTime)

	// fees, total_rewards, out_sum, and avg_blk_reward may also be null for
	// early epochs; store as "" so the cache constraint is satisfied. The
	// comparer skips fees/total_rewards comparison when the Koios value is "".
	var fees, totalRewards string
	if info.Fees != nil {
		fees = *info.Fees
	}
	if info.TotalRewards != nil {
		totalRewards = *info.TotalRewards
	}
	outSum := strOrEmpty(info.OutSum)
	avgBlkReward := strOrEmpty(info.AvgBlkReward)

	// 1b. Fetch /totals — a single extra request, so fetched sequentially
	// rather than added to the per-pool worker pool below. Any failure aborts
	// the epoch the same way an epoch_info failure does, rather than caching
	// epoch_info/pool rows with a permanently missing totals row.
	totalsResp, err := koios.GetTotals(ctx, epoch)
	if err != nil {
		return 0, classifyFetchErr(fmt.Errorf("get totals: %w", err))
	}

	// 1c. Fetch /epoch_params — the per-epoch protocol parameters
	// (dingo #3931). Another single sequential request, treated exactly like
	// /totals above: any failure aborts the epoch rather than caching
	// epoch_info and pool rows with a permanently missing parameter row.
	paramsResp, err := koios.GetEpochParams(ctx, epoch)
	if err != nil {
		return 0, classifyFetchErr(fmt.Errorf("get epoch params: %w", err))
	}

	now := time.Now()

	// The parameter row is committed BEFORE CommitEpochData, not inside it.
	// CommitEpochData advances koios_epoch_info.fetched_at, which is the
	// freshness marker GetEpochsNeedingCheck compares against; writing the
	// parameters first means a process killed between the two writes leaves
	// the epoch looking unfetched and it is simply re-fetched. Committing
	// them after would advance fetched_at with no parameter row behind it.
	if err := cache.UpsertEpochParams(
		epochParamsFromKoios(network, epoch, paramsResp, now),
	); err != nil {
		return 0, fmt.Errorf("commit epoch params: %w", err)
	}

	// 1d. Skip pools that could not possibly have any history yet this
	// epoch — cuts wasted requests substantially on early epochs, when most
	// of the network's ever-registered pools (firstActiveEpochs is hoisted
	// once for the whole historical pool set) don't exist yet.
	activePoolIDs := poolIDs
	if len(firstActiveEpochs) > 0 {
		activePoolIDs = make([]string, 0, len(poolIDs))
		for _, id := range poolIDs {
			if first, ok := firstActiveEpochs[id]; ok && epoch < first {
				continue
			}
			activePoolIDs = append(activePoolIDs, id)
		}
	}

	// 2. Fetch per-pool epoch history rows in parallel.
	// Pool rows are accumulated in memory and written atomically at the end so
	// that a force-refresh or partial failure cannot leave a mixed old+new set
	// in the cache for this epoch.
	var poolRows []KoiosPoolEpoch
	poolSem := make(chan struct{}, 5)
	var poolWg sync.WaitGroup
	var poolMu sync.Mutex
	// poolErrMu guards poolErr. A mutex (rather than a buffered channel) is
	// used so a permanent error can never be lost to a same-instant transient
	// one: whichever goroutine takes the lock next always applies the "a
	// permanent error always wins" rule below instead of racing for a single
	// buffer slot.
	var poolErrMu sync.Mutex
	var poolErr error
	var poolsDone atomic.Int64
	poolTotal := len(activePoolIDs)

	// poolCtx is cancelled the moment any pool request hits a permanent
	// (non-retryable) error, so a quota/auth failure stops the loop below from
	// scheduling every remaining pool as a doomed request. Deriving from ctx
	// means an outer Fetch-level cancellation (a permanent error in another
	// epoch) also stops this loop, while the ctx.Err() check after
	// poolWg.Wait() below still inspects the original ctx so a locally
	// triggered poolCancel is never confused with real caller cancellation.
	poolCtx, poolCancel := context.WithCancel(ctx)
	defer poolCancel()

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
	for _, poolID := range activePoolIDs {
		select {
		case <-poolCtx.Done():
			break outer
		case poolSem <- struct{}{}:
		}

		poolWg.Add(1)
		go func(id string) {
			defer poolWg.Done()
			defer func() { <-poolSem }()
			defer poolsDone.Add(1)

			item, histErr := koios.GetPoolEpochHistory(poolCtx, id, epoch)
			if histErr != nil {
				wrapped := fmt.Errorf("pool %s history: %w", id, histErr)
				isPermanent := errors.Is(histErr, ErrKoiosPermanent)
				poolErrMu.Lock()
				// A permanent error always wins so a concurrent transient
				// error can never mask the reason scheduling stopped.
				if poolErr == nil || (isPermanent && !errors.Is(poolErr, ErrKoiosPermanent)) {
					poolErr = wrapped
				}
				poolErrMu.Unlock()
				if isPermanent {
					// Stop scheduling further pools for this epoch — retrying
					// or continuing through the rest of the pool set cannot
					// succeed once auth/quota has failed.
					poolCancel()
				}
				return
			}
			if item == nil {
				return // Pool wasn't active this epoch.
			}

			var margin, activeStakePct string
			if item.Margin != nil {
				// Format without trailing zeros so "0.100" and "0.1" compare
				// equally after Rat normalisation in ComparePoolEpoch.
				margin = strconv.FormatFloat(*item.Margin, 'g', -1, 64)
			}
			if item.ActiveStakePct != nil {
				activeStakePct = strconv.FormatFloat(*item.ActiveStakePct, 'g', -1, 64)
			}
			memberRewards := strOrEmpty(item.MemberRewards)

			poolMu.Lock()
			poolRows = append(poolRows, KoiosPoolEpoch{
				Network:        network,
				Epoch:          epoch,
				PoolBech32:     id,
				ActiveStake:    item.ActiveStake,
				BlockCnt:       item.BlockCnt,
				Delegators:     item.DelegatorCnt,
				Margin:         margin,
				FixedCost:      item.FixedCost,
				PoolFees:       item.PoolFees,
				DelegRewards:   item.DelegRewards,
				MemberRewards:  memberRewards,
				ActiveStakePct: activeStakePct,
				SaturationPct:  strconv.FormatFloat(item.SaturationPct, 'g', -1, 64),
				EpochRos:       strconv.FormatFloat(item.EpochRos, 'g', -1, 64),
				FetchedAt:      now,
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

	poolErrMu.Lock()
	err = poolErr
	poolErrMu.Unlock()
	if err != nil {
		return 0, classifyFetchErr(err)
	}

	// 3. Commit pool rows and epoch info atomically.
	//
	// CommitEpochData deletes the old pool set, batch-inserts the new one,
	// and upserts epoch info — all in one transaction. This ensures the
	// freshness marker (fetched_at) is never left stale relative to the
	// pool rows, which would suppress the automatic recheck.
	if err := cache.CommitEpochData(KoiosEpochInfo{
		Network:        network,
		Epoch:          epoch,
		ActiveStake:    activeStake,
		Fees:           fees,
		TotalRewards:   totalRewards,
		EpochEndTime:   epochEndTime,
		Era:            info.Era,
		OutSum:         outSum,
		TxCount:        info.TxCount,
		BlkCount:       info.BlkCount,
		EpochStartTime: unixTime(info.StartTime),
		FirstBlockTime: unixTime(info.FirstBlockTime),
		LastBlockTime:  unixTime(info.LastBlockTime),
		AvgBlkReward:   avgBlkReward,
		FetchedAt:      now,
	}, poolRows, &KoiosTotals{
		Treasury:           totalsResp.Treasury,
		Reserves:           totalsResp.Reserves,
		Fees:               totalsResp.Fees,
		Reward:             totalsResp.Reward,
		Circulation:        totalsResp.Circulation,
		Supply:             totalsResp.Supply,
		DepositsStake:      totalsResp.DepositsStake,
		DepositsDRep:       totalsResp.DepositsDRep,
		DepositsProposal:   totalsResp.DepositsProposal,
		TreasuryDonation:   totalsResp.TreasuryDonation,
		TreasuryWithdrawal: totalsResp.TreasuryWithdrawal,
		ReservesWithdrawal: totalsResp.ReservesWithdrawal,
		FetchedAt:          now,
	}); err != nil {
		return 0, fmt.Errorf("commit epoch: %w", err)
	}

	return len(poolRows), nil
}

// unixTime converts a Koios Unix-seconds timestamp to a UTC time.Time, leaving
// it as the zero value when Koios reports 0 (unset/unknown) rather than
// producing the 1970-01-01 epoch, which would be misleading for "unknown".
func unixTime(sec int64) time.Time {
	if sec == 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0).UTC()
}

// epochParamsFromKoios flattens a /epoch_params response into the cache row.
//
// Every value is carried across as the literal text Koios published — a
// json.Number keeps its own digits, so "7.21e-05" is stored exactly as sent
// with no float round-trip — and a null becomes "", which
// CompareEpochProtocolParams reads as "this era does not define this
// parameter" rather than as zero.
func epochParamsFromKoios(
	network string,
	epoch uint64,
	resp *KoiosEpochParamsResp,
	now time.Time,
) KoiosEpochParams {
	if resp == nil {
		return KoiosEpochParams{Network: network, Epoch: epoch, FetchedAt: now}
	}
	return KoiosEpochParams{
		Network:              network,
		Epoch:                epoch,
		Era:                  resp.Era,
		MinFeeA:              numOrEmpty(resp.MinFeeA),
		MinFeeB:              numOrEmpty(resp.MinFeeB),
		MaxBlockBodySize:     numOrEmpty(resp.MaxBlockSize),
		MaxTxSize:            numOrEmpty(resp.MaxTxSize),
		MaxBlockHeaderSize:   numOrEmpty(resp.MaxBhSize),
		KeyDeposit:           strOrEmpty(resp.KeyDeposit),
		PoolDeposit:          strOrEmpty(resp.PoolDeposit),
		MaxEpoch:             numOrEmpty(resp.MaxEpoch),
		NOpt:                 numOrEmpty(resp.OptimalPoolCount),
		A0:                   numOrEmpty(resp.Influence),
		Rho:                  numOrEmpty(resp.MonetaryExpandRate),
		Tau:                  numOrEmpty(resp.TreasuryGrowthRate),
		ProtocolMajor:        numOrEmpty(resp.ProtocolMajor),
		ProtocolMinor:        numOrEmpty(resp.ProtocolMinor),
		MinPoolCost:          strOrEmpty(resp.MinPoolCost),
		PriceMem:             numOrEmpty(resp.PriceMem),
		PriceStep:            numOrEmpty(resp.PriceStep),
		MaxTxExMem:           numOrEmpty(resp.MaxTxExMem),
		MaxTxExSteps:         numOrEmpty(resp.MaxTxExSteps),
		MaxBlockExMem:        numOrEmpty(resp.MaxBlockExMem),
		MaxBlockExSteps:      numOrEmpty(resp.MaxBlockExSteps),
		MaxValueSize:         numOrEmpty(resp.MaxValSize),
		CollateralPercentage: numOrEmpty(resp.CollateralPercent),
		MaxCollateralInputs:  numOrEmpty(resp.MaxCollateralInputs),
		Decentralisation:     numOrEmpty(resp.Decentralisation),
		MinUtxoValue:         strOrEmpty(resp.MinUtxoValue),
		CoinsPerUtxoSize:     strOrEmpty(resp.CoinsPerUtxoSize),
		FetchedAt:            now,
	}
}

// numOrEmpty dereferences a nullable Koios JSON number, preserving its
// literal text (json.Number) so no decimal or exponent value is ever routed
// through a float64. Returns "" when Koios published null.
func numOrEmpty(n *json.Number) string {
	if n == nil {
		return ""
	}
	return n.String()
}

// strOrEmpty dereferences a nullable Koios string field, returning "" when nil.
func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
