// Copyright 2026 Blink Labs Software
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
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// accountFetchConcurrency bounds how many /account_reward_history chunk
// requests run in parallel for a single epoch's account fetch — mirrors
// fetchEpoch's per-pool worker pool concurrency.
const accountFetchConcurrency = 5

// afterChunkCancelForTest is a test-only synchronization seam: when non-nil,
// FetchAccountRewardsForEpoch's chunk-error path invokes it synchronously
// immediately after calling cancel(). Production code never sets it, so this
// is always a nil check (no-op) outside of
// fetch_accounts_test.go's TestFetchAccountRewardsForEpochStopsDispatchingAfterFirstChunkError,
// which uses it to know deterministically that cancellation has actually
// happened instead of inferring it from a fixed sleep margin.
var afterChunkCancelForTest func()

// ResolveKoiosAccountUniverse returns the bech32 stake address of every
// account Koios has ever seen — the Koios side of #3097's address universe.
// Hoist this once per Fetch run (or once per Observer fetch cycle) and reuse
// it across every epoch via FetchAccountRewardsForEpoch's addressUniverse
// parameter, exactly the way resolvePoolUniverse's poolIDs/firstActiveEpochs
// are hoisted once and reused across epochs for pool history.
func ResolveKoiosAccountUniverse(
	ctx context.Context,
	koios *KoiosClient,
) ([]string, error) {
	addrs, err := koios.GetAllAccountAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get all koios account addresses: %w", err)
	}
	return addrs, nil
}

// BuildAccountAddressUniverse returns the union of koiosAddrs (Koios's full
// historical account list — see ResolveKoiosAccountUniverse) and Dingo's own
// committed reward-account addresses at stakeEpoch (the koiosStakeEpoch(K-1)
// derivation checkEpoch already uses for reward_account_output — see
// koiosStakeEpoch's doc comment and ARCHITECTURE.md's Epoch alignment
// section; the exact same offset applies here since reward_account_output
// and reward_pool_output share one Epoch stamp, both written from
// app.epochs.snapshot in ledger/reward_calculation.go's
// saveStakeRewardOutputs).
//
// The union — not just Koios's own list — is what lets a Dingo-only account
// (one Dingo computed a reward for that Koios's /account_list has not yet
// indexed, or never will) still get checked and surface as a real
// acct_only_dingo mismatch rather than being silently skipped; the reverse
// (a Koios-only-known account Dingo never recorded a reward for) is exactly
// why Koios's list is unioned in rather than trusting only Dingo's own
// addresses — this mirrors why per-pool comparison already unions Koios's
// and Dingo's pool sets (see checkEpoch's onlyKoios/onlyDingo bookkeeping)
// rather than trusting either side alone.
//
// source may be nil — used when the caller has no Dingo database access
// configured for accounts at all (e.g. the standalone CLI's `fetch` command
// invoked without --metadata-*); the resulting universe is then Koios's list
// alone, which still lets every Koios-known account be checked, just unable
// to also surface a Dingo-only account Koios has never indexed.
//
// A single malformed Dingo row (an unsupported credential tag —
// StakeAddressFromCredential failing) is skipped here rather than failing
// the whole universe build: checkEpoch's own per-account comparison pass
// re-resolves the same rows and reports a decode failure explicitly as a
// dingo_db_error mismatch, so the condition is never silently lost, just not
// allowed to abort fetch for every other, well-formed account.
func BuildAccountAddressUniverse(
	ctx context.Context,
	source RewardParitySource,
	stakeEpoch uint64,
	koiosAddrs []string,
) ([]string, error) {
	seen := make(map[string]bool, len(koiosAddrs))
	out := make([]string, 0, len(koiosAddrs))
	for _, a := range koiosAddrs {
		if a == "" || seen[a] {
			continue
		}
		seen[a] = true
		out = append(out, a)
	}
	if source == nil {
		return out, nil
	}
	dingoRows, err := source.GetRewardAccountOutputs(ctx, stakeEpoch)
	if err != nil {
		return nil, fmt.Errorf(
			"get dingo reward account outputs epoch %d: %w",
			stakeEpoch,
			err,
		)
	}
	for _, row := range dingoRows {
		addr, addrErr := StakeAddressFromCredential(
			row.StakingKey,
			row.CredentialTag,
		)
		if addrErr != nil {
			continue
		}
		if seen[addr] {
			continue
		}
		seen[addr] = true
		out = append(out, addr)
	}
	return out, nil
}

// FetchAccountRewardsForEpoch fetches Koios /account_reward_history
// reference data for every address in addressUniverse for Koios reporting
// epoch epoch, and commits it to the cache only once every chunk has
// succeeded (see Cache.CommitAccountRewardsForEpoch) — a partial failure
// never leaves the epoch's koios_account_coverage row marked complete, so
// checkEpoch's coverage gate can never mistake a partially-fetched epoch for
// a valid reference set.
//
// addressUniverse is split into koiosAccountChunkSize-sized groups and
// fetched with accountFetchConcurrency-bounded parallelism, mirroring
// fetchEpoch's per-pool worker pool. This is the minimal viable
// chunking/concurrency for #3097 — bounding a single request's blast radius
// and the outbound payload size, nothing more. Byte-size-aware request
// shaping, mid-fetch resumable checkpointing across process restarts,
// adaptive rate-limiting, and duplicate-page detection are #3099's scope;
// see this function's own scope-boundary note in ARCHITECTURE.md's Koios
// Parity Tracker "Per-account exact parity (#3097)" subsection. A process
// restart mid-fetch simply redoes the whole epoch's account fetch from
// scratch on the next attempt — safe (idempotent, same final state) even if
// not maximally efficient, exactly like FetchEpochWithClient's identical
// note about the pool-history fetch.
//
// On a permanent Koios error (quota/auth) mid-fetch, remaining chunks are
// cancelled and the permanent error is returned unwrapped so the caller
// aborts rather than continuing to schedule doomed requests — mirrors
// fetchEpoch's identical pool-history handling. An isolated transient chunk
// failure is NOT individually retried inside this function (unlike
// fetchEpoch's per-pool loop, which simply skips a pool with no history that
// epoch): any transient failure aborts this call's whole fetch (nothing is
// committed), because — unlike a single pool's history query — a missing
// chunk here means a whole slice of the requested address universe has no
// answer at all, which can never be treated as "this epoch's reference data
// is complete". The caller (Fetch's per-epoch worker, or the Observer's
// fetchIfNeeded retry loop) is expected to retry the whole epoch, the same
// way it already retries a fetchEpoch pool-history failure.
//
// epochEndTime/graceHours gate a distinct hazard: every chunk can succeed yet
// still return zero reward rows across the entire address universe, because
// Koios has simply not finished publishing /account_reward_history for a
// just-closed epoch yet (the same publishing lag CompareEpochAggregates/
// ComparePoolEpoch already treat as reference_lag, not a real discrepancy —
// see compare.go). Committing that empty result as complete=true would be
// permanent: GetEpochsMissingAccountCoverage never re-selects an epoch whose
// coverage row already reports complete=1, so a real, later-published
// non-empty answer would never be fetched. When len(rows) == 0 and epoch
// closed less than graceHours ago (epochEndTime non-zero), the coverage row
// is committed with complete=false instead — recording the attempt without
// ever letting a lag-induced empty snapshot look like an authoritative "zero
// accounts earned rewards this epoch" — so a later Fetch/Observer attempt
// retries it. Once the grace window elapses, a persistently empty result is
// accepted as final (complete=true) so the epoch is not retried forever.
func FetchAccountRewardsForEpoch(
	ctx context.Context,
	koios *KoiosClient,
	cache *Cache,
	network string,
	epoch uint64,
	addressUniverse []string,
	epochEndTime time.Time,
	graceHours int,
	logger *slog.Logger,
) (fetched int, err error) {
	return fetchAccountRewardsForEpoch(
		ctx, koios, cache, network, epoch, addressUniverse,
		epochEndTime, graceHours, 0, 0, false, logger,
	)
}

// fetchAccountRewardsForEpoch is FetchAccountRewardsForEpoch's real
// implementation, additionally parameterized by chunkSize/chunkMaxBytes (<=0
// meaning "use the package defaults", koiosAccountChunkSize and
// koiosAccountChunkMaxBytesDefault respectively) so FetchEpochAccountsWithAddrs
// can thread operator-configured --account-chunk-size/--account-chunk-max-bytes
// through without changing FetchAccountRewardsForEpoch's own public
// signature — every existing direct caller/test of that function keeps
// working unchanged.
//
// dingo #3099 adds durable, resumable checkpointing on top of #3097's
// original single-shot implementation: each chunk's fetched rows and
// per-address "checked" markers are saved to
// koios_account_fetch_staged_rows/koios_account_checked
// (Cache.SaveAccountFetchChunkProgress) as soon as that chunk succeeds,
// rather than only accumulated in memory. A killed/restarted process
// therefore resumes from whichever chunks already checkpointed
// (Cache.GetDoneAccountChunkHashes) instead of redoing the whole epoch's
// fetch from scratch. The final commit step — reading back every staged row
// (Cache.GetStagedAccountRows) and calling the existing, unmodified
// Cache.CommitAccountRewardsForEpoch exactly once — is otherwise identical to
// #3097's original single-shot commit: still one atomic, all-or-nothing
// write, still gated by graceHours/epochEndTime the same way, still never
// setting complete=true except when every chunk in the current plan has
// succeeded.
//
// Address universe changes across resumed attempts are handled by content-
// addressed chunk hashing (hashAddressChunk, sha256 of a sorted chunk's own
// addresses): Cache.InvalidateStaleAccountChunks prunes checkpoint state for
// any chunk whose exact address grouping is no longer part of the current
// plan before dispatch, so a changed universe or a --account-chunk-size/
// --account-chunk-max-bytes tuning change only re-fetches the affected
// chunks, never silently reuses stale progress from a different plan.
//
// forceRefresh (threaded from FetchConfig.ForceRefresh) bypasses that same
// content-addressed resume logic entirely: an unchanged address universe
// produces the exact same chunk hashes as a prior run, so without this an
// operator re-running with --force-refresh to repair suspected stale/corrupt
// cached data would find every chunk already "done" and skip re-dispatching
// any of them, silently re-committing the same old staged rows instead of
// actually going back to Koios.
func fetchAccountRewardsForEpoch(
	ctx context.Context,
	koios *KoiosClient,
	cache *Cache,
	network string,
	epoch uint64,
	addressUniverse []string,
	epochEndTime time.Time,
	graceHours int,
	chunkSize, chunkMaxBytes int,
	forceRefresh bool,
	logger *slog.Logger,
) (fetched int, err error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	now := time.Now()

	if len(addressUniverse) == 0 {
		// The universe can become empty after a previous, non-empty attempt
		// already left checkpoint data behind for this (network, epoch) —
		// e.g. Dingo's reward_account_output rows for this stake epoch were
		// pruned/rolled back between calls. None of that old data belongs to
		// the current (empty) plan; passing nil here invalidates every
		// existing chunk (InvalidateStaleAccountChunks treats "not in the
		// current plan" as stale for any hash not in currentChunkHashes,
		// and nil means none are). Skipping this would leave stale rows in
		// koios_account_checked/koios_account_fetch_staged_rows that
		// GetZeroRewardAccountsForEpoch/GetAccountUniverseForEpoch (used by
		// accountLifecycleMismatches) would then read as if they belonged to
		// this epoch's current, correctly-empty universe.
		if err := cache.InvalidateStaleAccountChunks(network, epoch, nil); err != nil {
			return 0, fmt.Errorf(
				"invalidate stale account chunks for empty universe: %w",
				err,
			)
		}
		// Commit a complete, empty coverage record so this epoch is never
		// perpetually treated as "not yet fetched" by checkEpoch's coverage
		// gate.
		if commitErr := cache.CommitAccountRewardsForEpoch(network, epoch, nil, 0, true, now); commitErr != nil {
			return 0, fmt.Errorf("commit empty account coverage: %w", commitErr)
		}
		return 0, nil
	}

	// Sorted so the same address set always produces the same chunk
	// boundaries (and therefore the same content-addressed chunk hashes)
	// regardless of addressUniverse's incoming order, which is not itself
	// guaranteed stable across calls (BuildAccountAddressUniverse's Dingo-side
	// half comes from a plain SQL query with no ORDER BY) — required for
	// resumability and selective invalidation to mean anything.
	sorted := make([]string, len(addressUniverse))
	copy(sorted, addressUniverse)
	sort.Strings(sorted)

	if chunkSize <= 0 {
		chunkSize = koiosAccountChunkSize
	}
	if chunkMaxBytes <= 0 {
		chunkMaxBytes = koiosAccountChunkMaxBytesDefault
	}
	// chunkAddressesByCountAndSize only bounds the encoded size of the
	// address array itself; the actual /account_reward_history POST body is
	// that array wrapped in {"_stake_addresses":[...],"_epoch_no":N} (see
	// GetAccountRewardHistory) — a fixed envelope that isn't otherwise
	// accounted for. Reserve it from the configured budget so the true
	// request body can never exceed --account-chunk-max-bytes; for any
	// reasonable configured budget this reservation is negligible, but for a
	// deliberately tiny one it's the difference between honoring the
	// configured bound and silently exceeding it.
	effectiveChunkMaxBytes := max(
		chunkMaxBytes-koiosAccountRequestEnvelopeOverhead,
		1,
	)
	groups := chunkAddressesByCountAndSize(
		sorted,
		chunkSize,
		effectiveChunkMaxBytes,
	)

	type chunkPlan struct {
		hash  string
		addrs []string
	}
	plans := make([]chunkPlan, len(groups))
	hashes := make([]string, len(groups))
	for i, g := range groups {
		h := hashAddressChunk(g)
		plans[i] = chunkPlan{hash: h, addrs: g}
		hashes[i] = h
	}

	// Only chunks whose hash actually changed are invalidated here — never
	// nil, even for forceRefresh: deleting every existing chunk's checkpoint
	// data up front, before any replacement data has actually been fetched,
	// would leave koios_account_checked/koios_account_fetch_staged_rows
	// wiped or partial if this call then fails partway (e.g. a mid-refresh
	// Koios outage), while the untouched koios_account_coverage row from the
	// last successful commit still reports complete=true — so a later
	// Observer/Fetch attempt would never retry, and accountLifecycleMismatches
	// would read that partial/wiped state as if it belonged to a genuinely
	// complete epoch. forceRefresh instead bypasses the doneHashes skip below,
	// so every chunk is re-dispatched regardless — each one's own
	// SaveAccountFetchChunkProgress call safely replaces just that chunk's
	// rows in one transaction once its re-fetch actually succeeds, so a
	// chunk never re-reached by a failed refresh attempt simply keeps
	// whatever valid data it already had.
	if err := cache.InvalidateStaleAccountChunks(network, epoch, hashes); err != nil {
		return 0, fmt.Errorf("invalidate stale account chunks: %w", err)
	}
	doneHashes, err := cache.GetDoneAccountChunkHashes(network, epoch)
	if err != nil {
		return 0, fmt.Errorf("get done account chunks: %w", err)
	}

	// A chunk that checkpointed with zero rows while still within the grace
	// window must not be trusted as final: Koios's own /account_reward_history
	// publishing can lag behind epoch closure (see the zero-row/grace-hours
	// gate below), so an empty result checkpointed early could simply mean
	// "hasn't published yet," not "confirmed nothing." Without this check, a
	// resumed/retried call would see every such chunk as already "done,"
	// never re-ask Koios, and eventually commit complete=true from stale,
	// possibly-lagged empty checkpoints once graceHours elapses — silently
	// losing rewards Koios published in the meantime. Only a chunk with
	// genuinely non-empty staged rows, or one checked after the grace window
	// has already closed, is safe to skip re-dispatching.
	withinGrace := graceHours > 0 && !epochEndTime.IsZero() &&
		now.Sub(epochEndTime) < time.Duration(graceHours)*time.Hour
	var hashesWithRows map[string]bool
	if withinGrace {
		hashesWithRows, err = cache.GetChunkHashesWithStagedRows(network, epoch)
		if err != nil {
			return 0, fmt.Errorf("get chunk hashes with staged rows: %w", err)
		}
	}

	var pending []chunkPlan
	for _, p := range plans {
		// forceRefresh (--force-refresh) never trusts an existing checkpoint
		// as done, regardless of doneHashes/hashesWithRows — see the
		// invalidation comment above for why this bypasses the skip instead
		// of deleting the old data up front.
		if !forceRefresh && doneHashes[p.hash] &&
			(!withinGrace || hashesWithRows[p.hash]) {
			continue
		}
		pending = append(pending, p)
	}

	var errMu sync.Mutex
	var firstErr error
	var newlyFetched atomic.Int64

	// fetchCtx is cancelled the moment any chunk error is seen — transient
	// or permanent — so this call stops scheduling further doomed chunk
	// requests once the epoch's reference set can no longer be complete;
	// see the per-goroutine comment below for why transient failures are
	// included, not just permanent ones. Mirrors fetchEpoch's poolCtx.
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, accountFetchConcurrency)
	var wg sync.WaitGroup

outer:
	for _, plan := range pending {
		// Check first so a cancellation from a prior iteration's chunk error
		// is never missed simply because the semaphore also happened to have
		// room this iteration (see the post-acquire recheck below for why a
		// single check here is not by itself sufficient).
		if fetchCtx.Err() != nil {
			break outer
		}
		select {
		case <-fetchCtx.Done():
			break outer
		case sem <- struct{}{}:
		}
		// Recheck immediately after acquiring the semaphore slot: Done() and
		// sem <- struct{}{} being simultaneously ready makes Go's select
		// choose between them nondeterministically, so cancellation fired by
		// a concurrently-running chunk's error between this iteration's top
		// check and the select above could otherwise still let this
		// iteration dispatch one more doomed worker. Release the slot and
		// stop rather than launch it.
		if fetchCtx.Err() != nil {
			<-sem
			break outer
		}

		wg.Add(1)
		go func(plan chunkPlan) {
			defer wg.Done()
			defer func() { <-sem }()

			items, hErr := koios.GetAccountRewardHistory(fetchCtx, plan.addrs, epoch)
			if hErr != nil {
				wrapped := fmt.Errorf("account reward history chunk (%d addresses): %w", len(plan.addrs), hErr)
				isPermanent := errors.Is(hErr, ErrKoiosPermanent)
				errMu.Lock()
				if firstErr == nil ||
					(isPermanent && !errors.Is(firstErr, ErrKoiosPermanent)) {
					firstErr = wrapped
				}
				errMu.Unlock()
				// A missing chunk means this epoch's account reference set can
				// never be complete — nothing is committed below once err !=
				// nil, so there is no point spending the rest of the epoch's
				// Koios request budget on chunks whose results will only be
				// discarded. Cancel on the first error, transient or
				// permanent, not just permanent ones. This only cancels
				// fetchCtx (this call's own per-epoch context derived from
				// ctx above), never the caller's shared multi-epoch context,
				// so an isolated transient failure still just drops this one
				// epoch for a later retry — and, unlike #3097's original
				// implementation, whichever chunks already checkpointed
				// before this error fired survive for that later retry to
				// resume from.
				cancel()
				// afterChunkCancelForTest, when non-nil, lets a test observe
				// deterministically that cancel() has actually fired for this
				// chunk's error, rather than inferring it from a fixed sleep
				// margin (see fetch_accounts_test.go's
				// TestFetchAccountRewardsForEpochStopsDispatchingAfterFirstChunkError).
				// nil in every non-test build/call path, so this is a no-op
				// outside that one test.
				if afterChunkCancelForTest != nil {
					afterChunkCancelForTest()
				}
				return
			}

			rows := make([]KoiosAccountRewards, len(items))
			for i, item := range items {
				rows[i] = KoiosAccountRewards{
					StakeAddress:   item.StakeAddress,
					RewardType:     item.Type,
					Earned:         item.Amount,
					SpendableEpoch: item.SpendableEpoch,
					PoolIDBech32:   strOrEmpty(item.PoolIDBech32),
					FetchedAt:      now,
				}
			}
			if saveErr := cache.SaveAccountFetchChunkProgress(network, epoch, plan.hash, rows, plan.addrs, now); saveErr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("save account fetch chunk progress: %w", saveErr)
				}
				errMu.Unlock()
				cancel()
				return
			}
			newlyFetched.Add(int64(len(rows)))
		}(plan)
	}

	wg.Wait()

	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	errMu.Lock()
	err = firstErr
	errMu.Unlock()
	if err != nil {
		return 0, classifyFetchErr(err)
	}

	stagedRows, err := cache.GetStagedAccountRows(network, epoch)
	if err != nil {
		return 0, fmt.Errorf("get staged account rows: %w", err)
	}

	// A zero-row result, for an epoch that closed within the grace window,
	// is far more likely to be Koios's own account_reward_history publishing
	// lag than genuine "nobody earned a reward this epoch" — see this
	// function's doc comment. This must be judged per chunk, not on the
	// aggregate stagedRows total: if one chunk has real rows while another
	// is still empty-and-lagging, the aggregate total alone is non-zero even
	// though the lagging chunk was never actually reconfirmed this call —
	// committing complete=true here would let that chunk's later-published
	// rewards be permanently missed, since a completed epoch is never
	// re-fetched. Re-querying after dispatch (rather than reusing the
	// pre-dispatch hashesWithRows) picks up whatever any chunk resolved to
	// during this very call.
	finalHashesWithRows, err := cache.GetChunkHashesWithStagedRows(
		network,
		epoch,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"get chunk hashes with staged rows (final): %w",
			err,
		)
	}
	allChunksHaveRows := true
	for _, p := range plans {
		if !finalHashesWithRows[p.hash] {
			allChunksHaveRows = false
			break
		}
	}
	complete := allChunksHaveRows || graceHours <= 0 ||
		epochEndTime.IsZero() ||
		now.Sub(epochEndTime) >= time.Duration(graceHours)*time.Hour

	if commitErr := cache.CommitAccountRewardsForEpoch(network, epoch, stagedRows, len(addressUniverse), complete, now); commitErr != nil {
		return 0, fmt.Errorf("commit account rewards: %w", commitErr)
	}
	// koios_account_fetch_staged_rows/koios_account_checked are deliberately
	// NOT cleared here, even once complete=true: koios_account_checked is
	// the durable ledger accountLifecycleMismatches (check.go) reads later
	// to report zero-reward/newly-registered/deregistered accounts, and
	// koios_account_fetch_staged_rows must survive so a later idempotent
	// re-run of this same, already-complete epoch with an unchanged universe
	// finds every chunk already done AND still has real staged rows to
	// re-commit — clearing either table here would make that re-run commit
	// an empty reward set over the correct one. --force-refresh instead goes
	// through the forceRefresh path above, which unconditionally invalidates
	// every existing chunk before dispatch so it always re-fetches from
	// Koios rather than relying on this retained state.

	logger.Info("koiosparity: epoch account rewards fetched",
		"network", network,
		"epoch", epoch,
		"addresses", len(addressUniverse),
		"chunks", len(plans),
		"pending_chunks", len(pending),
		"rows", len(stagedRows),
		"complete", complete,
	)
	return int(newlyFetched.Load()), nil
}

// FetchEpochAccountsWithAddrs fetches and caches Koios account-reward
// reference data for exactly one epoch, given an already-resolved Koios
// account-address list (koiosAddrs — see ResolveKoiosAccountUniverse) and
// the Dingo RewardParitySource (nil is valid; see
// BuildAccountAddressUniverse) to union in Dingo's own known addresses.
// Callers that fetch many epochs in one run (Fetch, Observer) should resolve
// koiosAddrs once and reuse it across every call, exactly like
// FetchEpochWithPools does for the pool universe.
//
// graceHours is forwarded to FetchAccountRewardsForEpoch's zero-row/lag gate
// (see its doc comment); the epoch's EpochEndTime is looked up from the
// already-committed koios_epoch_info row (fetchEpoch/fetchPoolsIfNeeded
// always commit it before this is ever called — see
// GetEpochsMissingAccountCoverage's join). Only a genuinely missing row
// (sql.ErrNoRows — no koios_epoch_info row exists yet, which should not
// happen in practice) leaves epochEndTime at its zero value, which simply
// disables the grace check rather than failing the fetch outright. Any other
// GetEpochInfo error (a real database error) is propagated as a failure of
// this function instead: silently treating it the same as "unknown end
// time" would let an empty account fetch bypass the grace gate and commit
// as complete, suppressing retries and risking a false PASS.
//
// chunkSize/chunkMaxBytes (dingo #3099) thread operator-configured
// --account-chunk-size/--account-chunk-max-bytes through to
// fetchAccountRewardsForEpoch's dual-bounded chunking; 0 for either means
// "use the package default" (koiosAccountChunkSize/koiosAccountChunkMaxBytesDefault).
//
// forceRefresh (threaded from FetchConfig.ForceRefresh; always false for the
// Observer, which has no force-refresh concept) forces
// fetchAccountRewardsForEpoch to re-dispatch every chunk in the current plan
// regardless of existing checkpoint state, so an operator-requested
// --force-refresh actually repairs suspected stale/corrupt cached data by
// going back to Koios, rather than finding every chunk already checkpointed
// "done" and silently re-committing the same old rows. It never deletes
// existing checkpoint data up front — each chunk's own successful re-fetch
// safely replaces just that chunk's rows, so a failed/interrupted refresh
// attempt never leaves an already-covered epoch with wiped or partial
// checkpoint state.
func FetchEpochAccountsWithAddrs(
	ctx context.Context,
	koios *KoiosClient,
	cache *Cache,
	network string,
	epoch uint64,
	source RewardParitySource,
	koiosAddrs []string,
	graceHours int,
	chunkSize, chunkMaxBytes int,
	forceRefresh bool,
	logger *slog.Logger,
) (int, error) {
	stakeEpoch, ok := koiosStakeEpoch(epoch)
	if !ok {
		// Pre-staking epoch (0 or 1, see preStakingThroughEpoch) — no valid
		// stake epoch, and fetchEpoch's own PreStaking-marker path never
		// reaches this for epochs 0-1 in practice. Nothing to fetch — and no
		// koios_account_coverage row is written, matching how these epochs
		// never get a real account fetch attempt; GetEpochsMissingAccountCoverage
		// explicitly excludes pre_staking epochs so this never causes a
		// perpetual backfill re-selection.
		return 0, nil
	}
	universe, err := BuildAccountAddressUniverse(
		ctx,
		source,
		stakeEpoch,
		koiosAddrs,
	)
	if err != nil {
		return 0, classifyFetchErr(
			fmt.Errorf("build account address universe: %w", err),
		)
	}
	var epochEndTime time.Time
	info, infoErr := cache.GetEpochInfo(network, epoch)
	switch {
	case infoErr == nil:
		epochEndTime = info.EpochEndTime
	case errors.Is(infoErr, sql.ErrNoRows):
		// No koios_epoch_info row yet — leave epochEndTime at its zero
		// value, which FetchAccountRewardsForEpoch's grace-hours gate
		// already treats as "unknown end time" (see its epochEndTime.IsZero()
		// checks).
	default:
		return 0, classifyFetchErr(
			fmt.Errorf("get epoch info for grace-hours gate: %w", infoErr),
		)
	}
	return fetchAccountRewardsForEpoch(
		ctx,
		koios,
		cache,
		network,
		epoch,
		universe,
		epochEndTime,
		graceHours,
		chunkSize,
		chunkMaxBytes,
		forceRefresh,
		logger,
	)
}
