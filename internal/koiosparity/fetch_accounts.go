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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// accountFetchConcurrency bounds how many /account_reward_history chunk
// requests run in parallel for a single epoch's account fetch — mirrors
// fetchEpoch's per-pool worker pool concurrency.
const accountFetchConcurrency = 5

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

// chunkAddresses splits addrs into groups of at most size, preserving order.
func chunkAddresses(addrs []string, size int) [][]string {
	if size <= 0 {
		size = koiosAccountChunkSize
	}
	if len(addrs) == 0 {
		return nil
	}
	chunks := make([][]string, 0, (len(addrs)+size-1)/size)
	for i := 0; i < len(addrs); i += size {
		end := min(i+size, len(addrs))
		chunks = append(chunks, addrs[i:end])
	}
	return chunks
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
func FetchAccountRewardsForEpoch(
	ctx context.Context,
	koios *KoiosClient,
	cache *Cache,
	network string,
	epoch uint64,
	addressUniverse []string,
	logger *slog.Logger,
) (fetched int, err error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	now := time.Now()

	if len(addressUniverse) == 0 {
		// Nothing to check — commit a complete, empty coverage record so this
		// epoch is never perpetually treated as "not yet fetched" by
		// checkEpoch's coverage gate.
		if commitErr := cache.CommitAccountRewardsForEpoch(network, epoch, nil, 0, true, now); commitErr != nil {
			return 0, fmt.Errorf("commit empty account coverage: %w", commitErr)
		}
		return 0, nil
	}

	chunks := chunkAddresses(addressUniverse, koiosAccountChunkSize)

	var mu sync.Mutex
	var rows []KoiosAccountRewards

	var errMu sync.Mutex
	var firstErr error

	// fetchCtx is cancelled the moment a permanent error is seen, so this
	// call stops scheduling further doomed chunk requests — mirrors
	// fetchEpoch's poolCtx.
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, accountFetchConcurrency)
	var wg sync.WaitGroup

outer:
	for _, chunk := range chunks {
		select {
		case <-fetchCtx.Done():
			break outer
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(addrs []string) {
			defer wg.Done()
			defer func() { <-sem }()

			items, hErr := koios.GetAccountRewardHistory(fetchCtx, addrs, epoch)
			if hErr != nil {
				wrapped := fmt.Errorf("account reward history chunk (%d addresses): %w", len(addrs), hErr)
				isPermanent := errors.Is(hErr, ErrKoiosPermanent)
				errMu.Lock()
				if firstErr == nil ||
					(isPermanent && !errors.Is(firstErr, ErrKoiosPermanent)) {
					firstErr = wrapped
				}
				errMu.Unlock()
				if isPermanent {
					cancel()
				}
				return
			}

			mu.Lock()
			for _, item := range items {
				rows = append(rows, KoiosAccountRewards{
					StakeAddress:   item.StakeAddress,
					RewardType:     item.Type,
					Earned:         item.Amount,
					SpendableEpoch: item.SpendableEpoch,
					PoolIDBech32:   strOrEmpty(item.PoolIDBech32),
					FetchedAt:      now,
				})
			}
			mu.Unlock()
		}(chunk)
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

	if commitErr := cache.CommitAccountRewardsForEpoch(network, epoch, rows, len(addressUniverse), true, now); commitErr != nil {
		return 0, fmt.Errorf("commit account rewards: %w", commitErr)
	}

	logger.Info("koiosparity: epoch account rewards fetched",
		"network", network,
		"epoch", epoch,
		"addresses", len(addressUniverse),
		"chunks", len(chunks),
		"rows", len(rows),
	)
	return len(rows), nil
}

// FetchEpochAccountsWithAddrs fetches and caches Koios account-reward
// reference data for exactly one epoch, given an already-resolved Koios
// account-address list (koiosAddrs — see ResolveKoiosAccountUniverse) and
// the Dingo RewardParitySource (nil is valid; see
// BuildAccountAddressUniverse) to union in Dingo's own known addresses.
// Callers that fetch many epochs in one run (Fetch, Observer) should resolve
// koiosAddrs once and reuse it across every call, exactly like
// FetchEpochWithPools does for the pool universe.
func FetchEpochAccountsWithAddrs(
	ctx context.Context,
	koios *KoiosClient,
	cache *Cache,
	network string,
	epoch uint64,
	source RewardParitySource,
	koiosAddrs []string,
	logger *slog.Logger,
) (int, error) {
	stakeEpoch, ok := koiosStakeEpoch(epoch)
	if !ok {
		// Pre-staking epoch (0) — no valid stake epoch, and fetchEpoch's own
		// PreStaking-marker path never reaches this for epochs 0-1 in
		// practice. Nothing to fetch.
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
	return FetchAccountRewardsForEpoch(
		ctx,
		koios,
		cache,
		network,
		epoch,
		universe,
		logger,
	)
}
