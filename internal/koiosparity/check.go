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
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
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
	// AccountsEnabled runs #3097's per-account exact-parity comparison phase
	// (CompareAccountEpoch) alongside the existing epoch-aggregate/pool
	// phases, consulting koios_account_rewards/koios_account_coverage —
	// which only ever get populated when the corresponding fetch phase was
	// also run with accounts enabled (see FetchConfig.AccountsEnabled). False
	// by default for the standalone CLI (opt-in: per-account checking issues
	// far more Koios requests than pool-level checking); see
	// ObserverConfig.AccountsEnabled for the in-process default.
	AccountsEnabled bool
}

// CheckResult summarises a completed check run.
type CheckResult struct {
	EpochsChecked int // epochs freshly (re)checked this run
	PoolsChecked  int
	MismatchCount int // mismatches recorded for epochs freshly (re)checked this run

	// FailEpochs/ErrorEpochs are the *persisted* status for every epoch within
	// the requested scope (CheckConfig.FromEpoch/ThroughEpoch, 0 = unbounded on
	// that side) — not just epochs freshly (re)checked this run. This is
	// deliberate: when nothing needs (re)checking (e.g. a prior FAIL/ERROR's
	// reference row is still fresh), the persisted failure must still surface
	// here rather than reading as success just because no work was performed.
	// See EffectiveCheckOutcome.
	FailEpochs  []uint64
	ErrorEpochs []uint64
}

// EffectiveCheckOutcome derives FailEpochs/ErrorEpochs from persisted
// CheckEpochStatus rows, restricted to [fromEpoch, throughEpoch] (0 = no
// bound on that side). Used both internally by Check (for its requested
// scope) and by callers that need the exit-code-relevant outcome for a status
// summary they already fetched (e.g. the default 'run' command), so a stale
// CheckResult (nil, or empty because nothing needed rechecking) never masks a
// persisted FAIL/ERROR.
func EffectiveCheckOutcome(
	statuses []CheckEpochStatus,
	fromEpoch, throughEpoch uint64,
) *CheckResult {
	result := &CheckResult{}
	for _, s := range statuses {
		if fromEpoch > 0 && s.Epoch < fromEpoch {
			continue
		}
		if throughEpoch > 0 && s.Epoch > throughEpoch {
			continue
		}
		switch s.Status {
		case StatusFail:
			result.FailEpochs = append(result.FailEpochs, s.Epoch)
		case StatusError:
			result.ErrorEpochs = append(result.ErrorEpochs, s.Epoch)
		}
	}
	return result
}

// Check compares the Koios reference cache against Dingo's metadata database
// for unchecked or stale epochs. It reads reward_pool_input and epoch_summary
// directly — no Blockfrost or other HTTP endpoints are contacted.
func Check(
	ctx context.Context,
	cfg CheckConfig,
	logger *slog.Logger,
) (*CheckResult, error) {
	// Check works entirely from the cache and never constructs a
	// KoiosClient (unlike Fetch), so nothing else validates cfg.Network —
	// without this, an unsupported network (e.g. "mainnet") would reach
	// compareEpochAccounts/StakeAddressFromCredential unrejected. See
	// validateKoiosNetwork's doc comment.
	if err := validateKoiosNetwork(cfg.Network); err != nil {
		return nil, err
	}
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
		epochs, err = cache.GetEpochsNeedingCheck(cfg.Network, cfg.AccountsEnabled)
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

	result := &CheckResult{}

	if len(epochs) == 0 {
		logger.Info("koiosparity: nothing to (re)check", "network", cfg.Network)
	} else {
		logger.Info("koiosparity: checking epochs",
			"network", cfg.Network,
			"count", len(epochs),
			"workers", cfg.Workers,
		)

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

				res, checkErr := checkEpoch(ctx, cache, dingo, cfg.Network, epoch, cfg.GraceHours, cfg.AccountsEnabled, logger)
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
	}

	// FailEpochs/ErrorEpochs reflect the persisted status of every epoch in
	// scope (see EffectiveCheckOutcome), computed after the loop above so a
	// prior FAIL/ERROR whose reference row is still fresh — and therefore
	// wasn't re-checked above — still surfaces here instead of reading as
	// success just because len(epochs) was 0 or nothing new failed.
	statuses, err := cache.GetStatusSummary(cfg.Network)
	if err != nil {
		return nil, fmt.Errorf("get status summary: %w", err)
	}
	effective := EffectiveCheckOutcome(
		statuses,
		cfg.FromEpoch,
		cfg.ThroughEpoch,
	)
	result.FailEpochs = effective.FailEpochs
	result.ErrorEpochs = effective.ErrorEpochs

	logger.Info("koiosparity: check complete",
		"network", cfg.Network,
		"epochs_checked", result.EpochsChecked,
		"mismatches", result.MismatchCount,
		"fail_epochs", len(result.FailEpochs),
		"error_epochs", len(result.ErrorEpochs),
	)
	return result, nil
}

// CheckEpoch compares the Koios reference cache against source (either a
// standalone DingoDB connection or the dingo #3098 in-process
// DatabaseSource) for exactly one epoch, persisting the result the same way
// checkEpoch's callers inside Check do. This is the primitive both Check's
// batch CLI/watch-loop mode and the in-process epoch observer (observer.go)
// share — the observer needs a "validate exactly this epoch now, regardless
// of whether it was already checked" call unconditioned by
// GetEpochsNeedingCheck's Koios-reference-freshness gate, so a
// rollback-driven replay of an already-checked epoch's boundary is always
// re-validated against Dingo's corrected committed state rather than trusting
// a stale prior result.
func CheckEpoch(
	ctx context.Context,
	cache *Cache,
	source RewardParitySource,
	network string,
	epoch uint64,
	graceHours int,
	accountsEnabled bool,
	logger *slog.Logger,
) (*EpochCompareResult, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	// See Check's identical guard: CheckEpoch is a separate public entry
	// point (used directly by Observer) that likewise never constructs a
	// KoiosClient, so it needs its own validateKoiosNetwork call rather than
	// relying on a caller upstream to have already checked network.
	if err := validateKoiosNetwork(network); err != nil {
		return nil, err
	}
	return checkEpoch(
		ctx,
		cache,
		source,
		network,
		epoch,
		graceHours,
		accountsEnabled,
		logger,
	)
}

// preStakingThroughEpoch is the last Koios reporting epoch that predates a
// valid "go" stake snapshot (see koiosStakeEpoch's doc comment for the
// mark/set/go derivation). fetchEpoch commits a PreStaking marker instead of
// erroring/retrying forever for epoch <= preStakingThroughEpoch;
// checkEpoch/koiosStakeEpoch/the #3097 account-fetch path
// (FetchEpochAccountsWithAddrs) all key off this same constant so the
// pre-staking exclusion stays consistent across every phase — a null
// active_stake on any epoch above this value is a real, retryable error
// instead.
const preStakingThroughEpoch = 1

// koiosStakeEpoch returns the Dingo epoch whose reward_pool_input/
// reward_pool_output rows and epoch_summary/mark stake distribution actually
// correspond to Koios reporting epoch koiosEpoch's active stake and reward
// calculation ("K-1").
//
// This is not the same epoch number Koios reports things under, because
// Dingo's reward_pool_input.Epoch is stamped by *capture time*
// (ledger/snapshot/rotation.go's saveSnapshotInTxn, called with
// epoch=evt.NewEpoch), while the same row's DelegatedStake/DelegatorCount are
// the "go" stake distribution reward_calculation.go's
// stakeRewardEpochsForNewEpoch actually consumes one epoch later, at
// epochs.snapshot = epochs.performance - 1 (performance being the true,
// unambiguous calendar epoch whose blocks are being measured — the same
// epoch Koios reports the corresponding pool_history/epoch_info row under).
// reward_pool_output.Epoch is written from that same epochs.snapshot value in
// the same reward-application call, so it shares this offset with
// reward_pool_input's stake fields exactly. See ARCHITECTURE.md's Koios
// Parity Tracker "Epoch alignment" section for the full derivation and
// koiosParamEpoch below for the distinct offset reward_pool_input's
// BlocksProduced/Margin/FixedCost fields need instead.
//
// ok is false for koiosEpoch <= preStakingThroughEpoch (0 and 1), neither of
// which has a valid stake epoch (checkEpoch never reaches this for those
// epochs in practice — it's already filtered out by the PreStaking marker
// fetch commits for epochs 0-1 — but the guard both avoids a uint64
// underflow for epoch 0 and keeps epoch 1 from being treated as having a
// real stakeEpoch of 0, which previously let #3097's account-fetch path
// wastefully run Koios requests for a pre-staking epoch).
func koiosStakeEpoch(koiosEpoch uint64) (epoch uint64, ok bool) {
	if koiosEpoch <= preStakingThroughEpoch {
		return 0, false
	}
	return koiosEpoch - 1, true
}

// koiosParamEpoch returns the Dingo reward_pool_input epoch whose
// BlocksProduced and pool Margin/FixedCost fields describe Koios reporting
// epoch koiosEpoch ("K+1"). ledger/snapshot/rotation.go's
// buildRewardStateInputs stamps these fields from evt.PreviousEpoch (the just
// -ended epoch) onto the row captured for the *new* epoch — one epoch after
// the epoch they describe — independent of the stake-epoch offset above,
// which governs the same row's DelegatedStake/DelegatorCount instead. See
// koiosStakeEpoch's doc comment and ARCHITECTURE.md.
func koiosParamEpoch(koiosEpoch uint64) uint64 {
	return koiosEpoch + 1
}

// checkEpoch performs the full comparison for one epoch and persists results.
//
// Epoch aggregates (epoch_summary, reward_ada_pots) are compared against the
// Koios epoch_info row. Per-pool reward inputs (reward_pool_input) are compared
// against Koios pool_history rows using a single bulk DB query per epoch.
// Koios defines epoch pool membership; pools in Dingo but not in Koios are
// flagged as pool_only_dingo.
//
// Dingo's reward_pool_input/reward_pool_output/epoch_summary rows do not
// share Koios's epoch numbering uniformly across fields — see
// koiosStakeEpoch/koiosParamEpoch above — so this function resolves the
// distinct Dingo epoch numbers each field group actually needs rather than
// querying epoch itself for everything. reward_ada_pots (treasury/reserves/
// fees, compared in CompareEpochTotals) is unaffected and still read at
// epoch itself: it is a point-in-time ledger pot balance captured at the
// boundary into that same epoch, not a delayed reward-calculation input.
func checkEpoch(
	ctx context.Context,
	cache *Cache,
	dingo RewardParitySource,
	network string,
	epoch uint64,
	graceHours int,
	accountsEnabled bool,
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

	// Pre-staking epochs (Koios active_stake=null) have no reference data to
	// compare against — record PASS with zero mismatches rather than running
	// comparisons against an empty pool set, which would spuriously flag every
	// Dingo-side pool as pool_only_dingo. Nothing fallible needs to complete
	// first, so it's safe to replace any prior evidence immediately.
	if koiosEpoch.PreStaking {
		if err := cache.CommitEpochMismatches(network, epoch, nil); err != nil {
			return nil, fmt.Errorf("commit mismatches: %w", err)
		}
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

	// Resolve the distinct Dingo epoch numbers this Koios epoch's fields
	// actually live under — see koiosStakeEpoch/koiosParamEpoch. stakeEpoch is
	// always valid here: PreStaking (checked above) already excludes Koios
	// epochs 0-1, so epoch >= 2 and stakeEpoch = epoch-1 >= 1.
	stakeEpoch, hasStakeEpoch := koiosStakeEpoch(epoch)
	paramEpoch := koiosParamEpoch(epoch)

	// 1. Compare epoch-level aggregates. total_active_stake needs the mark
	// stake distribution actually used as this epoch's reward/leader-election
	// basis (epoch_summary at stakeEpoch), not epoch_summary at epoch itself.
	var dingoEpochStake *DingoEpochData
	var epochErr error
	if hasStakeEpoch {
		dingoEpochStake, epochErr = dingo.GetEpochData(ctx, stakeEpoch)
	} else {
		epochErr = fmt.Errorf("epoch %d: no valid stake epoch (predates staking)", epoch)
	}
	allMismatches = append(
		allMismatches,
		CompareEpochAggregates(
			network,
			epoch,
			koiosEpoch,
			dingoEpochStake,
			epochErr,
			now,
			graceHours,
		)...,
	)

	// 1b. Compare /totals fields (treasury, reserves, and totals' own
	// fees/reward) — independent of the /epoch_info comparison above despite
	// overlapping field names; see CompareEpochTotals. Unlike total_active_stake
	// above, reward_ada_pots is a point-in-time ledger pot balance captured at
	// the boundary into epoch itself (not a delayed reward-calculation input),
	// so it is read at epoch, not stakeEpoch — see ARCHITECTURE.md. A missing
	// totals row (e.g. cached before totals fetching existed, or a
	// --skip-fetch run against a cache that never fetched it) is reported by
	// CompareEpochTotals as an explicit "koios_totals" / CategoryDBMissing
	// mismatch rather than skipped, so it can never silently produce a PASS
	// that never actually validated treasury/reserves/fees. CompareEpochTotals
	// has no fetchErr parameter (unlike CompareEpochAggregates) since it
	// deliberately skips only on a nil dingoEpoch, rather than duplicate-report
	// a DB error under a second field name; a real query failure here is
	// reported explicitly below instead.
	dingoEpochPots, potsErr := dingo.GetEpochData(ctx, epoch)
	if potsErr != nil {
		allMismatches = append(allMismatches, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "reward_ada_pots_fetch",
			DingoValue: fmt.Sprintf("error: %v", potsErr),
			KoiosValue: "",
			Category:   CategoryDBError,
			CheckedAt:  now,
		})
		dingoEpochPots = nil
	}
	koiosTotals, totalsErr := cache.GetTotals(network, epoch)
	if totalsErr != nil && !errors.Is(totalsErr, sql.ErrNoRows) {
		return nil, fmt.Errorf("get koios totals: %w", totalsErr)
	}
	allMismatches = append(allMismatches,
		CompareEpochTotals(network, epoch, koiosTotals, dingoEpochPots, now)...,
	)

	// 2. Bulk-load all pool reward data for this epoch from Dingo's DB,
	// spread across stakeEpoch (active stake/delegator count/member rewards)
	// and paramEpoch (blocks produced/margin/fixed cost) — see
	// GetPoolEpochDataMap's doc comment. Two-to-three queries regardless of
	// how many pools Koios knows about.
	var dingoPoolMap map[string]*DingoPoolEpochData
	var dingoPoolErr error
	if hasStakeEpoch {
		dingoPoolMap, dingoPoolErr = dingo.GetPoolEpochDataMap(
			ctx,
			stakeEpoch,
			paramEpoch,
		)
	} else {
		dingoPoolErr = epochErr
	}
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
			poolMismatches := ComparePoolEpoch(
				network,
				epoch,
				koiosPool,
				dingoPool,
				now,
				graceHours,
				epochEndTime,
			)
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
		for keyHex, dingoPool := range dingoPoolMap {
			// GetPoolEpochDataMap returns the union of the stake-epoch
			// (K-1) and param-epoch (K+1) reads, so a pool that registered
			// during K appears here through its param-epoch row alone: its
			// first mark snapshot is captured at the boundary into K+1, and
			// it is not part of K's active-stake basis at all. Koios has no
			// pool_history row for it until K+2, so comparing presence at K
			// would report a divergence where both sides agree the pool did
			// not yet exist. Only a pool actually in K's stake basis can be
			// present-only-in-Dingo (dingo #3483).
			if !dingoPool.StakePresent {
				continue
			}
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

	// 5. Per-account exact parity (#3097) — opt-in (accountsEnabled), and
	// only for epochs with a valid stake epoch (mirrors the pool-comparison
	// phases above; PreStaking epochs never reach here at all, so
	// hasStakeEpoch is always true in practice — see koiosStakeEpoch's doc
	// comment). Gated behind the account-coverage completeness check so an
	// interrupted or not-yet-run account fetch can never be silently treated
	// as "nothing to compare" — see KoiosAccountCoverage's doc comment.
	if accountsEnabled && hasStakeEpoch {
		allMismatches = append(
			allMismatches,
			compareEpochAccounts(
				ctx,
				cache,
				dingo,
				network,
				epoch,
				stakeEpoch,
				now,
				graceHours,
				epochEndTime,
				logger,
			)...,
		)
	}

	status := DetermineStatus(allMismatches)

	// Every fallible read above has already succeeded by this point, so it's
	// safe to replace prior evidence now. CommitEpochMismatches deletes and
	// (re)inserts in one transaction: if the insert fails partway through,
	// the delete rolls back with it and the previous record is left intact
	// instead of being erased with nothing to replace it.
	if err := cache.CommitEpochMismatches(network, epoch, allMismatches); err != nil {
		return nil, fmt.Errorf("commit mismatches: %w", err)
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

// compareEpochAccounts runs #3097's per-account exact-parity comparison for
// one epoch: it first consults KoiosAccountCoverage to make sure a complete
// Koios account-reward fetch actually exists for this epoch (never treating
// an absent/incomplete coverage row as "nothing to compare" — see
// CategoryAcctCoverageIncomplete's doc comment), then loads both sides and
// delegates the actual comparison to CompareAccountEpoch.
//
// stakeEpoch is the Dingo epoch reward_account_output rows for Koios
// reporting epoch `epoch` actually live under — the same koiosStakeEpoch
// (K-1) offset reward_pool_output already uses, since both are written from
// app.epochs.snapshot in the same ledger/reward_calculation.go call (see
// koiosStakeEpoch's doc comment and ARCHITECTURE.md's Epoch alignment
// section).
func compareEpochAccounts(
	ctx context.Context,
	cache *Cache,
	dingo RewardParitySource,
	network string,
	epoch, stakeEpoch uint64,
	now time.Time,
	graceHours int,
	epochEndTime time.Time,
	logger *slog.Logger,
) []CheckMismatch {
	coverage, covErr := cache.GetAccountCoverage(network, epoch)
	// sql.ErrNoRows ("no fetch has been attempted for this epoch yet") is a
	// legitimate incomplete-coverage state; any other error is a genuine
	// cache/DB failure and must not be mistaken for "nothing to compare".
	if covErr != nil && !errors.Is(covErr, sql.ErrNoRows) {
		return []CheckMismatch{{
			Network:    network,
			Epoch:      epoch,
			Field:      "koios_account_coverage",
			DingoValue: "",
			KoiosValue: fmt.Sprintf("error: %v", covErr),
			Category:   CategoryDBError,
			CheckedAt:  now,
		}}
	}
	if covErr != nil || coverage == nil || !coverage.Complete {
		detail := "no account fetch has been attempted for this epoch"
		if covErr == nil && coverage != nil {
			detail = fmt.Sprintf(
				"account fetch incomplete: requested=%d fetched=%d",
				coverage.RequestedCount,
				coverage.FetchedCount,
			)
		}
		return []CheckMismatch{{
			Network:    network,
			Epoch:      epoch,
			Field:      "koios_account_coverage",
			DingoValue: "",
			KoiosValue: detail,
			Category:   CategoryAcctCoverageIncomplete,
			CheckedAt:  now,
		}}
	}

	koiosRows, err := cache.GetAccountRewardsForEpoch(network, epoch)
	if err != nil {
		return []CheckMismatch{{
			Network:    network,
			Epoch:      epoch,
			Field:      "koios_account_rewards",
			DingoValue: "",
			KoiosValue: fmt.Sprintf("error: %v", err),
			Category:   CategoryDBError,
			CheckedAt:  now,
		}}
	}

	dingoOutputs, err := dingo.GetRewardAccountOutputs(ctx, stakeEpoch)
	if err != nil {
		return []CheckMismatch{{
			Network:    network,
			Epoch:      epoch,
			Field:      "reward_account_output",
			DingoValue: fmt.Sprintf("error: %v", err),
			KoiosValue: "",
			Category:   CategoryDBError,
			CheckedAt:  now,
		}}
	}

	var out []CheckMismatch
	dingoRows := make([]DingoAccountReward, 0, len(dingoOutputs))
	for _, row := range dingoOutputs {
		addr, addrErr := StakeAddressFromCredential(
			row.StakingKey,
			row.CredentialTag,
		)
		if addrErr != nil {
			logger.Warn(
				"koiosparity: failed to decode reward_account_output credential",
				"epoch",
				stakeEpoch,
				"error",
				addrErr,
			)
			out = append(out, CheckMismatch{
				Network:    network,
				Epoch:      epoch,
				Field:      "account_reward_address_decode",
				DingoValue: fmt.Sprintf("error: %v", addrErr),
				KoiosValue: "",
				Category:   CategoryDBError,
				CheckedAt:  now,
			})
			continue
		}
		dingoRows = append(dingoRows, DingoAccountReward{
			StakeAddress: addr,
			RewardType:   row.RewardType,
			Amount:       strconv.FormatUint(uint64(row.Amount), 10),
		})
	}

	out = append(
		out,
		CompareAccountEpoch(
			network,
			epoch,
			koiosRows,
			dingoRows,
			now,
			graceHours,
			epochEndTime,
		)...,
	)
	out = append(
		out,
		accountLifecycleMismatches(
			ctx, cache, dingo, network, epoch, stakeEpoch, dingoOutputs, now,
		)...,
	)
	return out
}

// accountLifecycleMismatches (dingo #3099) reports the two account
// dimensions #3097's CompareAccountEpoch structurally cannot: confirmed
// zero-reward accounts and newly-registered/deregistered accounts between
// adjacent stake epochs. Purely informational: every mismatch returned here
// uses one of CategoryAcctZeroReward/CategoryAcctNewlyRegistered/
// CategoryAcctDeregistered, which DetermineStatus treats as a no-op, never
// FAIL or ERROR.
//
// A genuine cache/DB error from either lookup is reported as CategoryDBError
// rather than silently swallowed — only an explicitly absent/incomplete
// previous-epoch signal is treated as "disable this report", never any
// other error, so a real storage failure can never masquerade as "nothing
// to report".
//
// maxAccountLifecycleSample bounds how many addresses are embedded, as a
// comma-joined debugging sample, in each aggregate lifecycle mismatch row —
// see aggregateAccountLifecycleMismatch's doc comment for why these are
// summarized rather than reported one row per address.
const maxAccountLifecycleSample = 20

func accountLifecycleMismatches(
	ctx context.Context,
	cache *Cache,
	dingo RewardParitySource,
	network string,
	epoch, stakeEpoch uint64,
	currentOutputs []*models.RewardAccountOutput,
	now time.Time,
) []CheckMismatch {
	var out []CheckMismatch

	zeroReward, err := cache.GetZeroRewardAccountsForEpoch(network, epoch)
	if err != nil {
		return append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "koios_account_checked",
			KoiosValue: fmt.Sprintf("error: %v", err),
			Category:   CategoryDBError,
			CheckedAt:  now,
		})
	}
	if len(zeroReward) > 0 {
		out = append(out, aggregateAccountLifecycleMismatch(
			network,
			epoch,
			CategoryAcctZeroReward,
			"account_zero_reward",
			zeroReward,
			now,
		))
	}

	// Newly-registered/deregistered accounts are diffed from Dingo's own
	// epoch-scoped reward_account_output universe — which stake addresses
	// Dingo actually computed a reward output for at this specific stake
	// epoch — not from koios_account_checked's persisted "requested
	// universe". That set unions in Koios's own all-time historical account
	// list (BuildAccountAddressUniverse), resolved once per Fetch run and
	// reused across every epoch, so it stays essentially static regardless
	// of whether an account is genuinely active/delegated in any given
	// epoch: diffing against it would make this report reflect almost
	// nothing for a Koios-known account (it never leaves the union) rather
	// than genuine epoch-over-epoch registration/deregistration.
	if stakeEpoch == 0 || dingo == nil {
		// No valid previous stake epoch to diff against (stakeEpoch-1 would
		// underflow) — should not be reachable in practice, since checkEpoch
		// already excludes pre-staking epochs via koiosStakeEpoch's own
		// guard before ever calling compareEpochAccounts. The dingo == nil
		// check is defensive (compareEpochAccounts always passes a real
		// source in production) and lets callers that only care about the
		// zero-reward half of this report (see e.g. this package's own
		// tests) pass nil rather than a throwaway source implementation.
		return out
	}
	if _, prunable := dingo.(*DatabaseSource); prunable {
		// DatabaseSource (the in-process observer's reward source) reads
		// reward_account_output through core-mode's rolling pruning window
		// (see DatabaseSource.GetRewardAccountOutputs) and cannot
		// distinguish "the previous stake epoch genuinely had no reward
		// accounts" from "its rows have since been pruned" — an empty,
		// error-free result either way. Treating a pruned-empty result as a
		// complete previous-epoch universe would make every account in the
		// current epoch look newly registered. Only *DingoDB (the
		// standalone CLI's full, unpruned copy) can be trusted for this
		// diff; skip it entirely for a prunable source rather than report a
		// false lifecycle signal. Zero-reward reporting above is unaffected
		// — it doesn't depend on any historical epoch's data.
		return out
	}
	previousOutputs, err := dingo.GetRewardAccountOutputs(ctx, stakeEpoch-1)
	if err != nil {
		return append(out, CheckMismatch{
			Network:    network,
			Epoch:      epoch,
			Field:      "reward_account_output",
			DingoValue: fmt.Sprintf("error: %v", err),
			Category:   CategoryDBError,
			CheckedAt:  now,
		})
	}

	// currentOutputs' own decode failures are already reported by
	// compareEpochAccounts's own dingoRows-building loop above (the same
	// underlying rows) — only previousOutputs has no other reporting path,
	// since nothing else in this package ever decodes the previous stake
	// epoch's addresses.
	prevSet, prevDecodeErrs := dingoRewardAddressSet(previousOutputs)
	currSet, currDecodeErrs := dingoRewardAddressSet(currentOutputs)
	if prevDecodeErrs > 0 {
		out = append(out, CheckMismatch{
			Network: network,
			Epoch:   epoch,
			Field:   "reward_account_output_address_decode",
			DingoValue: fmt.Sprintf(
				"%d previous-epoch reward_account_output row(s) failed to decode",
				prevDecodeErrs,
			),
			Category:  CategoryDBError,
			CheckedAt: now,
		})
	}
	if prevDecodeErrs > 0 || currDecodeErrs > 0 {
		// Either side's set is now missing an address purely because a row
		// failed to decode, not because it actually registered/deregistered
		// — diffing incomplete sets would misreport that row's address as
		// newly-registered or deregistered. The decode failure itself is
		// already reported (previous-epoch above; current-epoch by
		// compareEpochAccounts's own dingoRows-building loop), so skip the
		// diff rather than emit a misleading one.
		return out
	}

	var newlyRegistered, deregistered []string
	for addr := range currSet {
		if !prevSet[addr] {
			newlyRegistered = append(newlyRegistered, addr)
		}
	}
	for addr := range prevSet {
		if !currSet[addr] {
			deregistered = append(deregistered, addr)
		}
	}
	sort.Strings(newlyRegistered)
	sort.Strings(deregistered)

	if len(newlyRegistered) > 0 {
		out = append(out, aggregateAccountLifecycleMismatch(
			network,
			epoch,
			CategoryAcctNewlyRegistered,
			"account_lifecycle_newly_registered",
			newlyRegistered,
			now,
		))
	}
	if len(deregistered) > 0 {
		out = append(out, aggregateAccountLifecycleMismatch(
			network,
			epoch,
			CategoryAcctDeregistered,
			"account_lifecycle_deregistered",
			deregistered,
			now,
		))
	}
	return out
}

// dingoRewardAddressSet decodes a set of RewardAccountOutput rows into a
// deduplicated bech32 stake-address set, mirroring
// BuildAccountAddressUniverse's own dedup approach. A row with an
// unsupported credential tag is skipped from the set (rather than aborting
// the whole diff) but counted in decodeErrs — silently dropping such a row
// would make that address look deregistered (if it was in the previous
// epoch's set) or simply never newly-registered (if it's in the current
// epoch's set), which is misleading, not a real lifecycle change. The
// caller is responsible for reporting decodeErrs > 0 as a CategoryDBError
// mismatch when there's no other reporting path for this specific input
// (see accountLifecycleMismatches's call site).
func dingoRewardAddressSet(
	outputs []*models.RewardAccountOutput,
) (set map[string]bool, decodeErrs int) {
	set = make(map[string]bool, len(outputs))
	for _, o := range outputs {
		addr, err := StakeAddressFromCredential(o.StakingKey, o.CredentialTag)
		if err != nil {
			decodeErrs++
			continue
		}
		set[addr] = true
	}
	return set, decodeErrs
}

// aggregateAccountLifecycleMismatch builds one summary CheckMismatch row for
// an entire category of addresses, rather than one row per address.
// Zero-reward accounts in particular can be the large majority of a
// network's whole address universe (Koios never emits a row at all for a
// zero-reward account), so reporting them one-row-per-address would scale
// insert time, cache size, and JSON report memory with the size of the
// account universe, and would drown out genuine mismatches in
// CheckEpochStatus.MismatchCount. KoiosValue carries the total affected
// count; DingoValue carries a capped, comma-joined sample of addresses for
// debugging, not the full list — the persisted koios_account_checked/
// koios_account_universe data remains queryable directly for anyone who
// needs the complete list.
func aggregateAccountLifecycleMismatch(
	network string,
	epoch uint64,
	category, field string,
	addrs []string,
	now time.Time,
) CheckMismatch {
	sample := addrs
	if len(sample) > maxAccountLifecycleSample {
		sample = sample[:maxAccountLifecycleSample]
	}
	return CheckMismatch{
		Network:    network,
		Epoch:      epoch,
		Field:      field,
		DingoValue: "sample: " + strings.Join(sample, ","),
		KoiosValue: strconv.Itoa(len(addrs)),
		Category:   category,
		CheckedAt:  now,
	}
}
