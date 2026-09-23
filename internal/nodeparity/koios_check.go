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

package nodeparity

// Koios-backed comparison (blinklabs-io/dingo#1900): validates a Dingo
// instance replaying from genesis against Koios instead of a reference
// cardano-node, epoch by epoch.
//
// This exists because a real cardano-node cannot fill the reference role
// for a from-genesis replay at all: cardano-node's own replay races ahead
// of a freshly-started Dingo fast enough that there is never a matching
// historical block left to Acquire against by the time Dingo reaches it
// (confirmed live: cardano-node was already tens of thousands of blocks
// ahead of Dingo at the very first epoch boundary, in a 15-minute run).
// Koios has no such problem: it retains full per-epoch history
// indefinitely, so it can answer for epoch 1 exactly as readily as epoch
// 100 regardless of when Dingo asks.
//
// Restricted to preview/preprod, matching koios-parity's own restriction
// (koios_client.go's validateKoiosNetwork) and this tool's own existing
// --network validation: Koios only ever serves these two networks, and a
// from-genesis UTxO-set reconstruction (CheckUTxO below) walks every
// transaction one at a time, which is only tractable at these networks'
// scale. It is not a substitute for the existing cardano-node-based Check
// at mainnet scale -- see this package's doc comment for when to use which.
//
// This is intentionally narrower than DiffSnapshots/Diff (check.go,
// diff.go), which assume a full-fidelity Snapshot from a real
// cardano-node-equivalent LocalStateQuery server on both sides:
//
//   - Protocol parameters: full field-level comparison, reusing
//     koios-parity's own already-verified CompareEpochProtocolParams
//     (internal/koiosparity/compare.go) rather than a second, narrower
//     copy -- this covers every field koios-parity itself has verified
//     safe to compare, with its own documented exclusions (governance
//     parameters, coins_per_utxo_size on Alonzo, etc.) applying here too.
//   - Stake distribution: pool-by-pool active stake, as an absolute
//     lovelace amount (GetPoolDistr2's TotalPoolStake vs Koios's
//     pool_history active_stake) rather than a fraction -- avoids
//     reconstructing a fraction from two different Koios endpoints, and
//     directly reuses what GetPoolDistr2 already reports. Does NOT compare
//     registered VRF keys (unlike DiffSnapshots' StakeDistributionEntry) --
//     Koios's pool_history does not return one; comparing it would need a
//     separate pool_info call per pool per epoch, deferred as follow-up
//     scope.
//
//     KNOWN GAP: this only iterates the
//     pools Dingo itself reports via GetPoolDistr2, so it can detect a
//     pool whose Dingo-reported stake disagrees with Koios, but not a pool
//     Dingo's ledger state is missing entirely (a real bug that would look
//     identical to "this pool just isn't active yet" from here). Closing
//     this gap properly needs a per-epoch source of Koios's own active-pool
//     set to compare Dingo's list against -- Koios has no bulk endpoint for
//     that (pool_history is single-pool only; pool_list returns every
//     pool ever registered, historically 1000+ on preview alone, with only
//     a live/current active_stake, not a historical one). Iterating that
//     full list per epoch would reintroduce, one level up the call stack,
//     the exact sequential-Koios-call cost this file's own concurrency
//     work (stakeCheckConcurrency) was written to eliminate, and using
//     pool_list's current registration status as a stand-in for a
//     historical epoch's membership would be actively wrong for any epoch
//     not near Koios's live tip -- worse than the documented gap it would
//     replace. Deferred rather than rushed, as follow-up scope.
//   - UTxO set: full content (address, ADA amount, multi-asset tokens,
//     datum presence/form, reference script hash), not just existence --
//     built from Koios's own /tx_info input/output data via a
//     from-genesis reconstruction, seeded from Dingo's own answer
//     captured at the earliest point Dingo will still Acquire.
//     koiosparity.CanonicalKoiosUTxOEntry and this package's own
//     canonicalUTxOEntry (shared with the cardano-node-based
//     Check/DiffSnapshots) are built to produce byte-identical strings for
//     the same real-world UTxO content, so UTxODiff compares them
//     directly. Not yet live-verified against real Koios data at the time
//     this was written (both Koios sources' rate limits were exhausted
//     from same-day testing) -- verified only against the documented
//     /tx_info response schema and this package's own unit tests; treat
//     the first live run as the real confirmation.
//
// Each of the three runs its own independent Acquire, on its own
// connection, rather than sharing one the way Check's live-tip-agreement
// mode does. #4320 added Acquire-time validation (VerifyPointQueryable) that
// rejects a point up front if ANY point-aware query type's own retention
// floor has passed it, not only the one the caller actually intends to ask,
// so a shared Acquire would cut protocol params and stake off at the UTxO
// query's much tighter floor instead of their own, much longer ones
// (protocol params are effectively unbounded; stake is capped at 3 epochs
// behind Dingo's own live epoch). Separate Acquire calls keep each check
// bounded by its own floor.
//
// Running Dingo with --storage-mode api removes the UTxO half of that
// exposure entirely (checkUtxoRetentionWindow skips its own retention check
// in that mode, the same way cleanupConsumedUtxos does). #4320 added the
// equivalent carve-out to pool-stake snapshot pruning
// (ledger/snapshot/rotation.go's cleanupOldSnapshots), so in API mode the
// stake comparison is no longer bounded by that window either.

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"golang.org/x/sync/errgroup"
)

// KoiosNetworks are the only networks Koios's own client, and this
// comparison, ever supports -- see this file's doc comment.
var KoiosNetworks = map[string]bool{"preview": true, "preprod": true}

// NewKoiosClient constructs a Koios client for network, validating it is
// preview or preprod first so an unsupported network (in particular
// "mainnet", where this comparison would not make sense at all -- see this
// file's doc comment) fails immediately with a clear error rather than
// reaching koiosparity.NewKoiosClient's own, differently-worded rejection.
func NewKoiosClient(
	network, apiKey, baseURL string,
	allowInsecureHTTP bool,
) (*koiosparity.KoiosClient, error) {
	if !KoiosNetworks[network] {
		return nil, fmt.Errorf(
			"koios-backed comparison only supports preview or preprod, got %q",
			network,
		)
	}
	return koiosparity.NewKoiosClient(network, apiKey, baseURL, allowInsecureHTTP)
}

// CheckProtocolParams compares Dingo's own current protocol parameters
// (queried live via client, already Acquired to the point under test) against
// Koios's /epoch_params for epoch, returning every field-level disagreement
// CompareEpochProtocolParams finds. An empty, non-nil slice means a clean
// comparison ran and found no disagreement; a nil slice paired with a non-nil
// error means the comparison itself could not run.
// applyResolvedEra applies an already-resolved HardForkCurrentEraQuery
// result to dingoParams, or fails outright if that resolution itself
// failed. Split out of CheckProtocolParams so the regression this guards
// against -- a GetCurrentEra error being silently swallowed in favor of an
// ambiguous type-inferred era guess -- is provable with plain values, not a
// live wire round-trip: exercising it through the real
// localstatequery.Client would make the test depend on gouroboros's exact
// number of HardForkCurrentEraQuery calls per CheckProtocolParams
// invocation, which is an internal timing detail of a third-party client,
// not part of this package's contract.
func applyResolvedEra(
	dingoParams *koiosparity.DingoProtocolParams,
	eraID int,
	eraErr error,
) error {
	if eraErr != nil {
		return fmt.Errorf("dingo current era query: %w", eraErr)
	}
	if era := eras.GetEraById(uint(eraID)); era != nil {
		dingoParams.EraID = uint(eraID)
		dingoParams.EraName = era.Name
	}
	return nil
}

// protocolParamsClient is the subset of *localstatequery.Client's exported
// methods CheckProtocolParams calls. A real *localstatequery.Client only
// ever resolves its era once per connection and caches it on success
// (gouroboros's Client.getCurrentEra), so on a real client
// GetCurrentProtocolParams's own internal era lookup and CheckProtocolParams's
// later explicit GetCurrentEra call below can never disagree: whichever
// runs first either fails the whole query (GetCurrentProtocolParams's own
// internal call, which if it fails means GetCurrentProtocolParams itself
// returns an error before the explicit call is ever reached) or succeeds
// and caches, making every later call on that same client a guaranteed
// cache hit. That makes the explicit call's own error branch unreachable
// through any real wire connection -- no fake LocalStateQuery server
// configuration can make GetCurrentProtocolParams succeed while a later
// GetCurrentEra on the same client fails. This interface exists so a test
// double can decouple the two outcomes anyway and pin that call site's
// error handling directly; see
// TestCheckProtocolParams_PropagatesExplicitEraQueryError.
type protocolParamsClient interface {
	GetCurrentProtocolParams() (lcommon.ProtocolParameters, error)
	GetCurrentEra() (int, error)
}

// CheckProtocolParams compares Dingo's own current protocol parameters
// (queried live via client, already Acquired to the point under test)
// against Koios's /epoch_params for epoch, returning every field-level
// disagreement CompareEpochProtocolParams finds.
//
// cache, when non-nil, is consulted before ever calling Koios: koios_epoch_params
// rows are immutable once Koios publishes them for a given (network, epoch) --
// the same assumption internal/koiosparity's own observer already relies on
// (UpsertEpochParams's doc comment) -- so a cache hit is trusted outright and
// this function makes zero Koios calls for that epoch. A cache miss still
// calls Koios exactly as before, and the freshly-fetched row is written back
// via UpsertEpochParams so a later call (this run, a future from-genesis run,
// or dingo's own embedded koios-parity observer sharing the same cache file)
// never re-fetches it. Every write here targets the SAME koios_epoch_params
// table dingo's embedded observer writes to concurrently -- see cache.go's
// OpenCache doc comment (WAL + busy_timeout + a single-connection pool per
// process) for why two independent processes sharing one cache.db this way is
// safe. A cache read/write error never fails the check itself: the cache is
// strictly an optimization layer over the same live Koios call this function
// already tolerates failing (koiosErr below), so a cache-only hiccup (e.g. a
// lock wait timeout while dingo's observer holds the writer slot) degrades to
// "fetch live," not to a hard error.
func CheckProtocolParams(
	ctx context.Context,
	client protocolParamsClient,
	koios *koiosparity.KoiosClient,
	cache *koiosparity.Cache,
	network string,
	epoch uint64,
) ([]koiosparity.CheckMismatch, error) {
	pp, err := client.GetCurrentProtocolParams()
	if err != nil {
		return nil, fmt.Errorf("dingo protocol params query: %w", err)
	}
	dingoParams, err := koiosparity.ProtocolParamsFromNative(pp)
	if err != nil {
		return nil, fmt.Errorf("convert dingo protocol params: %w", err)
	}

	// ProtocolParamsFromNative infers the era from pp's own Go type, which
	// is ambiguous for exactly one case: allegra.AllegraProtocolParameters
	// is a type alias for shelley.ShelleyProtocolParameters (gouroboros), so
	// a type switch alone cannot tell Shelley and Allegra apart. Resolve it
	// authoritatively via HardForkCurrentEraQuery instead, on the same
	// Acquired connection -- this is the same era index the node's own
	// wire protocol reports, not a second guess.
	//
	// A GetCurrentEra error must fail this whole check, not be silently
	// swallowed in favor of the ambiguous type-inferred guess:
	// queryHardFork's HardForkCurrentEraQuery case returns
	// errEpochNotResolved for a pinned point no epoch row covers, instead of
	// silently answering era 0, so this call can fail. Falling back to the
	// type-inferred guess (Shelley) on that failure would let
	// CompareEpochProtocolParams report a real pparams_era
	// CategoryValueMismatch against Koios's correct era, and DetermineStatus
	// would return StatusFail -- reporting "ledger state diverged from
	// Koios" for what was actually a failed query, not a real divergence.
	eraID, eraErr := client.GetCurrentEra()
	if err := applyResolvedEra(dingoParams, eraID, eraErr); err != nil {
		return nil, err
	}

	var koiosParams *koiosparity.KoiosEpochParams
	var koiosErr error
	if cache != nil {
		if cached, err := cache.GetEpochParams(network, epoch); err == nil {
			koiosParams = cached
		}
	}
	if koiosParams == nil {
		koiosResp, err := koios.GetEpochParams(ctx, epoch)
		koiosErr = err
		if koiosErr == nil {
			row := koiosparity.EpochParamsFromKoios(network, epoch, koiosResp, time.Now().UTC())
			koiosParams = &row
			if cache != nil {
				// Best-effort: a cache write failure (e.g. a lock wait timeout
				// against dingo's own concurrently-writing observer) must not
				// fail a check that already has a trustworthy live answer --
				// see this function's doc comment.
				_ = cache.UpsertEpochParams(row)
			}
		}
	}

	// graceHours=0, epochEndTime=zero: koios-parity's grace window exists
	// for its own DB-persisted, potentially-lagging reference data. Dingo's
	// answer here comes from a live query the caller just Acquired, not a
	// DB row that might lag behind chain progression, so no grace period
	// applies.
	return koiosparity.CompareEpochProtocolParams(
		network, epoch, koiosParams, dingoParams, koiosErr,
		time.Now().UTC(), 0, time.Time{},
	), nil
}

// StakeMismatch is one pool's active-stake disagreement between Dingo and
// Koios for an epoch, as absolute lovelace amounts.
type StakeMismatch struct {
	PoolIDBech32 string
	DingoStake   uint64
	// KoiosStake is Koios's literal decimal string, kept exact -- "" when
	// Koios has no pool_history row for this pool/epoch at all (Reason
	// explains why that itself counts as a mismatch here).
	KoiosStake string
	// DiffLovelace is the exact signed difference (Dingo - Koios), in
	// whole lovelace. Meaningless (left 0) when Reason is set: those cases
	// have no numeric Koios value to diff against.
	DiffLovelace int64
	// Reason is non-empty only for the two cases that are not a numeric
	// disagreement: "no koios row for nonzero dingo stake" or "unparseable
	// koios active_stake value". Empty for an ordinary numeric mismatch.
	Reason string
	// KoiosFault is true only for the "unparseable koios active_stake
	// value" case: a comparison Koios's own data made untrustworthy, not a
	// real Dingo/Koios disagreement -- matching koiosparity.StatusError's
	// treatment of a Koios-side fetch failure, and unlike the "no koios row
	// for nonzero dingo stake" case (KoiosFault false), which is a genuine
	// divergence. A caller counting real mismatches must exclude this case
	// the same way it already excludes a StakeErr; a caller comparing on
	// Reason's literal string instead would silently miscount if this
	// doc-comment's wording ever changed.
	KoiosFault bool
}

// stakeDiffFailureKind classifies why stakeDiffLovelace could not compute an
// exact diff -- see that function's doc comment for why the two failure
// cases must not be conflated: stakeDiffUnparseableKoios is a Koios-side
// data fault (KoiosFault: true, excluded from the mismatch count),
// stakeDiffOverflow is a genuine Dingo-side fault (a real mismatch, not
// excluded). Conflating the two would silently hide a Dingo-side bug as if
// it were unremarkable Koios noise.
type stakeDiffFailureKind int

const (
	stakeDiffOK stakeDiffFailureKind = iota
	stakeDiffUnparseableKoios
	stakeDiffOverflow
)

// stakeDiffLovelace computes the exact signed difference between dingoStake
// and koiosStakeStr, in whole lovelace -- not a relative fraction. Both
// sides report an exact integer with nothing to round: GetPoolDistr2's
// TotalPoolStake is a uint64, and Koios's pool_history active_stake is
// documented and observed as a plain decimal integer string, never a
// fractional or exponent form (unlike, say, protocol-parameter rationals
// elsewhere in this package). There is therefore no independent-computation
// rounding for a tolerance to absorb: any nonzero difference is a real
// disagreement.
//
// kind is stakeDiffOK unless one of two things fails, and the caller must
// tell them apart:
//   - koiosStakeStr fails to parse as an integer at all -- a Koios-side data
//     fault.
//   - both values parse, but their difference doesn't fit in int64.
//     Cardano's entire max supply (45 billion ADA = 4.5e16 lovelace) fits
//     comfortably inside int64's range (~9.2e18) with room to spare, so a
//     difference this large only happens if dingoStake itself is an
//     implausible value -- a genuine Dingo-side fault, not a Koios one.
func stakeDiffLovelace(
	dingoStake uint64,
	koiosStakeStr string,
) (diff int64, kind stakeDiffFailureKind) {
	koiosInt, parsed := new(big.Int).SetString(koiosStakeStr, 10)
	if !parsed {
		return 0, stakeDiffUnparseableKoios
	}
	dingoInt := new(big.Int).SetUint64(dingoStake)
	diffInt := new(big.Int).Sub(dingoInt, koiosInt)
	if !diffInt.IsInt64() {
		return 0, stakeDiffOverflow
	}
	return diffInt.Int64(), stakeDiffOK
}

// stakeCheckConcurrency bounds how many /pool_history requests
// CheckStakeDistribution has in flight at once. Confirmed live against the
// Koios mirror this comparison runs against: a single /pool_history call
// averaged ~1.5s, and issuing them one pool at a time made the
// stake-distribution check alone take ~2 minutes per epoch with preview's
// current 75 active pools -- almost entirely spent waiting on network
// round trips, not on anything CPU-bound, so bounded concurrency is a safe
// win. Kept well short of unbounded to avoid hammering a mirror another
// team is hosting for us (unlike the public host, which has its own
// documented tier limits this comparison must not trip either).
const stakeCheckConcurrency = 8

// parseFloatOrZero parses s as a float64, returning 0 for "" or an
// unparseable string -- used only for koios_pool_epoch's SaturationPct/
// EpochRos columns, which evaluatePoolStake never reads (it only reads
// ActiveStake), so a lossy or zeroed value here can never affect a
// comparison verdict.
func parseFloatOrZero(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// parseFloatPtrOrNil parses s as a float64, returning nil for "" (Koios's own
// null marker in the cached row -- see KoiosPoolEpoch's doc comment) rather
// than a zero value that would be indistinguishable from a real 0.0.
func parseFloatPtrOrNil(s string) *float64 {
	if s == "" {
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &v
}

// strPtrOrNil returns nil for "" (Koios's own null marker in the cached
// row), and a pointer to s otherwise -- the inverse of strOrEmpty
// (internal/koiosparity/fetch.go), which this mirrors so a cached row round
// -trips back into the same *string-shaped null Koios itself reports.
func strPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// evaluatePoolStake decides one pool's StakeMismatch (nil for a clean
// match), given Dingo's own stake for it and Koios's /pool_history answer
// (nil if Koios has no row for this pool/epoch at all) -- the whole
// decision CheckStakeDistribution's per-pool goroutine makes, pulled out so
// a test can drive it directly with a synthetic hist instead of needing a
// real Koios server. Testing stakeDiffLovelace alone is not enough to prove
// this function's own overflow-case handling: it never exercises the
// function that actually builds the StakeMismatch clients see, so a
// regression there could pass every existing test.
func evaluatePoolStake(
	poolBech32 string,
	dingoStake uint64,
	hist *koiosparity.KoiosPoolHistoryItem,
) *StakeMismatch {
	if hist == nil {
		if dingoStake == 0 {
			// A pool that just registered this epoch, with no active stake
			// yet and no Koios snapshot yet either -- both sides agree
			// there's nothing here.
			return nil
		}
		// Dingo reports real stake for a pool Koios has no pool_history
		// row for at all -- that disagreement itself is the mismatch, not
		// a reason to skip.
		return &StakeMismatch{
			PoolIDBech32: poolBech32,
			DingoStake:   dingoStake,
			Reason:       "no koios pool_history row for nonzero dingo stake",
		}
	}
	diff, kind := stakeDiffLovelace(dingoStake, hist.ActiveStake)
	switch kind {
	case stakeDiffOK:
		// Falls through to the ordinary numeric comparison below.
	case stakeDiffUnparseableKoios:
		return &StakeMismatch{
			PoolIDBech32: poolBech32,
			DingoStake:   dingoStake,
			KoiosStake:   hist.ActiveStake,
			Reason:       "unparseable koios active_stake value",
			KoiosFault:   true,
		}
	case stakeDiffOverflow:
		return &StakeMismatch{
			PoolIDBech32: poolBech32,
			DingoStake:   dingoStake,
			KoiosStake:   hist.ActiveStake,
			Reason: "stake difference too large to represent -- " +
				"dingo's reported stake is implausible",
		}
	}
	if diff != 0 {
		return &StakeMismatch{
			PoolIDBech32: poolBech32,
			DingoStake:   dingoStake,
			KoiosStake:   hist.ActiveStake,
			DiffLovelace: diff,
		}
	}
	return nil
}

// CheckStakeDistribution compares Dingo's own pool-by-pool active stake
// (queried live via client, already Acquired to the point under test)
// against Koios's /pool_history for epoch, for every pool Dingo itself
// reports -- see this file's doc comment for why this iterates Dingo's own
// (small) pool set rather than Koios's full historical pool_list, and why
// VRF keys are not compared. The per-pool /pool_history calls run with
// bounded concurrency (stakeCheckConcurrency) rather than sequentially --
// see that constant's doc comment.
//
// cache, when non-nil, is consulted once up front (a single
// GetAllPoolsForEpoch read for the whole epoch, not one read per pool) rather
// than per pool goroutine: koios_pool_epoch rows are immutable once Koios
// closes an epoch, the same assumption CheckProtocolParams's own cache.go
// doc comment makes for koios_epoch_params, so a cached row is trusted
// outright and that pool's /pool_history call is skipped entirely. Any pool
// this epoch's cache does not yet cover still calls Koios exactly as before,
// and the freshly-fetched row is written back via UpsertPoolEpoch for later
// reuse -- see CheckProtocolParams's doc comment for the concurrent-write
// safety argument, which applies identically here (same cache.db, same
// per-process single-connection WAL pool). A cache error (read or write)
// never fails the check: it degrades to "fetch this pool live from Koios,"
// never to a hard error.
func CheckStakeDistribution(
	ctx context.Context,
	client *localstatequery.Client,
	koios *koiosparity.KoiosClient,
	cache *koiosparity.Cache,
	network string,
	epoch uint64,
) ([]StakeMismatch, error) {
	pd, err := client.GetPoolDistr2(nil)
	if err != nil {
		return nil, fmt.Errorf("dingo stake distribution query: %w", err)
	}

	type poolStake struct {
		bech32 string
		stake  uint64
	}
	pools := make([]poolStake, 0, len(pd.Pools))
	for pid, entry := range pd.Pools {
		pools = append(pools, poolStake{pid.String(), entry.TotalPoolStake})
	}

	var cached map[string]koiosparity.KoiosPoolEpoch
	if cache != nil {
		if rows, err := cache.GetAllPoolsForEpoch(network, epoch); err == nil {
			cached = make(map[string]koiosparity.KoiosPoolEpoch, len(rows))
			for _, row := range rows {
				cached[row.PoolBech32] = row
			}
		}
	}

	results := make([]*StakeMismatch, len(pools))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(stakeCheckConcurrency)
	for i, p := range pools {
		g.Go(func() error {
			if row, ok := cached[p.bech32]; ok {
				hist := &koiosparity.KoiosPoolHistoryItem{
					EpochNo:        row.Epoch,
					ActiveStake:    row.ActiveStake,
					BlockCnt:       row.BlockCnt,
					DelegatorCnt:   row.Delegators,
					FixedCost:      row.FixedCost,
					PoolFees:       row.PoolFees,
					DelegRewards:   row.DelegRewards,
					EpochRos:       parseFloatOrZero(row.EpochRos),
					SaturationPct:  parseFloatOrZero(row.SaturationPct),
					Margin:         parseFloatPtrOrNil(row.Margin),
					ActiveStakePct: parseFloatPtrOrNil(row.ActiveStakePct),
					MemberRewards:  strPtrOrNil(row.MemberRewards),
				}
				results[i] = evaluatePoolStake(p.bech32, p.stake, hist)
				return nil
			}
			hist, err := koios.GetPoolEpochHistory(gctx, p.bech32, epoch)
			if err != nil {
				return fmt.Errorf(
					"koios pool_history for %s epoch %d: %w",
					p.bech32, epoch, err,
				)
			}
			results[i] = evaluatePoolStake(p.bech32, p.stake, hist)
			if cache != nil && hist != nil {
				// Best-effort, same rationale as CheckProtocolParams's cache
				// write: a hiccup here must not fail a check that already has
				// a trustworthy live answer.
				_ = cache.UpsertPoolEpoch(koiosparity.PoolEpochFromKoiosHistoryItem(
					network, epoch, p.bech32, hist, time.Now().UTC(),
				))
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	var mismatches []StakeMismatch
	for _, m := range results {
		if m != nil {
			mismatches = append(mismatches, *m)
		}
	}
	sort.Slice(mismatches, func(i, j int) bool {
		return mismatches[i].PoolIDBech32 < mismatches[j].PoolIDBech32
	})
	return mismatches, nil
}

// fetchTxInfosCached is the cache-first counterpart to
// koiosparity.KoiosClient.GetTxInfos, completing the set: protocol params
// (CheckProtocolParams) and stake distribution (CheckStakeDistribution)
// already consult the shared cache before Koios, and this makes the UTxO
// half do the same (blinklabs-io/dingo#1900).
//
// Unlike those two, /tx_info is a BATCH endpoint, so an all-or-nothing cache
// check would throw away almost all of the benefit: one uncached transaction
// in a chunk of KoiosTxInfoBatchSize would re-fetch the other 39 that are
// already known. This looks up the whole chunk, asks Koios for exactly the
// misses, and merges -- so a chunk that is half cached costs half a request's
// worth of hashes, and a fully cached chunk costs no request at all. The
// merged result is returned in txHashes order, preserving GetTxInfos' own
// ordering contract (a UTxO created by one transaction and spent by a later
// one in the same chunk must be applied in chain order), and a hash neither
// cached nor returned live is still an error, exactly as GetTxInfos requires.
//
// Caching historical tx_info indefinitely is safe for the same reason
// caching epoch params is: a settled transaction's inputs and outputs are
// immutable Koios-side, so there is nothing to invalidate. Writes target the
// same cache.db dingo's own embedded koios-parity observer writes to
// concurrently -- see cache.go's OpenCache doc comment for why that is safe
// -- and are best-effort: a cache read error degrades to a live fetch and a
// cache write error is ignored, because the caller already has a trustworthy
// live answer and the cache is strictly an optimisation over the call it
// already tolerates failing.
func fetchTxInfosCached(
	ctx context.Context,
	koios *koiosparity.KoiosClient,
	cache *koiosparity.Cache,
	network string,
	txHashes []string,
) ([]koiosparity.KoiosTxInfoItem, error) {
	if cache == nil || len(txHashes) == 0 {
		return koios.GetTxInfos(ctx, txHashes)
	}

	cached, err := cache.GetTxInfos(network, txHashes)
	if err != nil {
		// Best-effort read: fall back to the live path this function is
		// wrapping rather than failing a check the cache is only an
		// optimisation for.
		cached = nil
	}

	missing := make([]string, 0, len(txHashes))
	seen := make(map[string]struct{}, len(txHashes))
	for _, hash := range txHashes {
		if _, ok := cached[hash]; ok {
			continue
		}
		if _, dup := seen[hash]; dup {
			continue
		}
		seen[hash] = struct{}{}
		missing = append(missing, hash)
	}

	fetched := make(map[string]koiosparity.KoiosTxInfoItem, len(missing))
	if len(missing) > 0 {
		items, err := koios.GetTxInfos(ctx, missing)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			fetched[item.TxHash] = item
		}
		// Best-effort, same rationale as CheckProtocolParams's cache write:
		// a hiccup here must not fail a fetch that already succeeded.
		_ = cache.UpsertTxInfos(network, items, time.Now().UTC())
	}

	all := make([]koiosparity.KoiosTxInfoItem, len(txHashes))
	for i, hash := range txHashes {
		if item, ok := cached[hash]; ok {
			all[i] = item
			continue
		}
		item, ok := fetched[hash]
		if !ok {
			// Unreachable through KoiosClient.GetTxInfos, which already
			// errors on a hash it got no result for; kept so a future
			// change to either side cannot silently drop a transaction
			// from the reconstruction.
			return nil, fmt.Errorf(
				"koios /tx_info: no result for requested tx hash %s", hash,
			)
		}
		all[i] = item
	}
	return all, nil
}

// UTxOSet is a live UTxO set mapping ref ("<txHash>#<outputIndex>") to its
// canonical content encoding: koiosparity.CanonicalKoiosUTxOEntry for a ref
// tracked from Koios's own /tx_info data, or canonicalUTxOEntry (this
// package's own, shared with the cardano-node-based Check/DiffSnapshots)
// for a ref read directly from Dingo. The two encoders are built to
// produce byte-identical strings for the same real-world UTxO content, so
// UTxODiff can compare them directly -- full content, not just existence.
type UTxOSet map[string]string

// UTxOChanges applies one block's transactions to a running UTxOSet using
// Koios's own reported data for them (via txInfos, typically fetched through
// koiosparity.KoiosClient.GetTxInfos) -- not Dingo's own decode of the same
// block -- so the running reconstruction stays independent of Dingo end to
// end, not just at its genesis seed.
//
// Applied through KoiosTxInfoItem's Consumed/Produced rather than its raw
// Inputs/Outputs, which are the transaction body's request, not what the
// ledger did with it: a phase-2-invalid transaction consumes its collateral
// and produces its collateral return instead, exactly the distinction
// incremental.go's blockUtxoDelta already draws by using gouroboros'
// Transaction.Consumed()/Produced(). Applying the body of one would leave
// the reconstruction claiming refs the ledger never spent and missing the
// collateral return it did create -- a false missing/extra pair persisting
// until the next re-baseline, with Dingo's collateral handling never
// actually compared.
func UTxOChanges(set UTxOSet, txInfos []koiosparity.KoiosTxInfoItem) {
	for _, info := range txInfos {
		for _, in := range info.Consumed() {
			delete(set, fmt.Sprintf("%s#%d", in.TxHash, in.TxIndex))
		}
		for _, out := range info.Produced() {
			key := fmt.Sprintf("%s#%d", out.TxHash, out.TxIndex)
			set[key] = koiosparity.CanonicalKoiosUTxOEntry(out)
		}
	}
}

// UTxODiff reports every divergence between want (the Koios-derived
// reconstruction) and got (Dingo's own live answer): refs present in want
// but missing from got, refs present in got but missing from want, and
// refs present in both whose canonical content disagrees -- all sorted for
// deterministic output. A ref in both diffs's differs slice is the sharper
// class of bug: both sides agree it exists, but disagree on what it
// actually contains.
func UTxODiff(want, got UTxOSet) (missing, extra, differs []string) {
	for k, wantVal := range want {
		gotVal, ok := got[k]
		switch {
		case !ok:
			missing = append(missing, k)
		case wantVal != gotVal:
			differs = append(differs, fmt.Sprintf(
				"%s: koios=%q dingo=%q", k, wantVal, gotVal,
			))
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			extra = append(extra, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	sort.Strings(differs)
	return missing, extra, differs
}
