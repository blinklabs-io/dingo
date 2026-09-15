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
// mode does: Dingo's Acquire-time validation (VerifyPointQueryable,
// blinklabs-io/dingo#382) rejects a point if ANY point-aware query type's
// own retention floor has passed it, not only the one the caller actually
// intends to ask. UTxO's retention floor is by far the tightest of the
// three during a from-genesis replay, so a shared Acquire needlessly cut
// protocol-params and stake off at UTxO's window instead of their own,
// much longer ones (protocol params are effectively unbounded; stake is
// capped at 3 epochs behind Dingo's own live epoch, unrelated to UTxO's
// floor) -- confirmed live switching to separate Acquire calls let
// protocol-params/stake keep succeeding for 20+ consecutive epochs where
// they previously stopped at the first one UTxO's floor rejected.
//
// Running Dingo with --storage-mode api removes the UTxO half of that
// exposure entirely (checkUtxoRetentionWindow already skips its own
// retention check in that mode, the same way cleanupConsumedUtxos does),
// and is required for the stake comparison to reach more than a handful of
// epochs too: pool-stake snapshot pruning (ledger/snapshot/rotation.go's
// cleanupOldSnapshots) now also retains without bound in API mode,
// mirroring UTxO's own existing carve-out -- confirmed live, 24
// consecutive epochs with zero Acquire failures on either connection type,
// where CORE mode reliably started failing by epoch 9.

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/ledger/eras"
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
func CheckProtocolParams(
	ctx context.Context,
	client *localstatequery.Client,
	koios *koiosparity.KoiosClient,
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
	if eraID, eraErr := client.GetCurrentEra(); eraErr == nil {
		if era := eras.GetEraById(uint(eraID)); era != nil {
			dingoParams.EraID = uint(eraID)
			dingoParams.EraName = era.Name
		}
	}

	koiosResp, koiosErr := koios.GetEpochParams(ctx, epoch)
	var koiosParams *koiosparity.KoiosEpochParams
	if koiosErr == nil {
		row := koiosparity.EpochParamsFromKoios(network, epoch, koiosResp, time.Now().UTC())
		koiosParams = &row
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
	KoiosStake   string // Koios's literal decimal string, kept exact
	RelDiff      float64
}

// stakeRelativeTolerance bounds how far Dingo's own reported per-pool active
// stake (GetPoolDistr2's TotalPoolStake) may differ, relative to Koios's
// independently-reported pool_history active_stake for the same pool and
// epoch, before it counts as a real mismatch rather than rounding noise
// between two independent computations over the same underlying integers.
const stakeRelativeTolerance = 0.0005

// stakeRelDiff computes how far dingoStake differs from koiosStakeStr, as a
// fraction of koiosStake -- or of dingoStake when both are zero, so two
// independently-reported zero-stake pools still compare as an exact match
// rather than an undefined 0/0. ok is false only when koiosStakeStr fails to
// parse as a number.
func stakeRelDiff(dingoStake uint64, koiosStakeStr string) (relDiff float64, ok bool) {
	koiosFloat, parsed := new(big.Float).SetString(koiosStakeStr)
	if !parsed {
		return 0, false
	}
	dingoFloat := new(big.Float).SetUint64(dingoStake)
	denom := koiosFloat
	if denom.Sign() == 0 {
		denom = dingoFloat
	}
	if denom.Sign() == 0 {
		return 0, true
	}
	diff := new(big.Float).Sub(dingoFloat, koiosFloat)
	diff.Abs(diff)
	relDiff, _ = new(big.Float).Quo(diff, denom).Float64()
	return relDiff, true
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

// CheckStakeDistribution compares Dingo's own pool-by-pool active stake
// (queried live via client, already Acquired to the point under test)
// against Koios's /pool_history for epoch, for every pool Dingo itself
// reports -- see this file's doc comment for why this iterates Dingo's own
// (small) pool set rather than Koios's full historical pool_list, and why
// VRF keys are not compared. The per-pool /pool_history calls run with
// bounded concurrency (stakeCheckConcurrency) rather than sequentially --
// see that constant's doc comment.
func CheckStakeDistribution(
	ctx context.Context,
	client *localstatequery.Client,
	koios *koiosparity.KoiosClient,
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

	results := make([]*StakeMismatch, len(pools))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(stakeCheckConcurrency)
	for i, p := range pools {
		g.Go(func() error {
			hist, err := koios.GetPoolEpochHistory(gctx, p.bech32, epoch)
			if err != nil {
				return fmt.Errorf(
					"koios pool_history for %s epoch %d: %w",
					p.bech32, epoch, err,
				)
			}
			if hist == nil {
				// No Koios row for this pool/epoch at all -- e.g. a pool
				// that only just registered this epoch and has no
				// snapshot yet. Not a mismatch: nothing to compare
				// against.
				return nil
			}
			relDiff, ok := stakeRelDiff(p.stake, hist.ActiveStake)
			if !ok {
				return nil
			}
			if relDiff > stakeRelativeTolerance {
				results[i] = &StakeMismatch{
					PoolIDBech32: p.bech32,
					DingoStake:   p.stake,
					KoiosStake:   hist.ActiveStake,
					RelDiff:      relDiff,
				}
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

// UTxOSet is a live UTxO set mapping ref ("<txHash>#<outputIndex>") to its
// canonical content encoding: koiosparity.CanonicalKoiosUTxOEntry for a ref
// tracked from Koios's own /tx_info data, or canonicalUTxOEntry (this
// package's own, shared with the cardano-node-based Check/DiffSnapshots)
// for a ref read directly from Dingo. The two encoders are built to
// produce byte-identical strings for the same real-world UTxO content, so
// UTxODiff can compare them directly -- full content, not just existence.
type UTxOSet map[string]string

// UTxOChanges applies one block's transactions to a running UTxOSet using
// Koios's own reported inputs/outputs for them (via txInfos, typically
// fetched through koiosparity.KoiosClient.GetTxInfos) -- not Dingo's own
// decode of the same block -- so the running reconstruction stays
// independent of Dingo end to end, not just at its genesis seed.
func UTxOChanges(set UTxOSet, txInfos []koiosparity.KoiosTxInfoItem) {
	for _, info := range txInfos {
		for _, in := range info.Inputs {
			delete(set, fmt.Sprintf("%s#%d", in.TxHash, in.TxIndex))
		}
		for _, out := range info.Outputs {
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
