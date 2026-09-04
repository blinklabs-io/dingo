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

package ledger

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// PoolStakeShare is one pool's entry in the active stake distribution.
type PoolStakeShare struct {
	PoolKeyHash lcommon.PoolKeyHash
	// Stake is this pool's stake in the snapshot, in lovelace.
	Stake uint64
	// StakeFraction is Stake over PoolStakeDistribution.TotalActiveStake. It
	// is a share of the whole snapshot even when a filter was applied, so a
	// filtered distribution's fractions sum to less than one.
	StakeFraction *cbor.Rat
	// VrfKeyHash is the key block validation will hold this pool to.
	VrfKeyHash ledger.Blake2b256
}

// PoolStakeDistribution is the stake distribution across block-producing pools
// as of SnapshotEpoch.
type PoolStakeDistribution struct {
	// Tip is the chain tip read inside the same transaction as the stake rows.
	// It is what the distribution was evaluated against, so a caller reporting
	// a "the state as of" point must use this rather than sampling the tip
	// again afterwards: between the two reads the chain can advance, and across
	// an epoch boundary the later tip names an epoch whose snapshot is not the
	// one these rows came from.
	Tip ochainsync.Tip
	// SnapshotEpoch names the mark snapshot the distribution was read from,
	// which is the snapshot this node elects leaders from rather than live
	// stake.
	SnapshotEpoch uint64
	// TotalActiveStake is the whole snapshot's total, clamped to a minimum of
	// one. The clamp is inherited from totalActiveStake, where it exists
	// because cardano clients decode this as a NonZero value; see its doc
	// comment. It is the denominator every StakeFraction is taken over.
	TotalActiveStake uint64
	// TotalCirculatingSupply is genesis MaxLovelaceSupply minus the live
	// reserves pot, clamped the same way as TotalActiveStake. It is a
	// different total from TotalActiveStake -- see totalCirculatingSupply's
	// doc comment (blinklabs-io/dingo#3824) for why GetStakeDistribution, and
	// only GetStakeDistribution, needs this one instead.
	TotalCirculatingSupply uint64
	// Pools is ordered by PoolKeyHash. Callers that place this in a repeated
	// protobuf field or any other ordered encoding depend on that: without it
	// the order is Go map iteration order, so two identical requests against
	// an unchanged snapshot would differ.
	Pools []PoolStakeShare
}

// PoolStakeDistribution reads the active stake distribution across
// block-producing pools.
//
// The distribution comes from the mark snapshot at praos.StakeSnapshotEpoch,
// the same one leader election reads, rather than from live stake. A caller
// checking a leadership schedule against the node would otherwise be told they
// lead slots the node will not let them mint.
//
// poolFilter restricts which pools are reported. A nil filter reports every
// pool in the snapshot. A non-nil filter reports only the pools it names, which
// for an empty non-nil filter is no pools at all -- the distinction matters
// because GetPoolDistr2's wire form can carry an explicit empty set, which
// means "no pools" rather than "every pool" (see
// olocalstatequery.ShelleyPoolDistr2Query.PoolFilter). Filtering never
// renormalises: TotalActiveStake stays the whole snapshot's total and each
// fraction stays a share of it.
//
// A pool holding snapshot stake with no registration on record is omitted
// rather than reported with a zero VRF key hash, which would read as a real
// key. Its stake stays in TotalActiveStake, so every reported pool's own
// fraction is unaffected by the omission.
func (ls *LedgerState) PoolStakeDistribution(
	poolFilter []lcommon.PoolKeyHash,
) (*PoolStakeDistribution, error) {
	// The per-pool stakes, their total, and the epoch naming the snapshot they
	// come from all have to come from one view: read separately, an epoch
	// boundary landing in between would produce fractions that do not sum to
	// one, or a distribution belonging to an epoch other than the one this
	// query resolved.
	txn := ls.db.Transaction(false)
	defer txn.Release()
	metaTxn := txn.Metadata()

	// The epoch comes from the tip inside this transaction rather than from the
	// in-memory consensus snapshot; see epochAtTip for why. A chain that has
	// applied no blocks has no epoch record covering its tip, and epoch zero is
	// the right answer there.
	tip, current, err := ls.epochAtTip(txn)
	if err != nil {
		return nil, err
	}
	var epoch uint64
	if current != nil {
		epoch = current.EpochId
	}
	snapshotEpoch := praos.StakeSnapshotEpoch(epoch)

	// A filter is bounded by the caller, so only the pools it names are read;
	// loading the whole snapshot to discard most of it is work the request did
	// not ask for. This is the same split queryShelleyStakeSnapshots makes, and
	// for the same reason -- the unfiltered path, which is the one
	// cardano-cli's leadership schedule takes, still gets the single bulk read
	// rather than one query per pool.
	var stakeByPool map[string]uint64
	if poolFilter == nil {
		stakeByPool, err = ls.markStakeByPool(snapshotEpoch, true, metaTxn)
	} else {
		poolIds := make([]ledger.PoolId, 0, len(poolFilter))
		for _, pkh := range poolFilter {
			poolIds = append(poolIds, ledger.PoolId(pkh))
		}
		stakeByPool, err = ls.markStakeForPools(
			snapshotEpoch,
			poolIds,
			metaTxn,
		)
	}
	if err != nil {
		return nil, err
	}

	// Via the shared helper rather than the store directly: it clamps a zero
	// total up to one, which matters because the node-to-client reply types
	// this field as a NonZero Coin and cardano-cli's decoder rejects zero
	// outright. A chain whose first snapshot has not been taken yet would
	// otherwise produce a reply the caller cannot decode at all.
	//
	// Note this total does not come from the rows read above. For a mark
	// snapshot GetTotalActiveStake prefers epoch_summary.total_active_stake
	// when that row is marked ready, falling back to summing the snapshot rows
	// only when it is not -- so the fractions below are a ratio across two
	// tables, and one transaction makes that pair consistent rather than equal.
	//
	// They are equal by construction: snapshot rotation writes both in the same
	// transaction from one calculation, the summary's total being the running
	// sum of the very PoolStakes the rows are built from. The one thing that
	// separates them is cleanupOldSnapshots, which prunes snapshot rows below
	// currentEpoch-3 while deliberately retaining every epoch_summary row --
	// and snapshotEpoch here is praos.StakeSnapshotEpoch, currentEpoch-1, which
	// is always inside that retained window. Moving either bound breaks the
	// equality, which is what
	// TestQueryShelleyPoolDistr2_TotalMatchesRowsWhenSummaryIsReady is for.
	totalActiveStake, err := ls.totalActiveStake(snapshotEpoch, true, metaTxn)
	if err != nil {
		return nil, err
	}
	// Read inside the same transaction as everything else here so a caller
	// combining this with per-pool Stake gets one consistent view, the same
	// reason totalActiveStake is read from metaTxn rather than a fresh one.
	totalCirculatingSupply, err := ls.totalCirculatingSupply(
		snapshotEpoch,
		true,
		metaTxn,
	)
	if err != nil {
		return nil, err
	}

	keyHashes := make([]lcommon.PoolKeyHash, 0, len(stakeByPool))
	for hash := range stakeByPool {
		keyHashes = append(
			keyHashes,
			lcommon.PoolKeyHash(lcommon.NewBlake2b224([]byte(hash))),
		)
	}
	// Sorted before the VRF lookup so both the reported order and the
	// omission warnings below are a function of the snapshot alone rather than
	// of Go's randomised map iteration.
	slices.SortFunc(keyHashes, func(a, b lcommon.PoolKeyHash) int {
		return bytes.Compare(a.Bytes(), b.Bytes())
	})

	// The VRF key hash lives on the pool registration rather than the
	// snapshot, so it is fetched in bulk rather than per pool.
	vrfByPool, err := ls.poolVrfKeyHashes(keyHashes, metaTxn)
	if err != nil {
		return nil, err
	}

	dist := &PoolStakeDistribution{
		Tip:                    tip,
		SnapshotEpoch:          snapshotEpoch,
		TotalActiveStake:       totalActiveStake,
		TotalCirculatingSupply: totalCirculatingSupply,
		Pools:                  make([]PoolStakeShare, 0, len(keyHashes)),
	}
	for _, pkh := range keyHashes {
		stake := stakeByPool[string(pkh.Bytes())]
		vrf, ok := vrfByPool[pkh]
		if !ok {
			// A pool holding snapshot stake with no registration on record
			// cannot be given a VRF key hash, and reporting it with a zero one
			// is no better, since that reads as a real key. So it is left out
			// and the omission is logged rather than inferred.
			//
			// Omitting it costs less than it appears to. Its stake stays in
			// TotalActiveStake, so the reported fractions sum to slightly less
			// than one -- but a caller computing a leadership schedule checks
			// its OWN fraction, which is its stake over that same unchanged
			// total and so is unaffected by another pool being absent.
			//
			// Failing instead would cost far more. An error here does not fail
			// one query: it aborts the LocalStateQuery protocol, the node drops
			// the connection, and cardano-cli reports only a closed bearer --
			// the exact opaque failure #2997 was filed for. Worse, the
			// unfiltered form of this query covers every pool in the snapshot,
			// so one unregistered pool anywhere on the chain would break
			// leadership-schedule for every operator rather than for the one
			// pool concerned. The same reasoning keeps chainDepStateLabNonce
			// serving a slightly stale value instead of aborting.
			ls.config.Logger.Warn(
				"omitting pool with snapshot stake but no registration",
				"pool", hex.EncodeToString(pkh.Bytes()),
				"stake", stake,
				"epoch", snapshotEpoch,
				"component", "ledger",
			)
			continue
		}
		dist.Pools = append(dist.Pools, PoolStakeShare{
			PoolKeyHash:   pkh,
			Stake:         stake,
			StakeFraction: stakeFraction(stake, totalActiveStake),
			VrfKeyHash:    vrf,
		})
	}
	return dist, nil
}

// stakeFraction expresses a pool's share of the active stake. A snapshot with
// no stake at all — which is the state a chain is in before its first snapshot
// is taken — yields zero rather than dividing by it.
func stakeFraction(stake, total uint64) *cbor.Rat {
	if total == 0 {
		return &cbor.Rat{Rat: big.NewRat(0, 1)}
	}
	return &cbor.Rat{
		Rat: new(big.Rat).SetFrac(
			new(big.Int).SetUint64(stake),
			new(big.Int).SetUint64(total),
		),
	}
}

// poolVrfKeyHashes looks up the VRF key hash each pool will be held to.
//
// Resolution goes through registeredPoolVrfKeyHash, the same function
// verifyRegisteredVrfKey uses to decide whether an incoming block's VRF key
// belongs to the pool that produced it. That is not an incidental reuse: a
// leadership schedule is only worth anything if the node that produced it will
// accept the blocks it promises, so the key this reply names has to be the key
// block validation will require. Sharing the function means the two cannot
// drift apart silently.
//
// It also settles which registration wins when a pool has re-registered with a
// new VRF key. The Haskell node answers this query from a stake distribution
// snapshotted at the epoch boundary, so it reports the key that was in force
// then. dingo resolves the producing pool's key live at validation time, so
// reporting the snapshot-era key here would describe a schedule dingo itself
// would reject. Matching the validator is what keeps the reply true of the
// node serving it.
func (ls *LedgerState) poolVrfKeyHashes(
	keyHashes []lcommon.PoolKeyHash,
	txn types.Txn,
) (map[lcommon.PoolKeyHash]ledger.Blake2b256, error) {
	out := make(map[lcommon.PoolKeyHash]ledger.Blake2b256, len(keyHashes))
	if len(keyHashes) == 0 {
		return out, nil
	}
	pools, err := ls.db.Metadata().GetPools(keyHashes, txn)
	if err != nil {
		return nil, err
	}
	for _, pool := range pools {
		vrfKeyHash, ok := registeredPoolVrfKeyHash(&pool)
		if !ok {
			continue
		}
		pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool.PoolKeyHash))
		out[pkh] = ledger.Blake2b256(vrfKeyHash)
	}
	return out, nil
}
