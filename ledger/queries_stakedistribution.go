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
	"context"
	"encoding/hex"
	"errors"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// stakeDistributionEntry mirrors the anonymous struct type
// olocalstatequery.StakeDistributionResult.Results is keyed by. Named here
// only for readability inside this file: Go's struct types are structural
// (not nominal) for anonymous struct literals, so a value of this named
// type is directly assignable into that map without any conversion.
type stakeDistributionEntry = struct {
	cbor.StructAsArray
	StakeFraction *cbor.Rat
	VrfHash       ledger.Blake2b256
}

// queryShelleyStakeDistribution answers GetStakeDistribution: the stake
// distribution across every block-producing pool as of right now (or, for a
// pinned at, as of that exact point) -- NOT the periodic mark/set/go
// snapshot GetPoolDistr2 (queryShelleyPoolDistr2) answers from.
//
// These are two genuinely different real cardano-node queries, confirmed
// against cardano-ledger source (blinklabs-io/dingo#4152): GetPoolDistr2
// answers from SnapShot.ssStakeMarkPoolDistr, the frozen "set" snapshot
// leader election actually uses (dingo's own praos.StakeSnapshotEpoch,
// PoolStakeDistribution below), but GetStakeDistribution answers from
// poolsByTotalStakeFraction, which explicitly reads currentSnapshot --
// cardano-ledger's own doc comment for it: "we do not want to use one of
// the regular snapshots, but rather the most recent ledger state." Routing
// both queries through PoolStakeDistribution's mark[epoch-1] snapshot (as
// this function used to) made GetStakeDistribution silently omit any pool
// that registered or first delegated after that snapshot was captured --
// confirmed live against a real Preview cardano-node: 36 real pools
// reported by cardano-node were completely absent from dingo's reply for
// no reason other than this snapshot lag.
//
// Unlike GetPoolDistr2, this query has no pool filter on the wire, so
// every pool holding live stake is reported.
//
// It does NOT reuse PoolStakeDistribution's own StakeFraction (taken over
// TotalActiveStake, the sum of delegated stake): a real cardano-node's
// GetStakeDistribution reply uses total circulating supply as its
// denominator instead, confirmed against real cardano-node's raw wire bytes
// -- see totalCirculatingSupply's doc comment (blinklabs-io/dingo#3824) for
// the full story and why GetPoolDistr2 must not make the same change. That
// denominator logic is unchanged by this function's #4152 fix -- only the
// numerator (which pools, and how much stake each holds) moved off the
// mark snapshot.
//
// at is Query's pinned point (unpinned = live). A pinned at reads the
// reserves row in effect as of at.Slot (GetNetworkStateAsOfSlot) and
// reconstructs pool stakes as of that same slot, so a correct historical
// numerator is paired with the reserves that were genuinely true at that
// same point rather than whatever is live now.
func (ls *LedgerState) queryShelleyStakeDistribution(
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	if txn == nil {
		txn = ls.db.Transaction(false)
		defer txn.Release()
	}
	metaTxn := txn.Metadata()

	var targetSlot uint64
	if at.pinned() {
		targetSlot = at.Slot
	} else {
		tip, err := ls.db.GetTip(txn)
		if err != nil {
			return nil, err
		}
		targetSlot = tip.Point.Slot
	}
	epoch, found, err := ls.resolveAsOfEpoch(txn, at)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errEpochNotResolved(at)
	}

	calc := snapshot.NewCalculator(ls.db)
	live, err := calc.CalculateStakeDistributionInTxn(
		context.Background(), txn, targetSlot,
	)
	if err != nil {
		// Epoch data may not be synced yet for the requested slot (early
		// sync, or a chain that has applied no blocks yet): there is simply
		// no stake to report, not a real failure -- same reasoning and same
		// sentinel as GetLedgerPeerSnapshot's identical early-sync case
		// (queryLedgerPeerSnapshot).
		if errors.Is(err, types.ErrNoEpochData) {
			live = &snapshot.StakeDistribution{}
		} else {
			return nil, err
		}
	}

	var networkStateAsOfSlot *uint64
	if at.pinned() {
		slotCopy := at.Slot
		networkStateAsOfSlot = &slotCopy
	}
	totalCirculatingSupply, err := ls.totalCirculatingSupply(
		epoch,
		networkStateAsOfSlot,
		true,
		metaTxn,
	)
	if err != nil {
		return nil, err
	}

	keyHashes := make([]lcommon.PoolKeyHash, 0, len(live.PoolStakes))
	for pkh := range live.PoolStakes {
		keyHashes = append(keyHashes, pkh)
	}
	// Sorted before the VRF lookup so both the reported order and the
	// omission warnings below are a function of the reconstructed stake
	// alone rather than of Go's randomised map iteration -- same reasoning
	// as PoolStakeDistribution's identical sort.
	slices.SortFunc(keyHashes, func(a, b lcommon.PoolKeyHash) int {
		return bytes.Compare(a.Bytes(), b.Bytes())
	})

	// A pinned caller's VRF lookup must be bounded to the same slot its
	// stake and circulation were reconstructed at -- otherwise a pool that
	// re-registers with a new VRF key between the pinned slot and now would
	// have its historical stake paired with a key it did not yet hold
	// (blinklabs-io/dingo#4237). Left nil for a live query, which keeps
	// poolVrfKeyHashes' unbounded "latest registration" behavior.
	var vrfAsOfSlot *uint64
	if at.pinned() {
		vrfAsOfSlot = &targetSlot
	}
	vrfByPool, err := ls.poolVrfKeyHashes(keyHashes, vrfAsOfSlot, metaTxn)
	if err != nil {
		return nil, err
	}

	circulation := new(big.Int).SetUint64(totalCirculatingSupply)
	result := olocalstatequery.StakeDistributionResult{
		Results: make(
			map[ledger.PoolId]stakeDistributionEntry,
			len(keyHashes),
		),
	}
	for _, pkh := range keyHashes {
		stake := live.PoolStakes[pkh]
		vrf, ok := vrfByPool[pkh]
		if !ok {
			// Same reasoning as PoolStakeDistribution's identical omission:
			// a pool holding stake with no registration on record cannot be
			// given a VRF key hash, and a zero one reads as a real key. Omit
			// it and log/count it rather than fail the whole query over one
			// pool (blinklabs-io/dingo#2997, blinklabs-io/dingo#4152).
			ls.metrics.incPoolStakeDistributionOmittedPool()
			ls.config.Logger.Warn(
				"omitting pool with live stake but no registration",
				"pool", hex.EncodeToString(pkh.Bytes()),
				"stake", stake,
				"component", "ledger",
			)
			continue
		}
		fraction := new(big.Rat).SetFrac(
			new(big.Int).SetUint64(stake),
			circulation,
		)
		result.Results[ledger.PoolId(pkh)] = stakeDistributionEntry{
			StakeFraction: &cbor.Rat{Rat: fraction},
			VrfHash:       vrf,
		}
	}
	// Client.GetStakeDistribution decodes the wire reply directly into a
	// StakeDistributionResult (client.go's runQuery does a plain cbor.Decode
	// with no extra wrapping), so the one field must be the top-level array
	// element here. Returning the struct itself would let its own
	// StructAsArray encoding nest inside this slice's, producing a spurious
	// extra array layer that no real NtC client can decode.
	return []any{result.Results}, nil
}
