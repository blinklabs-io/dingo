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
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// ErrPoolDistrUnregisteredPool reports a pool holding stake in the snapshot
// with no registration on record. The pool cannot be given a VRF key hash, and
// the total active stake is summed over the whole snapshot, so omitting it
// would leave the reported fractions summing to less than one with nothing
// saying so.
var ErrPoolDistrUnregisteredPool = errors.New(
	"pool holds snapshot stake but has no registration",
)

// queryShelleyPoolDistr2 answers GetPoolDistr2, the stake distribution across
// block-producing pools.
//
// cardano-cli sends this while computing a leadership schedule, having chosen
// it over the deprecated GetPoolDistr once node-to-client protocol version 21
// is negotiated.
//
// The distribution is read from the same snapshot the node elects leaders
// from — the mark snapshot at praos.StakeSnapshotEpoch, not live stake. An
// operator checking a schedule against the node would otherwise be told they
// lead slots the node will not let them mint.
func (ls *LedgerState) queryShelleyPoolDistr2(
	q *olocalstatequery.ShelleyPoolDistr2Query,
) (any, error) {
	epoch := ls.loadConsensusSnapshot().currentEpoch.EpochId
	snapshotEpoch := praos.StakeSnapshotEpoch(epoch)

	// The per-pool stakes and their total have to come from one view: read
	// separately, an epoch boundary landing in between would produce
	// fractions that do not sum to one.
	txn := ls.db.Transaction(false)
	defer txn.Release()
	metaTxn := txn.Metadata()

	stakeByPool, err := ls.markStakeByPool(snapshotEpoch, true, metaTxn)
	if err != nil {
		return nil, err
	}
	totalActiveStake, err := ls.db.Metadata().GetTotalActiveStake(
		snapshotEpoch,
		snapshotTypeMark,
		metaTxn,
	)
	if err != nil {
		return nil, err
	}

	requested, all := q.PoolFilter()
	wanted := make(map[lcommon.PoolKeyHash]struct{}, len(requested))
	for _, pkh := range requested {
		wanted[lcommon.PoolKeyHash(pkh)] = struct{}{}
	}

	keyHashes := make([]lcommon.PoolKeyHash, 0, len(stakeByPool))
	for hash := range stakeByPool {
		pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224([]byte(hash)))
		if !all {
			if _, ok := wanted[pkh]; !ok {
				continue
			}
		}
		keyHashes = append(keyHashes, pkh)
	}

	// The VRF key hash lives on the pool registration rather than the
	// snapshot, so it is fetched in bulk rather than per pool.
	vrfByPool, err := ls.poolVrfKeyHashes(keyHashes, metaTxn)
	if err != nil {
		return nil, err
	}

	result := olocalstatequery.PoolDistr2Result{
		Pools: make(
			map[ledger.PoolId]olocalstatequery.PoolDistr2IndividualStake,
			len(keyHashes),
		),
		TotalActiveStake: totalActiveStake,
	}
	for _, pkh := range keyHashes {
		stake := stakeByPool[string(pkh.Bytes())]
		vrf, ok := vrfByPool[pkh]
		if !ok {
			// Dropping the pool would be worse than failing. Its stake is
			// still counted in TotalActiveStake, which is summed over the
			// whole snapshot, so the remaining fractions would sum to less
			// than one and the caller would have no way to tell. Reporting it
			// with a zero VRF hash is no better, since that reads as a real
			// key. This is a database inconsistency rather than a routine
			// case, so it fails loudly.
			return nil, fmt.Errorf(
				"%w: pool %x at epoch %d",
				ErrPoolDistrUnregisteredPool,
				pkh.Bytes(),
				snapshotEpoch,
			)
		}
		result.Pools[ledger.PoolId(pkh)] = olocalstatequery.PoolDistr2IndividualStake{
			StakeFraction:  stakeFraction(stake, totalActiveStake),
			TotalPoolStake: stake,
			VrfHash:        vrf,
		}
	}
	return []any{result}, nil
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

// poolVrfKeyHashes looks up the VRF key hash registered for each pool.
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
		// The VRF hash on the pool row is a denormalized copy that can outlive
		// the registration it came from, so the registration itself is what
		// decides whether the pool is usable here.
		if len(pool.Registration) == 0 {
			continue
		}
		if len(pool.VrfKeyHash) != lcommon.Blake2b256Size {
			continue
		}
		pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(pool.PoolKeyHash))
		out[pkh] = ledger.Blake2b256(
			lcommon.NewBlake2b256(pool.VrfKeyHash),
		)
	}
	return out, nil
}
