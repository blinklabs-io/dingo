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
	"context"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// poolStateFilter reports the pools GetPoolState is restricted to. all is
// true for the StrictMaybe's empty form; otherwise pools is the requested set,
// which may be empty.
func poolStateFilter(
	q *olocalstatequery.ShelleyPoolStateQuery,
) (pools []ledger.PoolId, all bool) {
	if len(q.Pools) == 0 {
		return nil, true
	}
	return q.Pools[0].Items(), false
}

// queryShelleyPoolState answers GetPoolState, which cardano-cli sends for
// `query pool-state`. The reply is the ledger's QueryPoolStateResult, four
// maps keyed by pool: the parameters in effect this epoch, the future
// parameters a re-registration made during the epoch will bring in at the
// next boundary, the epoch of each pending retirement, and the deposit each
// registered pool holds.
//
// The filter is the query's StrictMaybe: no filter answers every registered
// pool, and an explicit set restricts all four maps to its pools. A pool that
// is not registered, including one whose retirement epoch has begun, is
// absent from every map; a pool with a pending retirement is still
// registered and appears in all of them.
//
// Point-aware: registrations, retirements and deposits are read as of the
// acquired block, and the epoch is the one containing it, so a reply for a
// past point reports the state that point had rather than the live one. A
// point no epoch record covers fails with ErrHistoricalStateUnavailable.
func (ls *LedgerState) queryShelleyPoolState(
	ctx context.Context,
	q *olocalstatequery.ShelleyPoolStateQuery,
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	filter, all := poolStateFilter(q)
	if err := checkLocalStateQueryItemLimit(
		"GetPoolState",
		len(filter),
	); err != nil {
		return nil, err
	}
	pstate := map[ledger.PoolId]stakePoolParams{}
	fstate := map[ledger.PoolId]stakePoolParams{}
	retiring := map[ledger.PoolId]uint64{}
	deposits := map[ledger.PoolId]uint64{}
	reply := func() []any {
		return []any{[]any{pstate, fstate, retiring, deposits}}
	}
	if !all && len(filter) == 0 {
		return reply(), nil
	}
	txn, release := ls.readTxn(ctx, txn)
	defer release()

	consensus := ls.loadConsensusSnapshot()
	tip, epochRow, err := ls.epochAtPoint(at, txn)
	if err != nil {
		return nil, err
	}
	epoch := consensus.currentEpoch
	if epochRow != nil {
		epoch = *epochRow
	} else if at.pinned() {
		return nil, errEpochNotResolved(at)
	}
	slot := tip.Point.Slot
	meta := txn.Metadata()

	var keyHashes []lcommon.PoolKeyHash
	if all {
		active, err := ls.db.Metadata().GetActivePoolKeyHashesAtSlot(slot, meta)
		if err != nil {
			return nil, err
		}
		keyHashes = make([]lcommon.PoolKeyHash, 0, len(active))
		for _, hash := range active {
			keyHashes = append(keyHashes, lcommon.PoolKeyHash(
				lcommon.NewBlake2b224(hash),
			))
		}
	} else {
		seen := make(map[lcommon.PoolKeyHash]struct{}, len(filter))
		keyHashes = make([]lcommon.PoolKeyHash, 0, len(filter))
		for _, poolId := range filter {
			hash := lcommon.PoolKeyHash(poolId)
			if _, dup := seen[hash]; dup {
				continue
			}
			seen[hash] = struct{}{}
			keyHashes = append(keyHashes, hash)
		}
	}
	if len(keyHashes) == 0 {
		return reply(), nil
	}

	pparams, err := ls.snapshotProtocolParameters(consensus, epochRow, txn)
	if err != nil {
		return nil, err
	}
	includeBlsKey := false
	if pv, err := GetProtocolVersion(pparams); err == nil {
		includeBlsKey = pv.Major >= blsKeyMinProtocolMajor
	}
	networkID := uint8(ls.NewView(txn).NetworkId()) // #nosec G115 -- 0 or 1

	effective, err := ls.db.Metadata().GetPoolRegistrationsEffectiveForEpoch(
		keyHashes,
		epoch.StartSlot,
		epoch.EpochId,
		slot,
		meta,
	)
	if err != nil {
		return nil, err
	}
	if len(effective) == 0 {
		return reply(), nil
	}
	registered := make([]lcommon.PoolKeyHash, 0, len(effective))
	for i := range effective {
		registered = append(registered, lcommon.PoolKeyHash(
			lcommon.NewBlake2b224(effective[i].PoolKeyHash),
		))
	}
	latest, err := ls.db.Metadata().GetPoolRegistrationsAtSlot(
		registered,
		slot,
		meta,
	)
	if err != nil {
		return nil, err
	}
	latestByPool := make(map[string]*models.PoolRegistration, len(latest))
	for i := range latest {
		latestByPool[string(latest[i].PoolKeyHash)] = &latest[i]
	}
	held, err := ls.db.Metadata().GetPoolDepositsHeldAtSlot(
		registered,
		slot,
		meta,
	)
	if err != nil {
		return nil, err
	}
	retiringRows, err := ls.db.Metadata().GetRetiringPoolsAtSlot(
		epoch.EpochId,
		slot,
		meta,
	)
	if err != nil {
		return nil, err
	}
	retiringEpoch := make(map[string]uint64, len(retiringRows))
	for _, row := range retiringRows {
		retiringEpoch[string(row.PoolKeyHash)] = row.Epoch
	}

	for i := range effective {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reg := &effective[i]
		params, err := stakePoolParamsFromRegistration(
			reg,
			networkID,
			includeBlsKey,
		)
		if err != nil {
			return nil, err
		}
		id := ledger.PoolId(params.Operator)
		pstate[id] = params
		if deposit, ok := held[string(reg.PoolKeyHash)]; ok {
			deposits[id] = deposit
		}
		if epochNo, ok := retiringEpoch[string(reg.PoolKeyHash)]; ok {
			retiring[id] = epochNo
		}
		// A registration after the epoch began, on top of one in effect, is
		// the ledger's future parameters until the next boundary.
		if next := latestByPool[string(reg.PoolKeyHash)]; next != nil &&
			next.ID != reg.ID {
			future, err := stakePoolParamsFromRegistration(
				next,
				networkID,
				includeBlsKey,
			)
			if err != nil {
				return nil, err
			}
			fstate[id] = future
		}
	}
	return reply(), nil
}
