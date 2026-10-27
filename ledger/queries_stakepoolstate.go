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
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// queryShelleyStakePoolState answers GetStakePoolState (Shelley sub-query 40),
// the state of the registered stake pools, which cardano-cli sends for
// `query pool-params` and `query pool-state`.
//
// The pool filter is the query's StrictMaybe: no filter answers every
// registered pool, an explicit set answers those of its pools that are
// registered, and a pool that is not registered is left out of the map. A
// pool with a pending retirement is still registered.
//
// Each entry reads the same registration rows GetStakePoolParams does: the
// parameters in effect this epoch, and a re-registration made during the
// epoch as the future parameters. The delegators are the active accounts
// delegated to the pool; NumDelegators counts them whether or not the query
// asks for the list, and the list is left null when it does not.
//
// Only the requested pools are read. The delegator counts come from one
// grouped query restricted to them, and the credentials are fetched only when
// the query asks for the list, so a single-pool query without delegators
// never walks the account table.
//
// The epoch, the tip slot and the protocol version all come from the read
// transaction, like every row, so a QueryView answers for the epoch it froze
// even after a boundary has moved the live snapshots on.
//
// Gaps against the ledger record: a BLS key whose registration row carries
// no registration epoch is left out of the entry, and a deposit that was
// never recorded is 0.
//
// Live-only: pool registrations and delegations carry no per-point history to
// read back here.
func (ls *LedgerState) queryShelleyStakePoolState(
	ctx context.Context,
	q *olocalstatequery.ShelleyStakePoolStateQuery,
	txn *database.Txn,
) (any, error) {
	filter, all := q.PoolFilter()
	if err := checkLocalStateQueryItemLimit(
		"GetStakePoolState",
		len(filter),
	); err != nil {
		return nil, err
	}
	result := make(
		map[ledger.PoolId]olocalstatequery.StakePoolStateEntry,
	)
	if !all && len(filter) == 0 {
		return []any{result}, nil
	}
	txn, release := ls.readTxn(ctx, txn)
	defer release()

	// A filter names the pools to answer, and the registration rows read
	// below decide which of them are registered: a pool that is not is
	// simply absent from them. Only the unfiltered query needs the list of
	// registered pools.
	var keyHashes []lcommon.PoolKeyHash
	if all {
		active, err := ls.db.GetActivePoolKeyHashes(ctx, txn)
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
		return []any{result}, nil
	}

	meta := txn.Metadata()
	networkID := uint8(ls.NewView(txn).NetworkId()) // #nosec G115 -- 0 or 1
	consensus := ls.loadConsensusSnapshot()
	tip, epochRow, err := ls.epochAtTip(txn)
	if err != nil {
		return nil, err
	}
	epoch := consensus.currentEpoch
	if epochRow != nil {
		epoch = *epochRow
	}
	tipSlot := tip.Point.Slot
	pparams, err := ls.snapshotProtocolParameters(consensus, epochRow, txn)
	if err != nil {
		return nil, err
	}
	includeBlsKey := false
	if pv, err := GetProtocolVersion(pparams); err == nil {
		includeBlsKey = pv.Major >= blsKeyMinProtocolMajor
	}

	effective, err := ls.db.Metadata().GetPoolRegistrationsEffectiveForEpoch(
		keyHashes,
		epoch.StartSlot,
		epoch.EpochId,
		tipSlot,
		meta,
	)
	if err != nil {
		return nil, err
	}
	if len(effective) == 0 {
		return []any{result}, nil
	}
	latest, err := ls.db.Metadata().GetPoolRegistrationsAtSlot(
		keyHashes,
		math.MaxInt64,
		meta,
	)
	if err != nil {
		return nil, err
	}
	latestByPool := make(map[string]*models.PoolRegistration, len(latest))
	for i := range latest {
		latestByPool[string(latest[i].PoolKeyHash)] = &latest[i]
	}
	deposits, err := ls.db.Metadata().GetPoolDepositsHeld(keyHashes, meta)
	if err != nil {
		return nil, err
	}
	retiringRows, err := ls.db.Metadata().GetRetiringPools(epoch.EpochId, meta)
	if err != nil {
		return nil, err
	}
	retiring := make(map[string]uint64, len(retiringRows))
	for _, row := range retiringRows {
		retiring[string(row.PoolKeyHash)] = row.Epoch
	}
	delegatorCounts, err := ls.db.Metadata().GetActiveDelegatorCountsByPool(
		keyHashes,
		meta,
	)
	if err != nil {
		return nil, err
	}
	var delegators map[string][]models.StakeCredentialRef
	if q.IncludeDelegators {
		delegators, err = ls.db.Metadata().GetActiveDelegatorCredentialsByPool(
			keyHashes,
			meta,
		)
		if err != nil {
			return nil, err
		}
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
		entry := olocalstatequery.StakePoolStateEntry{
			Vrf:    params.VrfKeyHash,
			Pledge: params.Pledge,
			Cost:   params.Cost,
			Margin: params.Margin,
			AccountId: lcommon.Credential{
				CredType: uint(reg.RewardAccountCredentialTag),
				Credential: lcommon.NewBlake2b224(
					reg.RewardAccount,
				),
			},
			Relays:        params.Relays,
			Metadata:      params.Metadata,
			Deposit:       deposits[string(reg.PoolKeyHash)],
			NumDelegators: int(delegatorCounts[string(reg.PoolKeyHash)]), // #nosec G115 -- bounded by the account table
		}
		if params.Owners != nil {
			entry.Owners = *params.Owners
		}
		// The ledger's BlsKeyState always carries its registration epoch. A
		// key with no stored epoch has none to report: it is an imported key
		// of unknown age, which rotation treats as not eligible, or a row
		// whose slot the epoch history cannot place, where rotation's own
		// fallback cannot place it either. It is left out rather than
		// reported with an invented epoch.
		if params.BlsKey != nil && reg.LeiosKeyRegistrationEpoch != nil {
			entry.BlsKey = &olocalstatequery.BlsKeyState{
				Key: olocalstatequery.BlsKey{
					PublicKey:       params.BlsKey.PublicKey,
					PossessionProof: params.BlsKey.PossessionProof,
				},
				RegisteredIn: *reg.LeiosKeyRegistrationEpoch,
			}
		}
		if q.IncludeDelegators {
			credentials := make([]lcommon.Credential, 0, len(delegators[string(reg.PoolKeyHash)]))
			for _, ref := range delegators[string(reg.PoolKeyHash)] {
				hash, err := lcommon.NewBlake2b224Checked(ref.Key)
				if err != nil {
					return nil, fmt.Errorf("pool delegator: %w", err)
				}
				credentials = append(credentials, lcommon.Credential{
					CredType:   uint(ref.Tag),
					Credential: hash,
				})
			}
			set := cbor.NewSetType(credentials, true)
			entry.Delegators = &set
		}
		if epochNo, ok := retiring[string(reg.PoolKeyHash)]; ok {
			entry.Retiring = &epochNo
		}
		// A registration after the epoch began, on top of one in effect, is
		// the ledger's future parameters until the next boundary.
		if next := latestByPool[string(reg.PoolKeyHash)]; next != nil &&
			next.ID != reg.ID &&
			next.AddedSlot >= epoch.StartSlot {
			future, err := stakePoolParamsFromRegistration(
				next,
				networkID,
				includeBlsKey,
			)
			if err != nil {
				return nil, err
			}
			entry.FutureStakePoolParams, err = stakePoolParamsEntry(future)
			if err != nil {
				return nil, err
			}
		}
		result[ledger.PoolId(params.Operator)] = entry
	}
	return []any{result}, nil
}

// stakePoolParamsEntry converts the parameters to their GetStakePoolState
// form, which holds the reward account as the address bytes.
func stakePoolParamsEntry(
	p stakePoolParams,
) (*olocalstatequery.StakePoolParamsEntry, error) {
	account, err := p.RewardAccount.Bytes()
	if err != nil {
		return nil, fmt.Errorf("pool reward account bytes: %w", err)
	}
	entry := &olocalstatequery.StakePoolParamsEntry{
		Id:             ledger.PoolId(p.Operator),
		Vrf:            p.VrfKeyHash,
		Pledge:         p.Pledge,
		Cost:           p.Cost,
		Margin:         p.Margin,
		AccountAddress: account,
		Relays:         p.Relays,
		Metadata:       p.Metadata,
	}
	if p.Owners != nil {
		entry.Owners = *p.Owners
	}
	if p.BlsKey != nil {
		entry.BlsKey = &olocalstatequery.BlsKey{
			PublicKey:       p.BlsKey.PublicKey,
			PossessionProof: p.BlsKey.PossessionProof,
		}
	}
	return entry, nil
}
