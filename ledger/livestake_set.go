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
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
)

// liveStakeSource computes the metadata store's reward_live_stake figures
// from what the live UTxO set holds: a credential's stake is the lovelace in
// live outputs naming it as a stake credential, as txn sees the set, plus its
// account's reward balance and its pending reward credits. The row's
// registration and pool are the account's. Pointer-address stake is not part
// of the row.
type liveStakeSource struct {
	accountSource
	ur UtxoSetReader
}

// accountSource reads accounts as txn sees the set.
type accountSource struct {
	ls  *LedgerState
	ar  AccountReader
	txn ledgerdb.Txn
	// rr serves the pending credits; it is nil until the set holds the
	// reward rows.
	rr RewardOutputReader
}

// liveStakeSource returns the source for comparing a live-stake read, or
// false when reads do not compare or the set cannot answer.
func (ls *LedgerState) liveStakeSource(
	txn ledgerdb.Txn,
) (liveStakeSource, bool) {
	ur, mode := ls.utxoSetReaderFor(txn)
	if !mode || ur == nil {
		return liveStakeSource{}, false
	}
	ar := ls.accountReader(txn)
	if ar == nil || ls.utxoSetFailed.Load() {
		return liveStakeSource{}, false
	}
	return liveStakeSource{
		accountSource: accountSource{
			ls: ls, ar: ar, txn: txn, rr: ls.rewardOutputReader(txn),
		},
		ur: ur,
	}, true
}

// stake returns a's UTxO stake and its total with the reward balance and
// pending credits.
func (src liveStakeSource) stake(a *models.Account) (utxo, total uint64, err error) {
	utxo, err = src.ls.utxoStakeSeenBy(
		src.ur, src.txn, CredentialStakeKey(a.CredentialTag, a.StakingKey),
	)
	if err != nil {
		return 0, 0, err
	}
	reward := uint64(a.Reward)
	if src.rr != nil {
		pending, err := src.ls.pendingRewardCreditView(
			src.rr, src.txn, a.CredentialTag, a.StakingKey,
		)
		if err != nil {
			return 0, 0, err
		}
		if pending > math.MaxUint64-reward {
			return 0, 0, fmt.Errorf(
				"live stake overflow for credential %d:%x", a.CredentialTag, a.StakingKey,
			)
		}
		reward += pending
	}
	if utxo > math.MaxUint64-reward {
		return 0, 0, fmt.Errorf(
			"live stake overflow for credential %d:%x", a.CredentialTag, a.StakingKey,
		)
	}
	return utxo, utxo + reward, nil
}

// stagedAccountRefs appends to refs, which seen indexes, the accounts the
// batches txn sees may have changed and refs lacks. A caller confirms each
// with accountView.
func (src accountSource) stagedAccountRefs(
	refs []models.StakeCredentialRef,
	seen map[string]struct{},
) ([]models.StakeCredentialRef, error) {
	return src.ls.accountCandidates(src.ar, src.txn, refs, seen)
}

// accountCandidates appends to refs, which seen indexes, the accounts the
// set's indexes may not list for txn's view and refs lacks: those a visible
// batch staged, and, when a visible batch staged a rollback, those the
// rollback restores, which the indexes of the committed rows do not name. A
// caller confirms each with accountView.
func (ls *LedgerState) accountCandidates(
	ar AccountReader,
	txn ledgerdb.Txn,
	refs []models.StakeCredentialRef,
	seen map[string]struct{},
) ([]models.StakeCredentialRef, error) {
	var ceiling *uint64
	add := func(ref models.StakeCredentialRef) {
		if _, ok := seen[ref.MapKey()]; !ok {
			seen[ref.MapKey()] = struct{}{}
			refs = append(refs, ref)
		}
	}
	for _, o := range ls.stagedOps(txn) {
		if o.rollbackTo != nil && (ceiling == nil || *o.rollbackTo < *ceiling) {
			to := *o.rollbackTo
			ceiling = &to
		}
		for _, c := range o.accounts {
			add(models.NewStakeCredentialRef(
				c.Account.CredentialTag, c.Account.StakingKey,
			))
		}
	}
	if ceiling == nil {
		return refs, nil
	}
	ah, ok := ar.(AccountAsOfReader)
	if !ok {
		return nil, ls.unavailableAsOf("accounts")
	}
	changed, err := ah.AccountRefsChangedAfter(*ceiling)
	if err != nil {
		return nil, err
	}
	for _, ref := range changed {
		add(ref)
	}
	return refs, nil
}

func refSet(refs []models.StakeCredentialRef) map[string]struct{} {
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		seen[ref.MapKey()] = struct{}{}
	}
	return seen
}

// eachPoolDelegator calls fn with every active account delegating to pool.
func (src accountSource) eachPoolDelegator(
	pool []byte,
	fn func(a *models.Account) error,
) error {
	refs, err := src.ar.AccountsByPool(pool)
	if err != nil {
		return err
	}
	refs, err = src.stagedAccountRefs(refs, refSet(refs))
	if err != nil {
		return err
	}
	for _, ref := range refs {
		a, err := src.ls.accountView(src.ar, src.txn, ref.Tag, ref.Key)
		if err != nil {
			return err
		}
		if a == nil || !a.Active || !bytes.Equal(a.Pool, pool) {
			continue
		}
		if err := fn(a); err != nil {
			return err
		}
	}
	return nil
}

// expiryAllows reports whether the CIP-0163 gate expiryEpoch, zero for off,
// leaves a's reward account counted.
func expiryAllows(a *models.Account, expiryEpoch uint64) bool {
	return expiryEpoch == 0 || a.ExpirationEpoch == 0 ||
		a.ExpirationEpoch >= expiryEpoch
}

// poolInputRows is GetLiveStakeInputsForPools's rows: every active account
// delegating to one of pools that the CIP-0163 gate leaves counted, with its
// UTxO stake and reward balance, ordered as the store orders them.
func (src liveStakeSource) poolInputRows(
	pools [][]byte,
	expiryEpoch uint64,
) ([]*models.RewardStakeInput, error) {
	out := make([]*models.RewardStakeInput, 0)
	seen := make(map[string]struct{}, len(pools))
	for _, pool := range pools {
		if _, ok := seen[string(pool)]; ok || len(pool) == 0 {
			continue
		}
		seen[string(pool)] = struct{}{}
		err := src.eachPoolDelegator(pool, func(a *models.Account) error {
			if !expiryAllows(a, expiryEpoch) {
				return nil
			}
			_, total, err := src.stake(a)
			if err != nil {
				return err
			}
			out = append(out, &models.RewardStakeInput{
				PoolKeyHash:   bytes.Clone(pool),
				StakingKey:    bytes.Clone(a.StakingKey),
				CredentialTag: a.CredentialTag,
				Stake:         types.Uint64(total),
				Registered:    true,
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	slices.SortFunc(out, func(a, b *models.RewardStakeInput) int {
		return cmp.Or(
			bytes.Compare(a.PoolKeyHash, b.PoolKeyHash),
			cmp.Compare(a.CredentialTag, b.CredentialTag),
			bytes.Compare(a.StakingKey, b.StakingKey),
		)
	})
	return out, nil
}

// delegatedPools is the set of pools an active account delegates to.
func (src liveStakeSource) delegatedPools() (map[string]uint64, error) {
	refs, err := src.ar.ActiveAccountRefs()
	if err != nil {
		return nil, err
	}
	refs, err = src.stagedAccountRefs(refs, refSet(refs))
	if err != nil {
		return nil, err
	}
	out := make(map[string]uint64)
	for _, ref := range refs {
		a, err := src.ls.accountView(src.ar, src.txn, ref.Tag, ref.Key)
		if err != nil {
			return nil, err
		}
		if a != nil && a.Active && len(a.Pool) > 0 {
			out[string(a.Pool)] = 0
		}
	}
	return out, nil
}

// poolStakeMaps is GetStakeByPools's UTxO stake and delegator count per pool.
// Every pool asked for has an entry in both.
func (src liveStakeSource) poolStakeMaps(
	pools [][]byte,
) (stakes, delegators map[string]uint64, err error) {
	stakes = make(map[string]uint64, len(pools))
	delegators = make(map[string]uint64, len(pools))
	for _, pool := range pools {
		stakes[string(pool)] = 0
		delegators[string(pool)] = 0
	}
	for _, pool := range pools {
		if len(pool) == 0 {
			continue
		}
		var stake, count uint64
		err := src.eachPoolDelegator(pool, func(a *models.Account) error {
			utxo, _, err := src.stake(a)
			if err != nil {
				return err
			}
			if utxo > math.MaxUint64-stake {
				return fmt.Errorf("pool %x stake overflow", pool)
			}
			stake += utxo
			count++
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
		stakes[string(pool)] = stake
		delegators[string(pool)] = count
	}
	return stakes, delegators, nil
}

// drepPowerOf is the stake delegated to the DRep (typ, drep), by the active
// accounts the CIP-0163 gate leaves counted: UTxO stake plus reward balance.
// It also returns how many accounts count, which the store's batch and by-type
// reads leave a DRep out for when it is none.
func (src liveStakeSource) drepPowerOf(
	typ uint64,
	drep []byte,
	expiryEpoch uint64,
) (power uint64, delegators int, err error) {
	refs, err := src.ls.accountsByDrepCandidates(src.ar, src.txn, typ, drep)
	if err != nil {
		return 0, 0, err
	}
	for _, ref := range refs {
		a, err := src.ls.accountView(src.ar, src.txn, ref.Tag, ref.Key)
		if err != nil {
			return 0, 0, err
		}
		if a == nil || !a.Active || a.DrepType != typ ||
			!bytes.Equal(a.Drep, drep) || !expiryAllows(a, expiryEpoch) {
			continue
		}
		_, total, err := src.stake(a)
		if err != nil {
			return 0, 0, err
		}
		if total > math.MaxUint64-power {
			return 0, 0, fmt.Errorf("DRep %d/%x power overflow", typ, drep)
		}
		power += total
		delegators++
	}
	return power, delegators, nil
}

func (src liveStakeSource) drepPower(
	typ uint64,
	drep []byte,
	expiryEpoch uint64,
) (uint64, error) {
	power, _, err := src.drepPowerOf(typ, drep, expiryEpoch)
	return power, err
}

// liveStakeServed returns the source a live stake read is answered from: the
// set's accounts and stake.
func (s stagingLedgerDB) liveStakeServed(
	txn ledgerdb.Txn,
) (liveStakeSource, error) {
	if s.ls.accountServing(txn) != nil {
		if src, ok := s.ls.liveStakeSource(txn); ok {
			return src, nil
		}
	}
	return liveStakeSource{}, s.ls.setFamilyUnserved(
		nodesettings.FamilyLiveStake,
	)
}

func (s stagingLedgerDB) GetLiveStakeInputsForPools(
	poolKeyHashes [][]byte,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) ([]*models.RewardStakeInput, error) {
	src, err := s.liveStakeServed(txn)
	if err != nil {
		return nil, err
	}
	if len(poolKeyHashes) == 0 {
		return nil, nil
	}
	return src.poolInputRows(poolKeyHashes, expiryEpoch)
}

func (s stagingLedgerDB) GetDelegatedPoolKeyHashes(
	txn ledgerdb.Txn,
) ([][]byte, error) {
	src, err := s.liveStakeServed(txn)
	if err != nil {
		return nil, err
	}
	pools, err := src.delegatedPools()
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(pools))
	for p := range pools {
		out = append(out, []byte(p))
	}
	slices.SortFunc(out, bytes.Compare)
	return out, nil
}

func (s stagingLedgerDB) GetStakeByPools(
	ctx context.Context,
	poolKeyHashes [][]byte,
	txn ledgerdb.Txn,
) (map[string]uint64, map[string]uint64, error) {
	src, err := s.liveStakeServed(txn)
	if err != nil {
		return nil, nil, err
	}
	return src.poolStakeMaps(poolKeyHashes)
}

func (s stagingLedgerDB) GetDRepVotingPower(
	ctx context.Context,
	credentialTag uint8,
	drepCredential []byte,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) (uint64, error) {
	src, err := s.liveStakeServed(txn)
	if err != nil {
		return 0, err
	}
	return src.drepPower(uint64(credentialTag), drepCredential, expiryEpoch)
}

func (s stagingLedgerDB) GetDRepVotingPowerBatch(
	ctx context.Context,
	drepCredentials []models.StakeCredentialRef,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) (map[string]uint64, error) {
	src, err := s.liveStakeServed(txn)
	if err != nil {
		return nil, err
	}
	out := make(map[string]uint64, len(drepCredentials))
	for _, ref := range drepCredentials {
		power, count, err := src.drepPowerOf(uint64(ref.Tag), ref.Key, expiryEpoch)
		if err != nil {
			return nil, err
		}
		if count > 0 {
			out[ref.MapKey()] = power
		}
	}
	return out, nil
}

func (s stagingLedgerDB) GetDRepVotingPowerByType(
	ctx context.Context,
	drepTypes []uint64,
	expiryEpoch uint64,
	txn ledgerdb.Txn,
) (map[uint64]uint64, error) {
	src, err := s.liveStakeServed(txn)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]uint64, len(drepTypes))
	for _, drepType := range drepTypes {
		power, count, err := src.drepPowerOf(drepType, nil, expiryEpoch)
		if err != nil {
			return nil, err
		}
		if count > 0 {
			out[drepType] = power
		}
	}
	return out, nil
}
