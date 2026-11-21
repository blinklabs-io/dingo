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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
)

// liveStakeRowLister lists the metadata store's reward_live_stake rows.
type liveStakeRowLister interface {
	EachRewardLiveStake(
		txn ledgerdb.Txn,
		fn func(*models.RewardLiveStake) error,
	) error
}

// accountLister walks every account a set holds.
type accountLister interface {
	Accounts(fn func(*models.Account) error) error
}

// credentialStakeLister walks every stake credential that live outputs name.
type credentialStakeLister interface {
	EachCredentialStake(
		fn func(tag uint8, hash []byte, lovelace uint64) error,
	) error
}

// liveStakeLoaded reports whether set holds the accounts and the reward rows
// that its live stake is derived from.
func liveStakeLoaded(set UtxoSetWriter) (bool, error) {
	accounts, ok := set.(AccountReader)
	if !ok {
		return false, nil
	}
	loaded, err := accounts.AccountsLoaded()
	if err != nil || !loaded {
		return false, err
	}
	rewards, ok := set.(RewardOutputReader)
	if !ok {
		return false, nil
	}
	return rewards.RewardOutputsLoaded()
}

// expectedLiveStakeRow is a reward_live_stake row as the set derives it: the
// account's registration, pool and reward balance and the credential's UTxO
// stake. A credential has a row when it has an account or holds stake.
type expectedLiveStakeRow struct {
	pool       []byte
	utxo       uint64
	reward     uint64
	registered bool
}

func (r expectedLiveStakeRow) String() string {
	return fmt.Sprintf(
		"utxo=%d reward=%d registered=%t pool=%x",
		r.utxo, r.reward, r.registered, r.pool,
	)
}

func expectedLiveStake(
	set UtxoSetWriter,
) (map[string]expectedLiveStakeRow, error) {
	accounts, ok := set.(accountLister)
	stakes, ok2 := set.(credentialStakeLister)
	if !ok || !ok2 {
		return nil, errors.New("the set does not serve live stake")
	}
	utxo := map[string]uint64{}
	err := stakes.EachCredentialStake(
		func(tag uint8, hash []byte, lovelace uint64) error {
			if lovelace > 0 && len(hash) > 0 {
				utxo[models.NewStakeCredentialRef(tag, hash).MapKey()] = lovelace
			}
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	rows := make(map[string]expectedLiveStakeRow, len(utxo))
	err = accounts.Accounts(func(a *models.Account) error {
		if len(a.StakingKey) == 0 {
			return nil
		}
		key := models.NewStakeCredentialRef(
			a.CredentialTag, a.StakingKey,
		).MapKey()
		row := expectedLiveStakeRow{
			utxo: utxo[key], reward: uint64(a.Reward), registered: a.Active,
		}
		if a.Active {
			row.pool = a.Pool
		}
		rows[key] = row
		delete(utxo, key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	for key, lovelace := range utxo {
		rows[key] = expectedLiveStakeRow{utxo: lovelace}
	}
	return rows, nil
}

// liveStakeVerifyDiffLimit bounds the credentials a mismatch error lists.
const liveStakeVerifyDiffLimit = 5

// verifyLiveStakeRows compares every reward_live_stake row of ldb with the
// figures the set derives for the same credential, and the credentials each
// side holds a row for.
func verifyLiveStakeRows(
	ldb ledgerdb.LedgerDB,
	set UtxoSetWriter,
	txn ledgerdb.Txn,
) error {
	lister, ok := ldb.(liveStakeRowLister)
	if !ok {
		return errors.New("the metadata store cannot list live stake rows")
	}
	want, err := expectedLiveStake(set)
	if err != nil {
		return err
	}
	var diffs []string
	differing := 0
	note := func(format string, args ...any) {
		differing++
		if len(diffs) < liveStakeVerifyDiffLimit {
			diffs = append(diffs, fmt.Sprintf(format, args...))
		}
	}
	stored := 0
	err = lister.EachRewardLiveStake(
		txn,
		func(r *models.RewardLiveStake) error {
			stored++
			key := models.NewStakeCredentialRef(
				r.CredentialTag, r.StakingKey,
			).MapKey()
			exp, ok := want[key]
			if !ok {
				note(
					"%d:%x only in the store: utxo=%d reward=%d",
					r.CredentialTag, r.StakingKey,
					uint64(r.UtxoStake), uint64(r.RewardStake),
				)
				return nil
			}
			delete(want, key)
			got := expectedLiveStakeRow{
				pool: r.PoolKeyHash, utxo: uint64(r.UtxoStake),
				reward: uint64(r.RewardStake), registered: r.Registered,
			}
			if got.utxo != exp.utxo || got.reward != exp.reward ||
				uint64(r.TotalStake) != exp.utxo+exp.reward ||
				got.registered != exp.registered ||
				!bytes.Equal(got.pool, exp.pool) {
				note(
					"%d:%x store %s set %s",
					r.CredentialTag, r.StakingKey, got, exp,
				)
			}
			return nil
		},
	)
	if err != nil {
		return err
	}
	for key, exp := range want {
		note("%d:%x only in the set: %s", key[0], key[1:], exp)
	}
	if differing == 0 {
		return nil
	}
	slices.Sort(diffs)
	return fmt.Errorf(
		"live stake differs between the metadata store and the set: "+
			"%d credentials of %d store rows: %s",
		differing, stored, strings.Join(diffs, "; "),
	)
}
