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
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// The genesis writes put accounts, pools, DReps and delegations in the set
// as data, not as transitions of certificates; the metadata store keeps only
// the certificate history of a drained family. The set computes them from the
// genesis configuration with the functions the metadata store imports them
// with (models.GenesisStakeAccounts, models.GenesisDelegateeOf,
// models.GenesisDRep, models.PoolRegistrationFromCertificate), and reads
// nothing back from the store.

// stageGenesisStaking stages, at slot 0, what Shelley genesis staking
// imports: each pool registered with its deposit held, each delegating
// account active with its import baseline. A pool the set already holds, and
// an account it already holds in the same state, are left alone.
func (ls *LedgerState) stageGenesisStaking(
	txn ledgerdb.Txn,
	pools map[string]lcommon.PoolRegistrationCertificate,
	stakeDelegations map[string]string,
	keyDeposit uint64,
	poolDeposit uint64,
) {
	r := ls.accountReader(txn)
	if r == nil || ls.utxoSetFailed.Load() {
		return
	}
	if err := ls.stageGenesisAccounts(r, txn, stakeDelegations, keyDeposit); err != nil {
		ls.failAccountStaging(txn, err)
		return
	}
	if err := ls.stageGenesisPools(txn, pools, poolDeposit); err != nil {
		ls.failAccountStaging(txn, err)
	}
}

func (ls *LedgerState) stageGenesisAccounts(
	r AccountReader,
	txn ledgerdb.Txn,
	stakeDelegations map[string]string,
	keyDeposit uint64,
) error {
	accounts, err := models.GenesisStakeAccounts(stakeDelegations, keyDeposit)
	if err != nil {
		return err
	}
	for _, a := range accounts {
		prev, err := ls.accountView(r, txn, a.CredentialTag, a.StakingKey)
		if err != nil {
			return err
		}
		next := a
		next.ImportDeposit = nil
		if prev != nil {
			// The import upserts the delegation, the reward and the active
			// flag of an account it finds.
			next = *prev
			next.Pool, next.Drep, next.DrepType = a.Pool, a.Drep, a.DrepType
			next.Active, next.Reward = true, a.Reward
		}
		if prev == nil || !sameAccount(*prev, next) {
			ls.stageLedgerChanges(
				txn,
				LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
					Accounts: []AccountChange{{Slot: 0, Account: next}},
				},
			)
		}
		var deposit *uint64
		if a.ImportDeposit != nil {
			d := uint64(*a.ImportDeposit)
			deposit = &d
		}
		ls.stageImportBaseline(txn, ImportBaseline{
			Ref: models.NewStakeCredentialRef(
				a.CredentialTag,
				a.StakingKey,
			),
			Active:    true,
			AddedSlot: a.AddedSlot,
			Deposit:   deposit,
			Pool:      a.Pool,
			Drep:      a.Drep,
			DrepType:  a.DrepType,
		})
	}
	return nil
}

func (ls *LedgerState) stageGenesisPools(
	txn ledgerdb.Txn,
	pools map[string]lcommon.PoolRegistrationCertificate,
	poolDeposit uint64,
) error {
	r := ls.poolReader(txn)
	if r == nil {
		return nil
	}
	certs := make([]lcommon.PoolRegistrationCertificate, 0, len(pools))
	for _, c := range pools {
		certs = append(certs, c)
	}
	slices.SortFunc(certs, func(a, b lcommon.PoolRegistrationCertificate) int {
		return bytes.Compare(a.Operator[:], b.Operator[:])
	})
	for i := range certs {
		reg, err := models.PoolRegistrationFromCertificate(&certs[i], 0)
		if err != nil {
			return err
		}
		prev, err := ls.poolView(r, txn, reg.PoolKeyHash)
		if err != nil {
			return err
		}
		if prev != nil {
			continue
		}
		if len(reg.LeiosKeyPublic) > 0 {
			// The reference ledger registers a genesis pool's key in its
			// initial epoch.
			genesisEpoch := uint64(0)
			reg.LeiosKeyRegistrationEpoch = &genesisEpoch
		}
		deposit := poolDeposit
		ls.stagePool(txn, 0, PoolState{
			Pool:        bytes.Clone(reg.PoolKeyHash),
			Params:      CleanPoolRegistration(&reg),
			DepositHeld: &deposit,
		}, false)
		// The registration is also the pool's first certificate: the reads
		// of the pool's history, which find a pool and its parameters at a
		// slot, know a genesis pool only by it.
		event := reg
		ls.stagePoolEvent(txn, reg.PoolKeyHash, PoolEvent{
			Slot: 0, Position: uint64(i), Registration: &event,
		})
	}
	return nil
}

// stageGenesisGovernance stages, at slot 0, what the Conway genesis imports:
// each initial DRep registered with its deposit, and each delegation
// upserted into its account with a witness at slot 0 and, to a DRep
// credential, a live delegator link.
func (ls *LedgerState) stageGenesisGovernance(
	txn ledgerdb.Txn,
	initialDReps conway.ConwayGenesisInitialDReps,
	delegs conway.ConwayGenesisDelegs,
) {
	r := ls.accountReader(txn)
	if r == nil || ls.utxoSetFailed.Load() {
		return
	}
	dr := ls.drepReader(txn)
	if dr != nil {
		if err := ls.stageGenesisDReps(dr, txn, initialDReps); err != nil {
			ls.failAccountStaging(txn, err)
			return
		}
	}
	if err := ls.stageGenesisDelegations(r, dr, txn, delegs); err != nil {
		ls.failAccountStaging(txn, err)
	}
}

func sortedGenesisCredentials[V any](
	m map[*lcommon.Credential]V,
) []*lcommon.Credential {
	creds := make([]*lcommon.Credential, 0, len(m))
	for c := range m {
		if c != nil {
			creds = append(creds, c)
		}
	}
	slices.SortFunc(creds, func(a, b *lcommon.Credential) int {
		return cmp.Or(
			cmp.Compare(a.CredType, b.CredType),
			bytes.Compare(a.Credential[:], b.Credential[:]),
		)
	})
	return creds
}

func (ls *LedgerState) stageGenesisDReps(
	dr DRepReader,
	txn ledgerdb.Txn,
	initialDReps conway.ConwayGenesisInitialDReps,
) error {
	hr := ls.registrationHistoryReader(txn)
	for _, cred := range sortedGenesisCredentials(initialDReps) {
		drep, registration, err := models.GenesisDRep(cred, initialDReps[cred])
		if err != nil {
			return err
		}
		prev, err := ls.drepView(dr, txn, drep.CredentialTag, drep.Credential)
		if err != nil {
			return err
		}
		if prev == nil || !sameDrep(*prev, drep) {
			ls.stageDrep(txn, 0, drep)
		}
		// ImportDrep records the expiry history whether or not the row
		// changed, so a set that holds the DRep without its history gets it.
		ls.stageDrepExpiryHistory(txn, drep.AddedSlot, drep)
		if hr == nil {
			continue
		}
		deposit := uint64(registration.DepositAmount)
		got, err := ls.drepDepositView(
			hr,
			txn,
			drep.CredentialTag,
			drep.Credential,
		)
		if err != nil {
			return err
		}
		if !sameDepositPtr(&deposit, got) {
			ls.stageDRepDeposit(
				txn,
				0,
				drep.CredentialTag,
				drep.Credential,
				&deposit,
			)
		}
	}
	return nil
}

func (ls *LedgerState) stageGenesisDelegations(
	r AccountReader,
	dr DRepReader,
	txn ledgerdb.Txn,
	delegs conway.ConwayGenesisDelegs,
) error {
	for _, cred := range sortedGenesisCredentials(delegs) {
		tag, err := models.CredentialTagFromUint(cred.CredType)
		if err != nil {
			return err
		}
		effect, err := models.GenesisDelegateeOf(delegs[cred])
		if err != nil {
			return err
		}
		key := cred.Credential[:]
		ref := models.NewStakeCredentialRef(tag, bytes.Clone(key))
		prev, err := ls.accountView(r, txn, tag, key)
		if err != nil {
			return err
		}
		// The import inserts a new active account at slot 0, or upserts the
		// delegation and the active flag of an existing one.
		next := models.Account{StakingKey: ref.Key, CredentialTag: tag}
		if prev != nil {
			next = *prev
		}
		next.Pool, next.Drep, next.DrepType = effect.Pool, effect.Drep, effect.DrepType
		next.Active = true
		if prev == nil || !sameAccount(*prev, next) {
			ls.stageLedgerChanges(
				txn,
				LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
					Accounts: []AccountChange{{Slot: 0, Account: next}},
				},
			)
		}
		ls.stageWitness(txn, ref, 0, 0, false)
		if dr == nil || len(effect.Drep) == 0 {
			continue
		}
		drepTag := uint8(effect.DrepType) //nolint:gosec // GenesisDelegateeOf bounds a credential DRep type to 0 or 1
		drepRef := models.NewStakeCredentialRef(drepTag, effect.Drep)
		live, err := ls.drepLinkView(dr, txn, drepRef, ref)
		if err != nil {
			return err
		}
		if !live {
			ls.stageDrepLink(txn, 0, drepRef, ref, true)
		}
	}
	return nil
}
