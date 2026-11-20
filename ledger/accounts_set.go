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
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/common"
)

// AccountReader serves accounts from a live UTxO set that keeps them. The
// set computes account transitions itself rather than copying them.
type AccountReader interface {
	AccountsLoaded() (bool, error)
	// Account returns the account of (tag, key), active or not, or nil.
	Account(tag uint8, key []byte) (*models.Account, error)
	// AccountAt returns the account of (tag, key) as a rollback to slot
	// would leave it, or nil.
	AccountAt(tag uint8, key []byte, slot uint64) (*models.Account, error)
	// HasRewardRecord reports whether the reader applied r.
	HasRewardRecord(r RewardRecord) (bool, error)
	// AccountsByPool returns the active accounts delegating to pool.
	AccountsByPool(pool []byte) ([]models.StakeCredentialRef, error)
	// ActiveAccountRefs returns the credentials of every active account.
	ActiveAccountRefs() ([]models.StakeCredentialRef, error)
	// AccountsByDrep returns the active accounts delegating to the DRep of
	// typ and drep.
	AccountsByDrep(typ uint64, drep []byte) ([]models.StakeCredentialRef, error)
	// AccountsWithDrep returns every account, active or not, that names a
	// DRep.
	AccountsWithDrep() ([]models.StakeCredentialRef, error)
}

// failAccountStaging stops the live UTxO set from following the ledger
// when it cannot apply an account change the metadata store accepted: the
// two disagree, and startup rebuilds or resumes the set.
func (ls *LedgerState) failAccountStaging(err error) {
	ls.failUtxoSet(
		0,
		"live UTxO set cannot apply an account change; it stops following the ledger until startup resumes it",
		err,
	)
}

// failDrainedAccountStaging stops the set when the accounts family is drained
// and an account change cannot be staged: the SQL write was skipped, so the
// change would be lost.
func (ls *LedgerState) failDrainedAccountStaging() {
	if !ls.utxoSetFailed.Load() && ls.familyDrained(nodesettings.FamilyAccounts) {
		ls.failAccountStaging(drainedUnserved(nodesettings.FamilyAccounts))
	}
}

// rewardRecordView reports whether rec applies as txn sees the set: staged
// in a batch it sees and not dropped by a later staged rollback, or
// committed at or before every staged rollback.
func (ls *LedgerState) rewardRecordView(
	r AccountReader,
	txn ledgerdb.Txn,
	rec RewardRecord,
) (bool, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ops := ls.stagedEpochStateOps(txn)
	same := func(o RewardRecord) bool {
		return o.Slot == rec.Slot && o.Withdrawal == rec.Withdrawal &&
			bytes.Equal(o.Source, rec.Source) && o.Ref.Tag == rec.Ref.Tag &&
			bytes.Equal(o.Ref.Key, rec.Ref.Key)
	}
	committed := true
	staged := false
	for _, o := range ops {
		if o.rollbackTo != nil && rec.Slot > *o.rollbackTo {
			committed, staged = false, false
		}
		for _, sr := range o.rewardRecords {
			if same(sr) {
				staged = true
			}
		}
	}
	if staged {
		return true, nil
	}
	if !committed {
		return false, nil
	}
	return r.HasRewardRecord(rec)
}

// stageRewardChange stages a reward credit, or a withdrawal, of amount for
// ref at slot, unless rec says it was applied already, as the metadata
// store's reward journal makes each apply once.
func (ls *LedgerState) stageRewardChange(
	r AccountReader,
	txn ledgerdb.Txn,
	rec RewardRecord,
	amount uint64,
) error {
	if amount == 0 {
		return nil
	}
	done, err := ls.rewardRecordView(r, txn, rec)
	if err != nil || done {
		return err
	}
	prev, err := ls.accountView(r, txn, rec.Ref.Tag, rec.Ref.Key)
	if err != nil {
		return err
	}
	if prev == nil || (!rec.Withdrawal && !prev.Active) {
		return fmt.Errorf(
			"%w: reward change for %x", models.ErrAccountNotFound, rec.Ref.Key,
		)
	}
	rec.Amount = amount
	next := *prev
	balance := uint64(prev.Reward)
	if rec.Withdrawal {
		if amount > balance {
			return fmt.Errorf(
				"%w: withdrawal of %d exceeds the set's balance %d for %x",
				models.ErrRewardWithdrawalExceedsBalance,
				amount, balance, rec.Ref.Key,
			)
		}
		next.Reward = types.Uint64(balance - amount)
	} else {
		if balance > ^uint64(0)-amount {
			return fmt.Errorf("account reward overflow for %x", rec.Ref.Key)
		}
		next.Reward = types.Uint64(balance + amount)
	}
	ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		Accounts:      []AccountChange{{Slot: rec.Slot, Account: next}},
		RewardRecords: []RewardRecord{rec},
	})
	return nil
}

// stageRewardCredits stages credits, in order, for a set that keeps
// accounts; a failure stops the set following the ledger.
func (ls *LedgerState) stageRewardCredits(
	txn ledgerdb.Txn,
	credits []models.AccountRewardCredit,
) {
	ls.stageRewardCreditsAs(txn, credits, false)
}

// stageRewardCreditsAs is stageRewardCredits for credits a boundary applied
// after its SNAP point when postSnapshot is set.
func (ls *LedgerState) stageRewardCreditsAs(
	txn ledgerdb.Txn,
	credits []models.AccountRewardCredit,
	postSnapshot bool,
) {
	r := ls.accountReader(txn)
	if r == nil || ls.utxoSetFailed.Load() {
		ls.failDrainedAccountStaging()
		return
	}
	for _, c := range credits {
		source := c.SourceHash
		if source == nil {
			source = []byte{}
		}
		err := ls.stageRewardChange(r, txn, RewardRecord{
			Slot:         c.Slot,
			Source:       source,
			Ref:          models.NewStakeCredentialRef(c.CredentialTag, c.StakingKey),
			PostSnapshot: postSnapshot,
		}, c.Amount)
		if err != nil {
			ls.failAccountStaging(err)
			return
		}
	}
}

// accountReader returns the set's account reader when the set keeps
// accounts.
func (ls *LedgerState) accountReader(txn ledgerdb.Txn) AccountReader {
	if !ls.keepsEpochState() {
		return nil
	}
	r, ok := ls.readSet(txn).(AccountReader)
	if !ok {
		return nil
	}
	return r
}

// setServes reports whether a family's reads are answered from the set
// rather than the metadata store: the set follows the ledger and holds
// every row of the family. loaded
// caches the answer of check.
func (ls *LedgerState) setServes(
	loaded *atomic.Bool,
	check func() (bool, error),
) bool {
	if _, mode := ls.utxoSetReader(); !mode ||
		ls.utxoSetFailed.Load() {
		return false
	}
	if loaded.Load() {
		return true
	}
	ok, err := check()
	if err != nil || !ok {
		return false
	}
	loaded.Store(true)
	return true
}

// accountServing returns the account reader when account reads are answered
// from the set, or nil when they stay on the metadata store.
func (ls *LedgerState) accountServing(txn ledgerdb.Txn) AccountReader {
	r := ls.accountReader(txn)
	if r == nil || !ls.setServes(&ls.accountsSetLoaded, r.AccountsLoaded) {
		return nil
	}
	return r
}

// accountView returns the account of (tag, key) as txn sees the set: its
// committed state, under the rollbacks and changes of the batches txn sees,
// in order. A staged rollback drops the staged changes after it and reads
// the committed account as the rollback will leave it.
func (ls *LedgerState) accountView(
	r AccountReader,
	txn ledgerdb.Txn,
	tag uint8,
	key []byte,
) (*models.Account, error) {
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ops := ls.stagedEpochStateOps(txn)
	var ceiling *uint64
	var staged []AccountChange
	for _, o := range ops {
		if o.rollbackTo != nil {
			to := *o.rollbackTo
			if ceiling == nil || to < *ceiling {
				ceiling = &to
			}
			kept := staged[:0]
			for _, c := range staged {
				if c.Slot <= to {
					kept = append(kept, c)
				}
			}
			staged = kept
		}
		for _, c := range o.accounts {
			if c.Account.CredentialTag == tag &&
				bytes.Equal(c.Account.StakingKey, key) {
				staged = append(staged, c)
			}
		}
	}
	if len(staged) > 0 {
		a := staged[len(staged)-1].Account
		return &a, nil
	}
	if ceiling != nil {
		return r.AccountAt(tag, key, *ceiling)
	}
	return r.Account(tag, key)
}

// stageAccountTransaction stages the account changes a valid transaction
// makes, as the metadata store applies them: its withdrawals, then its
// account certificates in order, each to the account as the transaction and
// the batches txn sees left it. A historical backfill records withdrawals
// without changing the balance, so it stages none. A withdrawal larger than
// the balance is returned as models.ErrRewardWithdrawalExceedsBalance, the
// verdict the metadata store's withdrawal write gave, and fails the
// transaction; any other failure stops the set following the ledger.
func (ls *LedgerState) stageAccountTransaction(
	txn ledgerdb.Txn,
	tx lcommon.Transaction,
	slot uint64,
	txIndex uint32,
	blockHash []byte,
	opts database.BatchedTxIngestOpts,
	certDeposits map[int]uint64,
) error {
	r := ls.accountReader(txn)
	if !tx.IsValid() {
		return nil
	}
	if r == nil || ls.utxoSetFailed.Load() {
		ls.failDrainedAccountStaging()
		return nil
	}
	err := ls.stageAccountTransactionErr(
		r, txn, tx, slot, txIndex, blockHash, opts, certDeposits,
	)
	if errors.Is(err, models.ErrRewardWithdrawalExceedsBalance) {
		return err
	}
	if err != nil {
		ls.failAccountStaging(err)
	}
	return nil
}

func (ls *LedgerState) stageAccountTransactionErr(
	r AccountReader,
	txn ledgerdb.Txn,
	tx lcommon.Transaction,
	slot uint64,
	txIndex uint32,
	blockHash []byte,
	opts database.BatchedTxIngestOpts,
	certDeposits map[int]uint64,
) error {
	protocolMajor := opts.ProtocolMajor
	effectiveSlot := slot
	if opts.LedgerContextSlot != nil {
		effectiveSlot = *opts.LedgerContextSlot
	}
	dr := ls.drepReader(txn)
	pr := ls.poolReader(txn)
	for address, amount := range tx.Withdrawals() {
		if address == nil || amount == nil {
			continue
		}
		if amount.Sign() < 0 || !amount.IsUint64() {
			return fmt.Errorf("invalid reward withdrawal amount %s", amount)
		}
		tag, ok := models.StakeCredentialTagFromAddress(*address)
		if !ok {
			return errors.New("derive reward withdrawal credential tag")
		}
		key := address.StakeKeyHash()
		ref := models.NewStakeCredentialRef(tag, key.Bytes())
		// The metadata store witnesses a withdrawal through the reward
		// journal row of a positive one, and through the withdrawal witness
		// row unless the write is elided, which keeps a zero one.
		if amount.Sign() > 0 || !opts.SkipWithdrawalWitnessWrite {
			ls.stageWitness(txn, ref, slot, effectiveSlot, true)
		}
		if opts.HistoricalBackfill {
			continue
		}
		if err := ls.stageRewardChange(r, txn, RewardRecord{
			Slot:       slot,
			Withdrawal: true,
			Source:     tx.Hash().Bytes(),
			Ref:        ref,
		}, amount.Uint64()); err != nil {
			return err
		}
	}
	for certIndex, cert := range tx.Certificates() {
		if isGenesisMIR, err := ls.stageGenesisMIRCertificate(
			txn, cert, slot, txIndex, certIndex,
		); err != nil {
			return err
		} else if isGenesisMIR {
			continue
		}
		if pr != nil {
			var deposit *uint64
			if d, ok := certDeposits[certIndex]; ok {
				deposit = &d
			}
			isPool, err := ls.stagePoolCertificate(
				pr, txn, cert, slot, effectiveSlot,
				PoolEventPosition(txIndex, uint32(certIndex)), //nolint:gosec // a transaction holds far fewer than 2^32 certificates
				deposit,
			)
			if err != nil {
				return err
			}
			if isPool {
				continue
			}
		}
		if isCommittee, err := ls.stageCommitteeCertificate(txn, cert, slot); err != nil {
			return err
		} else if isCommittee {
			continue
		}
		if dcred, kind, url, hash, isDrep := models.DrepCertificateEffect(cert); isDrep {
			if dr == nil {
				continue
			}
			tag, err := models.CredentialTagFromUint(dcred.CredType)
			if err != nil {
				return err
			}
			if err := ls.stageDrepCertificate(
				r, dr, txn, tag, dcred.Credential[:], kind, url, hash,
				slot, protocolMajor,
			); err != nil {
				return err
			}
			if kind == models.DrepCertificateRegistration {
				var deposit *uint64
				if d, ok := certDeposits[certIndex]; ok {
					deposit = &d
				}
				ls.stageDRepDeposit(txn, slot, tag, dcred.Credential[:], deposit)
			}
			continue
		}
		cred, effect, isAccount, err := models.AccountCertificateEffect(cert)
		if err != nil {
			return err
		}
		if !isAccount {
			continue
		}
		tag, err := models.CredentialTagFromUint(cred.CredType)
		if err != nil {
			return err
		}
		key := cred.Credential[:]
		ls.stageWitness(
			txn, models.NewStakeCredentialRef(tag, key), slot, effectiveSlot,
			false,
		)
		if ev, ok := stakeCertificateEvent(cert, certDepositAt(certDeposits, certIndex)); ok {
			ev.Slot, ev.TxSlot = slot, slot
			ev.EffectiveSlot = poolEventEffectiveSlot(slot, effectiveSlot)
			ev.Position = PoolEventPosition(txIndex, uint32(certIndex)) //nolint:gosec // a transaction holds far fewer than 2^32 certificates
			ev.TxHash, ev.BlockHash = tx.Hash().Bytes(), bytes.Clone(blockHash)
			ls.stageStakeEvent(txn, models.NewStakeCredentialRef(tag, key), ev)
		}
		prev, err := ls.accountView(r, txn, tag, key)
		if err != nil {
			return fmt.Errorf("read account for certificate: %w", err)
		}
		next := models.Account{
			StakingKey:    bytes.Clone(key),
			CredentialTag: tag,
			AddedSlot:     slot,
			CreatedSlot:   slot,
		}
		var prevDelegation *models.AccountDelegation
		if prev != nil {
			next.Reward = prev.Reward
			next.ExpirationEpoch = prev.ExpirationEpoch
			next.CreatedSlot = min(prev.CreatedSlot, slot)
			prevDelegation = &models.AccountDelegation{
				Pool: prev.Pool, Drep: prev.Drep, DrepType: prev.DrepType,
				Active: prev.Active,
			}
		}
		if len(effect.Pool) > 0 {
			ls.stageDelegationEvent(
				txn, models.NewStakeCredentialRef(tag, key), slot,
				effectiveSlot,
				PoolEventPosition(txIndex, uint32(certIndex)), //nolint:gosec // a transaction holds far fewer than 2^32 certificates
				effect.Pool,
			)
		}
		d := models.ApplyAccountCertificate(prevDelegation, effect)
		next.Pool, next.Drep, next.DrepType, next.Active = d.Pool, d.Drep, d.DrepType, d.Active
		ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			Accounts: []AccountChange{{Slot: slot, Account: next}},
		})
		if dr != nil && models.ChangesDrepDelegation(cert) {
			if err := ls.stageDrepLinkUpdate(
				dr, txn, models.NewStakeCredentialRef(tag, key), prev,
				effect, slot, protocolMajor,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// certDepositAt returns the deposit computed for the certificate at index,
// nil when none was.
func certDepositAt(deposits map[int]uint64, index int) *uint64 {
	if d, ok := deposits[index]; ok {
		return &d
	}
	return nil
}

func (s stagingLedgerDB) SetTransactionWithOpts(
	ctx context.Context,
	tx lcommon.Transaction,
	point common.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	offsets *database.BlockIngestionResult,
	txn ledgerdb.Txn,
	opts database.BatchedTxIngestOpts,
) error {
	if err := s.LedgerDB.SetTransactionWithOpts(
		ctx, tx, point, idx, updateEpoch, pparamUpdates, certDeposits,
		offsets, txn, opts,
	); err != nil {
		return err
	}
	if err := s.ls.stageAccountTransaction( //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		txn, tx, point.Slot, idx, point.Hash, opts, certDeposits,
	); err != nil {
		return err
	}
	s.ls.stageTransactionFee(txn, tx, point.Slot, opts) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context

	s.ls.stagePParamUpdates(txn, tx, point.Slot, idx, updateEpoch, pparamUpdates) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	return nil
}

func (s stagingLedgerDB) SetTransactionBatchedWithOpts(
	ctx context.Context,
	tx lcommon.Transaction,
	point common.Point,
	idx uint32,
	updateEpoch uint64,
	pparamUpdates map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
	certDeposits map[int]uint64,
	offsets *database.BlockIngestionResult,
	acc database.BatchAccumulator,
	txn ledgerdb.Txn,
	opts database.BatchedTxIngestOpts,
) error {
	if err := s.LedgerDB.SetTransactionBatchedWithOpts(
		ctx, tx, point, idx, updateEpoch, pparamUpdates, certDeposits,
		offsets, acc, txn, opts,
	); err != nil {
		return err
	}
	if err := s.ls.stageAccountTransaction( //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		txn, tx, point.Slot, idx, point.Hash, opts, certDeposits,
	); err != nil {
		return err
	}
	s.ls.stageTransactionFee(txn, tx, point.Slot, opts) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context

	s.ls.stagePParamUpdates(txn, tx, point.Slot, idx, updateEpoch, pparamUpdates) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	return nil
}

// requireRewardCreditAccounts reports models.ErrAccountNotFound for the
// first credit whose account the set does not hold active, as the metadata
// store's credit does, for a drained accounts family whose store credit is
// skipped. Callers route the amount elsewhere (the treasury) on that answer,
// so it must reach them without staging anything or stopping the set.
// skipApplied skips a credit already applied, as the store's batch credit
// does before it looks the account up; the single credit looks the account
// up first and passes false.
func (ls *LedgerState) requireRewardCreditAccounts(
	txn ledgerdb.Txn,
	credits []models.AccountRewardCredit,
	skipApplied bool,
) error {
	r := ls.accountReader(txn)
	if r == nil || ls.utxoSetFailed.Load() {
		return nil
	}
	for _, c := range credits {
		if c.Amount == 0 {
			continue
		}
		if skipApplied {
			source := c.SourceHash
			if source == nil {
				source = []byte{}
			}
			done, err := ls.rewardRecordView(r, txn, RewardRecord{
				Slot:   c.Slot,
				Source: source,
				Ref:    models.NewStakeCredentialRef(c.CredentialTag, c.StakingKey),
			})
			if err != nil {
				return err
			}
			if done {
				continue
			}
		}
		a, err := ls.accountView(r, txn, c.CredentialTag, c.StakingKey)
		if err != nil {
			return err
		}
		if a == nil || !a.Active {
			return models.ErrAccountNotFound
		}
	}
	return nil
}

func (s stagingLedgerDB) AddAccountRewardByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	txn ledgerdb.Txn,
) error {
	if !s.accountsDrained() {
		if err := s.LedgerDB.AddAccountRewardByCredential(
			ctx, credentialTag, stakeKey, amount, slot, sourceHash, txn,
		); err != nil {
			return err
		}
	} else if err := s.ls.requireRewardCreditAccounts(txn, []models.AccountRewardCredit{{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		StakingKey: stakeKey, CredentialTag: credentialTag, Amount: amount,
	}}, false); err != nil {
		return err
	}
	s.ls.stageRewardCredits(txn, []models.AccountRewardCredit{{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		StakingKey: stakeKey, CredentialTag: credentialTag, Amount: amount,
		Slot: slot, SourceHash: sourceHash,
	}})
	return nil
}

func (s stagingLedgerDB) AddPostSnapshotAccountRewardByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	amount uint64,
	slot uint64,
	sourceHash []byte,
	txn ledgerdb.Txn,
) error {
	if !s.accountsDrained() {
		if err := s.LedgerDB.AddPostSnapshotAccountRewardByCredential(
			ctx, credentialTag, stakeKey, amount, slot, sourceHash, txn,
		); err != nil {
			return err
		}
	} else if err := s.ls.requireRewardCreditAccounts(txn, []models.AccountRewardCredit{{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		StakingKey: stakeKey, CredentialTag: credentialTag, Amount: amount,
	}}, false); err != nil {
		return err
	}
	s.ls.stageRewardCreditsAs(txn, []models.AccountRewardCredit{{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		StakingKey: stakeKey, CredentialTag: credentialTag, Amount: amount,
		Slot: slot, SourceHash: sourceHash,
	}}, true)
	if amount > 0 {
		// The store marks the credit even when its journal row existed, so
		// the set records it whether or not the account change applied.
		s.ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			PostSnapshotCredits: []PostSnapshotCredit{{
				Slot:   slot,
				Ref:    models.NewStakeCredentialRef(credentialTag, bytes.Clone(stakeKey)),
				Source: append([]byte{}, sourceHash...),
				Amount: amount,
			}},
		})
	}
	return nil
}

func (s stagingLedgerDB) AddAccountRewardsByCredential(
	ctx context.Context,
	credits []models.AccountRewardCredit,
	txn ledgerdb.Txn,
) error {
	if !s.accountsDrained() {
		if err := s.LedgerDB.AddAccountRewardsByCredential(ctx, credits, txn); err != nil {
			return err
		}
	} else if err := s.ls.requireRewardCreditAccounts(txn, credits, true); err != nil { //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
		return err
	}
	s.ls.stageRewardCredits(txn, credits) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	return nil
}

// stagePoolReap stages the clearing of every delegation to a reaped pool,
// stamped with the boundary slot, as ClearDelegationsToRetiredPool does in
// the metadata store: the accounts the set's pool index names, and those
// the batches txn sees staged to it.
func (ls *LedgerState) stagePoolReap(
	txn ledgerdb.Txn,
	pool []byte,
	slot uint64,
) {
	r := ls.accountReader(txn)
	if len(pool) == 0 {
		return
	}
	if r == nil || ls.utxoSetFailed.Load() {
		ls.failDrainedAccountStaging()
		return
	}
	refs, err := r.AccountsByPool(pool)
	if err != nil {
		ls.failAccountStaging(err)
		return
	}
	refs, err = ls.accountCandidates(r, txn, refs, refSet(refs))
	if err != nil {
		ls.failAccountStaging(err)
		return
	}
	for _, ref := range refs {
		a, err := ls.accountView(r, txn, ref.Tag, ref.Key)
		if err != nil {
			ls.failAccountStaging(err)
			return
		}
		if a == nil || !bytes.Equal(a.Pool, pool) {
			continue
		}
		next := *a
		next.Pool = nil
		next.AddedSlot = slot
		ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			Accounts: []AccountChange{{Slot: slot, Account: next}},
		})
	}
}

func (s stagingLedgerDB) ClearDelegationsToRetiredPool(
	poolKeyHash []byte,
	boundarySlot uint64,
	txn ledgerdb.Txn,
) error {
	if !s.accountsDrained() {
		if err := s.LedgerDB.ClearDelegationsToRetiredPool(
			poolKeyHash, boundarySlot, txn,
		); err != nil {
			return err
		}
	}
	s.ls.stagePoolReap(txn, poolKeyHash, boundarySlot)
	return nil
}

// stageAccountExpirations stages a CIP-0163 expiration for refs' accounts,
// active or not, as RenewAccountExpirations does, or with nil refs for
// every active account, as StampAllActiveAccountExpirations does, at slot.
func (ls *LedgerState) stageAccountExpirations(
	txn ledgerdb.Txn,
	refs []models.StakeCredentialRef,
	expiration uint64,
	slot uint64,
) {
	r := ls.accountReader(txn)
	if r == nil && ls.familyDrained(nodesettings.FamilyAccounts) {
		// The SQL write is skipped for a drained family, so an expiration
		// with no set to take it would be lost.
		ls.failAccountStaging(drainedUnserved(nodesettings.FamilyAccounts))
		return
	}
	if r == nil || ls.utxoSetFailed.Load() {
		ls.failDrainedAccountStaging()
		return
	}
	activeOnly := refs == nil
	if activeOnly {
		committed, err := r.ActiveAccountRefs()
		if err != nil {
			ls.failAccountStaging(err)
			return
		}
		committed, err = ls.accountCandidates(r, txn, committed, refSet(committed))
		if err != nil {
			ls.failAccountStaging(err)
			return
		}
		refs = committed
	}
	for _, ref := range refs {
		a, err := ls.accountView(r, txn, ref.Tag, ref.Key)
		if err != nil {
			ls.failAccountStaging(err)
			return
		}
		if a == nil || (activeOnly && !a.Active) {
			continue
		}
		next := *a
		next.ExpirationEpoch = expiration
		ls.stageLedgerChanges(txn, LedgerChanges{ //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
			Accounts: []AccountChange{{Slot: slot, Account: next}},
		})
	}
}

func sameDrep(a, b models.Drep) bool {
	return a.CredentialTag == b.CredentialTag &&
		bytes.Equal(a.Credential, b.Credential) &&
		a.AnchorURL == b.AnchorURL && bytes.Equal(a.AnchorHash, b.AnchorHash) &&
		a.AddedSlot == b.AddedSlot && a.Active == b.Active &&
		a.LastActivityEpoch == b.LastActivityEpoch &&
		a.ExpiryEpoch == b.ExpiryEpoch
}

func sameAccount(a, b models.Account) bool {
	return a.CredentialTag == b.CredentialTag &&
		bytes.Equal(a.StakingKey, b.StakingKey) &&
		bytes.Equal(a.Pool, b.Pool) && bytes.Equal(a.Drep, b.Drep) &&
		a.DrepType == b.DrepType && a.Active == b.Active &&
		a.Reward == b.Reward && a.AddedSlot == b.AddedSlot &&
		a.CreatedSlot == b.CreatedSlot &&
		a.ExpirationEpoch == b.ExpirationEpoch
}

func (s stagingLedgerDB) SetGenesisStakingWithDeposits(
	pools map[string]lcommon.PoolRegistrationCertificate,
	stakeDelegations map[string]string,
	keyDeposit uint64,
	poolDeposit uint64,
	blockHash []byte,
	txn ledgerdb.Txn,
) error {
	// The metadata store skips the tables of a drained family, so the set
	// staged below is then the only copy of the genesis staking.
	if s.accountsDrained() && s.ls.accountReader(txn) == nil {
		return drainedUnserved(nodesettings.FamilyAccounts)
	}
	if err := s.LedgerDB.SetGenesisStakingWithDeposits(
		pools, stakeDelegations, keyDeposit, poolDeposit, blockHash, txn,
	); err != nil {
		return err
	}
	s.ls.stageGenesisStaking(
		txn,
		pools,
		stakeDelegations,
		keyDeposit,
		poolDeposit,
	)
	return nil
}

func (s stagingLedgerDB) SetGenesisGovernance(
	initialDReps conway.ConwayGenesisInitialDReps,
	delegs conway.ConwayGenesisDelegs,
	blockHash []byte,
	txn ledgerdb.Txn,
) error {
	// The metadata store skips the tables of a drained family, so the set
	// staged below is then the only copy of the genesis DReps and
	// delegations.
	if s.accountsDrained() && s.ls.accountReader(txn) == nil {
		return drainedUnserved(nodesettings.FamilyAccounts)
	}
	if s.drepsDrained() && s.ls.drepReader(txn) == nil {
		return drainedUnserved(nodesettings.FamilyDReps)
	}
	if err := s.LedgerDB.SetGenesisGovernance(
		initialDReps, delegs, blockHash, txn,
	); err != nil {
		return err
	}
	s.ls.stageGenesisGovernance(txn, initialDReps, delegs)
	return nil
}

func (s stagingLedgerDB) GetAccountByCredential(
	ctx context.Context,
	credentialTag uint8,
	stakeKey []byte,
	includeInactive bool,
	txn ledgerdb.Txn,
) (*models.Account, error) {
	if r := s.ls.accountServing(txn); r != nil {
		a, err := s.ls.accountView(r, txn, credentialTag, stakeKey)
		if err != nil {
			return nil, err
		}
		if a == nil || !includeInactive && !a.Active {
			return nil, models.ErrAccountNotFound
		}
		return a, nil
	}
	return nil, s.accountsUnserved()
}

func (s stagingLedgerDB) GetAccountsByCredential(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	includeInactive bool,
	txn ledgerdb.Txn,
) (map[string]*models.Account, error) {
	if r := s.ls.accountServing(txn); r != nil {
		out := make(map[string]*models.Account, len(refs))
		for _, ref := range refs {
			a, err := s.ls.accountView(r, txn, ref.Tag, ref.Key)
			if err != nil {
				return nil, err
			}
			if a != nil && (includeInactive || a.Active) {
				out[ref.MapKey()] = a
			}
		}
		return out, nil
	}
	return nil, s.accountsUnserved()
}

// accountsUnserved is the error of an account read the set cannot answer.
func (s stagingLedgerDB) accountsUnserved() error {
	if s.accountsDrained() {
		return drainedAccountsRefusal()
	}
	return s.ls.setFamilyUnserved(nodesettings.FamilyAccounts)
}

func (s stagingLedgerDB) accountsDrained() bool {
	return s.ls.familyDrained(nodesettings.FamilyAccounts)
}

func (s stagingLedgerDB) drepsDrained() bool {
	return s.ls.familyDrained(nodesettings.FamilyDReps)
}

// RenewAccountExpirations skips the SQL write of a drained accounts family;
// callers stage the same expiration for the set.
func (s stagingLedgerDB) RenewAccountExpirations(
	ctx context.Context,
	refs []models.StakeCredentialRef,
	expirationEpoch uint64,
	txn ledgerdb.Txn,
) error {
	if s.accountsDrained() {
		return nil
	}
	return s.LedgerDB.RenewAccountExpirations(ctx, refs, expirationEpoch, txn)
}

// StampAllActiveAccountExpirations skips the SQL write of a drained accounts
// family and reports no stamped count; the caller stages the stamp for the
// set, which does not count it.
func (s stagingLedgerDB) StampAllActiveAccountExpirations(
	ctx context.Context,
	expirationEpoch uint64,
	txn ledgerdb.Txn,
) (int64, error) {
	if s.accountsDrained() {
		return 0, nil
	}
	return s.LedgerDB.StampAllActiveAccountExpirations(ctx, expirationEpoch, txn)
}
