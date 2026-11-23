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
	"log/slog"
	"maps"
	"math"
	"reflect"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ImportLedgerDB is the LedgerDB a ledger-state import writes a snapshot
// through. It is the ledger's own LedgerDB over a LedgerDB set that no ledger
// is running on, with the writes only an import makes: the rows of a
// snapshot carry no certificate, so they are written as the state a
// certificate would have left.
type ImportLedgerDB struct {
	stagingLedgerDB
	failed atomic.Pointer[error]
}

// NewImportLedgerDB returns the LedgerDB an import writes set through. db
// holds what is not ledger state: blocks, the chain tip and sync bookkeeping.
func NewImportLedgerDB(
	db *database.Database,
	set UtxoSetWriter,
	logger *slog.Logger,
) *ImportLedgerDB {
	if logger == nil {
		logger = slog.Default()
	}
	s := &ImportLedgerDB{}
	s.stagingLedgerDB = stagingLedgerDB{
		LedgerDB: dbadapter.New(db),
		ls: &LedgerState{
			db: db,
			config: LedgerStateConfig{
				UtxoSet: set,
				Logger:  logger,
				// A write the set refuses after a commit stops the
				// import, which would otherwise finish without it.
				FatalErrorFunc: func(err error) {
					s.failed.CompareAndSwap(nil, &err)
				},
			},
		},
	}
	return s
}

// Err returns the first write the set refused, or nil. A refused write
// leaves the set short of what the import committed, so the import fails.
func (s *ImportLedgerDB) Err() error {
	if err := s.failed.Load(); err != nil {
		return *err
	}
	return nil
}

// ImportAccount writes account as the snapshot holds it, with the import
// baseline its later history is measured from. An account the set already
// holds keeps the slots and expiry it was first written with, and its
// inactivity activation membership and floor: a snapshot carries neither,
// and the reward guard reads the floor from the account.
func (s *ImportLedgerDB) ImportAccount(
	account *models.Account,
	txn ledgerdb.Txn,
) error {
	if account == nil {
		return errors.New("import account: account is nil")
	}
	r := s.ls.accountReader(txn)
	if r == nil {
		return errors.New("import account: the LedgerDB set holds no accounts")
	}
	held, err := s.ls.accountView(
		r, txn, account.CredentialTag, account.StakingKey,
	)
	if err != nil {
		return fmt.Errorf("import account: %w", err)
	}
	next := *account
	if held != nil {
		next.AddedSlot = held.AddedSlot
		next.CreatedSlot = held.CreatedSlot
		next.ExpirationEpoch = held.ExpirationEpoch
		next.InactivityActivation = held.InactivityActivation
		next.InactivityActivationEpoch = held.InactivityActivationEpoch
	}
	baseline := ImportBaseline{
		Ref: models.NewStakeCredentialRef(
			account.CredentialTag, account.StakingKey,
		),
		Active:    account.Active,
		AddedSlot: account.AddedSlot,
		Pool:      account.Pool,
		Drep:      account.Drep,
		DrepType:  account.DrepType,
	}
	if account.ImportDeposit != nil {
		deposit := uint64(*account.ImportDeposit)
		baseline.Deposit = &deposit
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		Accounts: []AccountChange{{Slot: account.AddedSlot, Account: next}},
		ImportBaselines: []ImportBaselineChange{{
			Slot: account.AddedSlot, Baseline: baseline,
		}},
	})
	return nil
}

// ImportPool writes a pool's registration as the snapshot holds it. future
// is false for the parameters in force and true for a re-registration that
// takes effect at the next epoch boundary, which is written after them.
func (s *ImportLedgerDB) ImportPool(
	registration *models.PoolRegistration,
	future bool,
	txn ledgerdb.Txn,
) error {
	if registration == nil {
		return errors.New("import pool: registration is nil")
	}
	r := s.ls.poolReader(txn)
	if r == nil {
		return errors.New("import pool: the LedgerDB set holds no pools")
	}
	pool := registration.PoolKeyHash
	held, err := s.ls.poolView(r, txn, pool)
	if err != nil {
		return fmt.Errorf("import pool: %w", err)
	}
	params := CleanPoolRegistration(registration)
	st := PoolState{Pool: bytes.Clone(pool)}
	position := uint64(0)
	if future {
		if held == nil || held.Params == nil {
			return fmt.Errorf(
				"import pool: %x has future parameters and none in force", pool,
			)
		}
		st = *held
		st.Future = params
		position = 1
	} else {
		st.Params = params
		// A pool the snapshot lists with the parameters the set already
		// holds in force keeps the registration it was written with.
		if held != nil && sameRegistration(held.Params, params) {
			st.Params = held.Params
		}
		deposit := uint64(registration.DepositAmount)
		st.DepositHeld = &deposit
	}
	s.ls.stagePool(txn, registration.AddedSlot, st, false)
	s.ls.stagePoolEvent(txn, pool, PoolEvent{
		Slot:         registration.AddedSlot,
		Position:     position,
		Registration: params,
	})
	return nil
}

// RetirePools schedules pools to retire at epoch, as a retirement the
// snapshot holds pending does. slot is the snapshot's.
func (s *ImportLedgerDB) RetirePools(
	pools [][]byte,
	epoch, slot uint64,
	txn ledgerdb.Txn,
) error {
	r := s.ls.poolReader(txn)
	if r == nil {
		return errors.New("retire pools: the LedgerDB set holds no pools")
	}
	for _, pool := range pools {
		held, err := s.ls.poolView(r, txn, pool)
		if err != nil {
			return fmt.Errorf("retire pools: %w", err)
		}
		if held == nil {
			continue
		}
		st := *held
		retiring := epoch
		st.RetiringEpoch = &retiring
		s.ls.stagePool(txn, slot, st, false)
		s.ls.stagePoolEvent(txn, pool, PoolEvent{
			Slot:          slot,
			Position:      PoolEventSyntheticPosition,
			RetiringEpoch: epoch,
		})
	}
	return nil
}

// ImportDrep writes a DRep as the snapshot holds it, with its deposit and
// the accounts the snapshot lists as delegating to it. A DRep the set
// already holds keeps the slot and activity epoch it was written with.
func (s *ImportLedgerDB) ImportDrep(
	drep *models.Drep,
	deposit uint64,
	txn ledgerdb.Txn,
) error {
	if drep == nil {
		return errors.New("import drep: drep is nil")
	}
	r := s.ls.drepReader(txn)
	if r == nil {
		return errors.New("import drep: the LedgerDB set holds no DReps")
	}
	held, err := s.ls.drepView(r, txn, drep.CredentialTag, drep.Credential)
	if err != nil {
		return fmt.Errorf("import drep: %w", err)
	}
	slot := drep.AddedSlot
	next := *drep
	next.Delegators = nil
	if held != nil {
		next.AddedSlot = held.AddedSlot
		next.LastActivityEpoch = held.LastActivityEpoch
	}
	s.ls.stageDrep(txn, slot, next)
	s.ls.stageDrepExpiryHistory(txn, slot, next)
	if held == nil {
		// The registration predates the snapshot by an unknown span; at slot
		// zero it stands for any slot the set can be read as of.
		s.ls.stageDRepDeposit(
			txn, 0, drep.CredentialTag, drep.Credential, &deposit,
		)
	}
	ref := models.NewStakeCredentialRef(drep.CredentialTag, drep.Credential)
	for _, delegator := range drep.Delegators {
		s.ls.stageDrepLink(txn, slot, ref, delegator, true)
	}
	return nil
}

// SetImportedDormantDRepEpochs sets the count of consecutive dormant epochs
// to the snapshot's, as of slot.
func (s *ImportLedgerDB) SetImportedDormantDRepEpochs(
	epochs, slot uint64,
	txn ledgerdb.Txn,
) error {
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		Dormancy: []DormancyChange{{Slot: slot, Epochs: epochs}},
	})
	return nil
}

// importRowsWriter is a LedgerDB set that takes the rows only an import
// writes, which no certificate stages.
type importRowsWriter interface {
	ReplaceImportedBlockCounts(
		epoch uint64,
		pools map[string]uint64,
		total, capturedSlot uint64,
	) error
	DeleteNetworkDonationsForEpoch(epoch uint64) error
	ReplaceRewardBlocks(epochs map[uint64]RewardEpochBlocks) error
}

func (s *ImportLedgerDB) rowsWriter() (importRowsWriter, error) {
	w, ok := s.ls.config.UtxoSet.(importRowsWriter)
	if !ok {
		return nil, errors.New(
			"the LedgerDB set cannot take a ledger-state import's rows",
		)
	}
	return w, nil
}

// ReplaceImportedBlockCounts replaces an epoch's per-pool block counts with
// the snapshot's and records their total. It writes the set at once, not
// with txn.
func (s *ImportLedgerDB) ReplaceImportedBlockCounts(
	epoch uint64,
	pools map[string]uint64,
	total, capturedSlot uint64,
	_ ledgerdb.Txn,
) error {
	w, err := s.rowsWriter()
	if err != nil {
		return err
	}
	return w.ReplaceImportedBlockCounts(epoch, pools, total, capturedSlot)
}

// DeleteNetworkDonationsForEpoch removes the donations the set records for
// epoch. It writes the set at once, before any donation txn stages.
func (s *ImportLedgerDB) DeleteNetworkDonationsForEpoch(
	epoch uint64,
	_ ledgerdb.Txn,
) error {
	w, err := s.rowsWriter()
	if err != nil {
		return err
	}
	return w.DeleteNetworkDonationsForEpoch(epoch)
}

// ImportCertificates applies the certificates of tx as the block at point
// would.
func (s *ImportLedgerDB) ImportCertificates(
	tx lcommon.Transaction,
	point ocommon.Point,
	txn ledgerdb.Txn,
) error {
	return s.ls.stageAccountTransaction(
		txn, tx, point.Slot, 0, point.Hash,
		database.BatchedTxIngestOpts{}, map[int]uint64{},
	)
}

// DeactivateAccounts marks the accounts inactive as of slot, with no pool or
// DRep delegation, and closes their DRep delegator links. An account the set
// does not hold active is left alone.
func (s *ImportLedgerDB) DeactivateAccounts(
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	ar := s.ls.accountReader(txn)
	dr := s.ls.drepReader(txn)
	if ar == nil || dr == nil {
		return errors.New(
			"deactivate accounts: the LedgerDB set holds no accounts or DReps",
		)
	}
	for _, ref := range refs {
		held, err := s.ls.accountView(ar, txn, ref.Tag, ref.Key)
		if err != nil {
			return fmt.Errorf("deactivate accounts: %w", err)
		}
		if held == nil || !held.Active {
			continue
		}
		if len(held.Drep) > 0 && held.DrepType <= models.DrepTypeScriptHash {
			drep := models.NewStakeCredentialRef(
				uint8(held.DrepType), held.Drep, //nolint:gosec // a DRep credential type
			)
			live, err := s.ls.drepLinkView(dr, txn, drep, ref)
			if err != nil {
				return fmt.Errorf("deactivate accounts: %w", err)
			}
			if live {
				s.ls.stageDrepLink(txn, slot, drep, ref, false)
			}
		}
		next := *held
		next.Active = false
		next.Pool, next.Drep, next.DrepType = nil, nil, 0
		next.AddedSlot = slot
		s.ls.stageLedgerChanges(txn, LedgerChanges{
			Accounts: []AccountChange{{Slot: slot, Account: next}},
			ImportBaselines: []ImportBaselineChange{{
				Slot:     slot,
				Baseline: ImportBaseline{Ref: ref, AddedSlot: slot},
			}},
		})
	}
	return nil
}

// DeactivateDreps marks the DReps inactive as of slot and closes their
// delegator links; the accounts keep naming them. A DRep the set does not
// hold active is left alone.
func (s *ImportLedgerDB) DeactivateDreps(
	refs []models.StakeCredentialRef,
	slot uint64,
	txn ledgerdb.Txn,
) error {
	r := s.ls.drepReader(txn)
	if r == nil {
		return errors.New("deactivate DReps: the LedgerDB set holds no DReps")
	}
	for _, ref := range refs {
		held, err := s.ls.drepView(r, txn, ref.Tag, ref.Key)
		if err != nil {
			return fmt.Errorf("deactivate DReps: %w", err)
		}
		if held == nil || !held.Active {
			continue
		}
		drep := ref
		links, err := s.ls.drepLinkCandidates(r, txn, &drep)
		if err != nil {
			return fmt.Errorf("deactivate DReps: %w", err)
		}
		for _, l := range links {
			live, err := s.ls.drepLinkView(r, txn, l[0], l[1])
			if err != nil {
				return fmt.Errorf("deactivate DReps: %w", err)
			}
			if live {
				s.ls.stageDrepLink(txn, slot, l[0], l[1], false)
			}
		}
		next := *held
		next.Active = false
		s.ls.stageDrep(txn, slot, next)
	}
	return nil
}

// SetBlockNonce writes the nonce of the block at (slot, hash), which for an
// import is the snapshot tip's evolving nonce.
func (s *ImportLedgerDB) SetBlockNonce(
	blockHash []byte,
	slot uint64,
	nonce []byte,
	isCheckpoint bool,
	txn ledgerdb.Txn,
) error {
	s.ls.stageBlockNonces(txn, []models.BlockNonce{{
		Hash:         bytes.Clone(blockHash),
		Slot:         slot,
		Nonce:        bytes.Clone(nonce),
		IsCheckpoint: isCheckpoint,
	}}, 0)
	return nil
}

// ImportRewardBlocks writes the block counts and fees of the newest epochs as
// reward calculation reads them: the blocks the database holds above
// anchorSlot, the snapshot's tip, merged with the counts the snapshot
// carried. It runs once the import's epochs, ADA pots and block counts are
// written.
func (s *ImportLedgerDB) ImportRewardBlocks(
	ctx context.Context,
	anchorSlot uint64,
) error {
	w, err := s.rowsWriter()
	if err != nil {
		return err
	}
	r, ok := s.ls.config.UtxoSet.(ImportedBlocksReader)
	if !ok {
		return errors.New("the LedgerDB set holds no imported block counts")
	}
	imported, err := r.ImportedBlocks()
	if err != nil {
		return fmt.Errorf("import reward blocks: %w", err)
	}
	rows, err := RewardBlocksFromStore(ctx, importRewardBlocksDB{
		ImportLedgerDB: s, anchor: anchorSlot, imported: imported,
	})
	if err != nil {
		return fmt.Errorf("import reward blocks: %w", err)
	}
	return w.ReplaceRewardBlocks(rows)
}

// importRewardBlocksDB serves the imported block counts from the set, with the
// snapshot's tip as the boundary below which the snapshot's counts stand in
// for the blocks the database never applied.
type importRewardBlocksDB struct {
	*ImportLedgerDB
	anchor   uint64
	imported map[uint64]ImportedBlockEpoch
}

// GetPoolBlockIssuersInSlotRange returns no block: an import applies none,
// and the snapshot's counts stand for every block at or below its tip.
func (d importRewardBlocksDB) GetPoolBlockIssuersInSlotRange(
	_, _ uint64,
	_ ledgerdb.Txn,
) ([]models.PoolOpCertSequence, error) {
	return nil, nil
}

func (d importRewardBlocksDB) GetImportedPoolBlockCounts(
	epoch uint64,
	_ ledgerdb.Txn,
) (map[string]uint64, uint64, bool, error) {
	e, ok := d.imported[epoch]
	if !ok {
		return nil, 0, false, nil
	}
	return maps.Clone(e.Pools), e.Total, true, nil
}

// MergeImportedPoolBlockCounts adds the snapshot's counts for epoch to the
// ones observed above the snapshot's tip. Only the pools in counts take the
// imported counts; the total takes every imported pool's.
func (d importRewardBlocksDB) MergeImportedPoolBlockCounts(
	epoch uint64,
	epochStartSlot uint64,
	counts map[string]uint64,
	totalBlocks uint64,
	_ ledgerdb.Txn,
) (map[string]uint64, uint64, bool, error) {
	if d.anchor < epochStartSlot {
		return counts, totalBlocks, true, nil
	}
	e, ok := d.imported[epoch]
	if !ok {
		return nil, 0, false, nil
	}
	for pool, n := range e.Pools {
		observed, held := counts[pool]
		if !held {
			continue
		}
		if observed > math.MaxUint64-n {
			return nil, 0, false, fmt.Errorf(
				"imported block count overflow for epoch %d pool %x",
				epoch, pool,
			)
		}
		counts[pool] = observed + n
	}
	if totalBlocks > math.MaxUint64-e.Total {
		return nil, 0, false, fmt.Errorf(
			"imported block total overflow for epoch %d", epoch,
		)
	}
	return counts, totalBlocks + e.Total, true, nil
}

// sameRegistration reports whether a and b register the same parameters,
// whatever slot each was written at.
func sameRegistration(a, b *models.PoolRegistration) bool {
	if a == nil || b == nil {
		return false
	}
	x, y := CleanPoolRegistration(a), CleanPoolRegistration(b)
	x.AddedSlot, y.AddedSlot = 0, 0
	return reflect.DeepEqual(x, y)
}
