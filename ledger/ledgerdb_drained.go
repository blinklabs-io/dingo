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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrDrainedFamilyUnserved reports a read of a drained family that the live
// UTxO set cannot answer. The metadata store holds no rows for the family,
// so the read fails rather than return an empty answer.
var ErrDrainedFamilyUnserved = errors.New(
	"ledger-state family is drained and the live UTxO set cannot serve it",
)

func drainedUnserved(f nodesettings.Family) error {
	return fmt.Errorf("%w: %s", ErrDrainedFamilyUnserved, f)
}

// drainedAccountsRefusal is drainedUnserved for an account read that falls
// back to the metadata store, which refuses a drained accounts family with
// database.ErrFamilyDrained; the error is both, so a caller matching either
// sees it.
func drainedAccountsRefusal() error {
	return fmt.Errorf(
		"%w: %w", database.ErrFamilyDrained,
		drainedUnserved(nodesettings.FamilyAccounts),
	)
}

func (s stagingLedgerDB) noncesDrained() bool {
	return s.ls.familyDrained(nodesettings.FamilyNonces)
}

// GetBlockNonce reads from the set when the nonces family is drained.
func (s stagingLedgerDB) GetBlockNonce(
	point ocommon.Point,
	txn ledgerdb.Txn,
) ([]byte, error) {
	if s.noncesDrained() {
		return s.ls.blockNonce(point, txn)
	}
	return s.LedgerDB.GetBlockNonce(point, txn)
}

// GetBlockNoncesInSlotRange reads from the set when the nonces family is
// drained.
func (s stagingLedgerDB) GetBlockNoncesInSlotRange(
	start, end uint64,
	txn ledgerdb.Txn,
) ([]models.BlockNonce, error) {
	if s.noncesDrained() {
		return s.ls.blockNoncesInSlotRange(start, end, txn)
	}
	return s.LedgerDB.GetBlockNoncesInSlotRange(start, end, txn)
}

// GetLatestBlockNonce reads from the set when the nonces family is drained.
func (s stagingLedgerDB) GetLatestBlockNonce(
	txn ledgerdb.Txn,
) (models.BlockNonce, bool, error) {
	if s.noncesDrained() {
		return s.ls.latestBlockNonce(txn)
	}
	return s.LedgerDB.GetLatestBlockNonce(txn)
}

// SetBlockNonce skips the SQL write of a drained nonces family; callers stage
// the same nonce for the set.
func (s stagingLedgerDB) SetBlockNonce(
	blockHash []byte,
	slotNumber uint64,
	nonce []byte,
	isCheckpoint bool,
	txn ledgerdb.Txn,
) error {
	if s.noncesDrained() {
		return nil
	}
	return s.LedgerDB.SetBlockNonce(
		blockHash, slotNumber, nonce, isCheckpoint, txn,
	)
}

// SetBlockNonces skips the SQL write of a drained nonces family.
func (s stagingLedgerDB) SetBlockNonces(
	nonces []models.BlockNonce,
	txn ledgerdb.Txn,
) error {
	if s.noncesDrained() {
		return nil
	}
	return s.LedgerDB.SetBlockNonces(nonces, txn)
}

// DeleteBlockNoncesBeforeSlotWithoutCheckpoints skips the SQL delete of a
// drained nonces family; the staged prune reaches the set.
func (s stagingLedgerDB) DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
	slot uint64,
	txn ledgerdb.Txn,
) error {
	if s.noncesDrained() {
		return nil
	}
	return s.LedgerDB.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(slot, txn)
}

// DeleteBlockNoncesAfterPoint, for a drained nonces family, only checks that
// the set holds no nonce past point: startup reconciles the set to the
// ledger tip, so a nonce beyond it means the set was not rewound.
func (s stagingLedgerDB) DeleteBlockNoncesAfterPoint(
	point ocommon.Point,
	txn ledgerdb.Txn,
) error {
	if !s.noncesDrained() {
		return s.LedgerDB.DeleteBlockNoncesAfterPoint(point, txn)
	}
	latest, ok, err := s.ls.latestBlockNonce(txn)
	if err != nil {
		return err
	}
	if ok && latest.Slot > point.Slot {
		return fmt.Errorf(
			"live UTxO set holds a block nonce at slot %d past the ledger tip at slot %d",
			latest.Slot,
			point.Slot,
		)
	}
	return nil
}

// drainedNonceSource serves a rollback's nonce reads from the set once the
// nonces family is drained.
type drainedNonceSource struct {
	ls *LedgerState
}

func (d drainedNonceSource) BlockNonceAt(point ocommon.Point) ([]byte, error) {
	return d.ls.blockNonce(point, nil)
}

func (d drainedNonceSource) HasCheckpointAtOrBefore(slot uint64) (bool, error) {
	rows, err := d.ls.blockNoncesInSlotRange(0, slot+1, nil)
	if err != nil {
		return false, err
	}
	for _, row := range rows {
		if row.IsCheckpoint && len(row.Nonce) == lcommon.Blake2b256Size {
			return true, nil
		}
	}
	return false, nil
}

// installNonceSource gives the database the set's nonces for the rollbacks it
// runs once the nonces family is drained.
func (ls *LedgerState) installNonceSource() {
	if ls.db != nil && ls.keepsEpochState() {
		ls.db.SetNonceSource(drainedNonceSource{ls})
	}
}
