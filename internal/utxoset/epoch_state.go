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

package utxoset

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
)

// snapshotTypes are the pool stake snapshot types the metadata store keeps.
var snapshotTypes = []string{"mark", "set", "go", "actv"}

func decodeInto(b []byte, v any) error {
	_, err := cbor.Decode(b, v)
	return err
}

// The row IDs belong to the metadata store, so the copies leave them out.
func encodeEpoch(e models.Epoch) (badgerutxo.EpochRow, error) {
	e.ID = 0
	v, err := cbor.Encode(e)
	if err != nil {
		return badgerutxo.EpochRow{}, fmt.Errorf("encode epoch %d: %w", e.EpochId, err)
	}
	return badgerutxo.EpochRow{
		Epoch: e.EpochId, StartSlot: e.StartSlot, Era: uint64(e.EraId), Value: v,
	}, nil
}

func decodeEpoch(r badgerutxo.EpochRow) (models.Epoch, error) {
	var e models.Epoch
	if err := decodeInto(r.Value, &e); err != nil {
		return e, fmt.Errorf("decode epoch %d: %w", r.Epoch, err)
	}
	// The store rewrites the era alone when it undoes a mid-epoch switch.
	e.EraId = uint(r.Era)
	return e, nil
}

func encodeSnapshot(s *models.PoolStakeSnapshot) (badgerutxo.SnapshotRow, error) {
	c := *s
	c.ID = 0
	v, err := cbor.Encode(c)
	if err != nil {
		return badgerutxo.SnapshotRow{}, fmt.Errorf(
			"encode snapshot %d/%s: %w", s.Epoch, s.SnapshotType, err,
		)
	}
	return badgerutxo.SnapshotRow{
		Epoch: s.Epoch, Type: s.SnapshotType, Pool: s.PoolKeyHash, Value: v,
	}, nil
}

func decodeSnapshot(r badgerutxo.SnapshotRow) (*models.PoolStakeSnapshot, error) {
	var s models.PoolStakeSnapshot
	if err := decodeInto(r.Value, &s); err != nil {
		return nil, fmt.Errorf("decode snapshot %d/%s: %w", r.Epoch, r.Type, err)
	}
	return &s, nil
}

func eraSwitchRecord(sw *database.MidEpochEraSwitch) *badgerutxo.EraSwitch {
	if sw == nil {
		return nil
	}
	return &badgerutxo.EraSwitch{
		Epoch:   sw.Epoch,
		Slot:    sw.Slot,
		FromEra: uint64(sw.FromEra),
		ToEra:   uint64(sw.ToEra),
	}
}

// epochStateBatch converts a commit's epoch and snapshot changes for the
// store.
func epochStateBatch(lc ledger.LedgerChanges) (
	[]badgerutxo.EpochRow,
	[]badgerutxo.SnapshotOp,
	error,
) {
	epochs := make([]badgerutxo.EpochRow, 0, len(lc.Epochs))
	for _, e := range lc.Epochs {
		row, err := encodeEpoch(e)
		if err != nil {
			return nil, nil, err
		}
		epochs = append(epochs, row)
	}
	ops := make([]badgerutxo.SnapshotOp, 0, len(lc.Snapshots))
	for _, c := range lc.Snapshots {
		switch c.Kind {
		case ledger.SnapshotSave:
			row, err := encodeSnapshot(c.Snapshot)
			if err != nil {
				return nil, nil, err
			}
			ops = append(ops, badgerutxo.SnapshotOp{
				Kind: badgerutxo.SnapshotPut, Row: row,
			})
		case ledger.SnapshotDeleteForEpoch:
			ops = append(ops, badgerutxo.SnapshotOp{
				Kind: badgerutxo.SnapshotDeleteEpochType,
				Row:  badgerutxo.SnapshotRow{Epoch: c.Epoch, Type: c.Type},
			})
		case ledger.SnapshotDeleteBeforeEpoch:
			ops = append(ops, badgerutxo.SnapshotOp{
				Kind: badgerutxo.SnapshotDeleteBefore,
				Row:  badgerutxo.SnapshotRow{Epoch: c.Epoch},
			})
		default:
			return nil, nil, fmt.Errorf("unknown snapshot change %d", c.Kind)
		}
	}
	return epochs, ops, nil
}

func (s *Set) ensureEpochState(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.EpochStateLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the epoch state marker: %w", err)
	}
	if loaded {
		return nil
	}
	txn := ldb.Begin(ctx, false)
	defer txn.Release()
	epochs, err := ldb.GetEpochs(txn)
	if err != nil {
		return fmt.Errorf("utxoset: read epochs: %w", err)
	}
	rows := make([]badgerutxo.EpochRow, 0, len(epochs))
	var lo, hi uint64
	for i, e := range epochs {
		row, err := encodeEpoch(e)
		if err != nil {
			return err
		}
		rows = append(rows, row)
		if i == 0 || e.EpochId < lo {
			lo = e.EpochId
		}
		hi = max(hi, e.EpochId)
	}
	var snaps []badgerutxo.SnapshotRow
	if len(epochs) > 0 {
		// Snapshots are captured for the epoch after the boundary that
		// writes them, so they can run a little past the newest epoch row.
		for epoch := lo; epoch <= hi+3; epoch++ {
			for _, typ := range snapshotTypes {
				got, err := ldb.GetPoolStakeSnapshotsByEpoch(epoch, typ, txn)
				if err != nil {
					return fmt.Errorf(
						"utxoset: read snapshots %d/%s: %w", epoch, typ, err,
					)
				}
				for _, sn := range got {
					row, err := encodeSnapshot(sn)
					if err != nil {
						return err
					}
					snaps = append(snaps, row)
				}
			}
		}
	}
	var eraSwitch *badgerutxo.EraSwitch
	value, err := ldb.GetSyncState(database.MidEpochEraSwitchSyncKey, txn)
	if err != nil {
		return fmt.Errorf("utxoset: read the mid-epoch era switch: %w", err)
	}
	if value != "" {
		sw, err := database.ParseMidEpochEraSwitch(value)
		if err != nil {
			return fmt.Errorf("utxoset: %w", err)
		}
		eraSwitch = eraSwitchRecord(&sw)
	}
	if err := s.store.LoadEpochState(rows, snaps, eraSwitch); err != nil {
		return fmt.Errorf("utxoset: load epoch state: %w", err)
	}
	s.logger.Info("loaded epochs and pool stake snapshots into the live UTxO set",
		"epochs", len(rows), "snapshots", len(snaps))
	return nil
}

// EpochStateLoaded reports whether the set holds every epoch and snapshot
// row.
func (s *Set) EpochStateLoaded() (bool, error) {
	return s.store.EpochStateLoaded()
}

// EraSwitch returns the mid-epoch era switch the set records, or nil.
func (s *Set) EraSwitch() (*database.MidEpochEraSwitch, error) {
	sw, err := s.store.EraSwitch()
	if err != nil || sw == nil {
		return nil, err
	}
	return &database.MidEpochEraSwitch{
		Epoch:   sw.Epoch,
		Slot:    sw.Slot,
		FromEra: uint(sw.FromEra),
		ToEra:   uint(sw.ToEra),
	}, nil
}

// Epochs returns every epoch row the set holds, in epoch order.
func (s *Set) Epochs() ([]models.Epoch, error) {
	rows, err := s.store.Epochs()
	if err != nil {
		return nil, err
	}
	return DecodeEpochRows(rows)
}

// DecodeEpochRows decodes epoch rows read from a set store.
func DecodeEpochRows(rows []badgerutxo.EpochRow) ([]models.Epoch, error) {
	out := make([]models.Epoch, 0, len(rows))
	for _, r := range rows {
		e, err := decodeEpoch(r)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// PoolStakeSnapshots returns epoch's snapshot rows of typ, every type when
// typ is empty, in pool order within a type. A positive limit stops the read
// after that many rows.
func (s *Set) PoolStakeSnapshots(
	epoch uint64,
	typ string,
	limit int,
) ([]*models.PoolStakeSnapshot, error) {
	rows, err := s.store.Snapshots(epoch, typ, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*models.PoolStakeSnapshot, 0, len(rows))
	for _, r := range rows {
		sn, err := decodeSnapshot(r)
		if err != nil {
			return nil, err
		}
		out = append(out, sn)
	}
	return out, nil
}
