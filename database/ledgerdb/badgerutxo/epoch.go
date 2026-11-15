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

package badgerutxo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Epoch state keys:
//
//	e <epoch:8 BE>                         epoch row: start slot, era, then
//	                                       the caller's encoded row
//	s <epoch:8 BE> <len:1> <type> <pool>   pool stake snapshot row
//	mera                                   the last mid-epoch era switch
//
// EpochRow is an epoch's row: its number, first slot, era and encoded
// contents. The store rewrites Era alone when it undoes a mid-epoch era
// switch, so a caller decoding Value takes the era from Era.
type EpochRow struct {
	Epoch     uint64
	StartSlot uint64
	Era       uint64
	Value     []byte
}

// EraSwitch records an era translation applied before the block at Slot,
// inside Epoch. A rollback to a slot before Slot gives the epoch's row
// FromEra back when it still carries ToEra, as the metadata store does: the
// epoch row survives a rollback that stays inside its epoch.
type EraSwitch struct {
	Epoch   uint64
	Slot    uint64
	FromEra uint64
	ToEra   uint64
}

func (e EraSwitch) encode() []byte {
	b := binary.BigEndian.AppendUint64(nil, e.Epoch)
	b = binary.BigEndian.AppendUint64(b, e.Slot)
	b = binary.BigEndian.AppendUint64(b, e.FromEra)
	return binary.BigEndian.AppendUint64(b, e.ToEra)
}

func decodeEraSwitch(v []byte) (EraSwitch, error) {
	if len(v) != 32 {
		return EraSwitch{}, errors.New("badgerutxo: malformed era switch")
	}
	return EraSwitch{
		Epoch:   binary.BigEndian.Uint64(v),
		Slot:    binary.BigEndian.Uint64(v[8:]),
		FromEra: binary.BigEndian.Uint64(v[16:]),
		ToEra:   binary.BigEndian.Uint64(v[24:]),
	}, nil
}

// SnapshotRow is one pool's stake snapshot row for an epoch and type.
type SnapshotRow struct {
	Epoch uint64
	Type  string
	Pool  []byte
	Value []byte
}

// SnapshotOpKind is what a SnapshotOp does.
type SnapshotOpKind uint8

const (
	// SnapshotPut stores Row.
	SnapshotPut SnapshotOpKind = iota
	// SnapshotDeleteEpochType deletes the rows of Row.Epoch and Row.Type.
	SnapshotDeleteEpochType
	// SnapshotDeleteBefore deletes the rows of epochs before Row.Epoch.
	SnapshotDeleteBefore
)

// SnapshotOp is one change to the pool stake snapshots.
type SnapshotOp struct {
	Kind SnapshotOpKind
	Row  SnapshotRow
}

func epochKey(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{EpochTag}, epoch)
}

func snapshotEpochPrefix(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{SnapshotTag}, epoch)
}

func snapshotTypePrefix(epoch uint64, typ string) []byte {
	k := snapshotEpochPrefix(epoch)
	k = append(k, byte(len(typ))) //nolint:gosec // snapshot types are a few bytes
	return append(k, typ...)
}

func snapshotKey(r SnapshotRow) []byte {
	return append(snapshotTypePrefix(r.Epoch, r.Type), r.Pool...)
}

func decodeSnapshotKey(k, v []byte) (SnapshotRow, error) {
	if len(k) < 10 || len(k) < 10+int(k[9]) {
		return SnapshotRow{}, errors.New("badgerutxo: malformed snapshot key")
	}
	tl := int(k[9])
	return SnapshotRow{
		Epoch: binary.BigEndian.Uint64(k[1:9]),
		Type:  string(k[10 : 10+tl]),
		Pool:  bytes.Clone(k[10+tl:]),
		Value: v,
	}, nil
}

func encodeEpoch(r EpochRow) []byte {
	v := binary.BigEndian.AppendUint64(nil, r.StartSlot)
	v = binary.BigEndian.AppendUint64(v, r.Era)
	return append(v, r.Value...)
}

func putEpoch(txn *badger.Txn, r EpochRow) error {
	return txn.Set(epochKey(r.Epoch), encodeEpoch(r))
}

func decodeEpoch(k, v []byte) (EpochRow, error) {
	if len(k) != 9 || len(v) < 16 {
		return EpochRow{}, errors.New("badgerutxo: malformed epoch entry")
	}
	return EpochRow{
		Epoch:     binary.BigEndian.Uint64(k[1:]),
		StartSlot: binary.BigEndian.Uint64(v),
		Era:       binary.BigEndian.Uint64(v[8:]),
		Value:     bytes.Clone(v[16:]),
	}, nil
}

// revertEraSwitchAfter undoes the recorded era switch when its block lies
// after slot: the switched epoch's row, if the rollback left it and it still
// carries the target era, returns to the source era, and the record goes.
func revertEraSwitchAfter(txn *badger.Txn, slot uint64) error {
	item, err := txn.Get(eraSwitchKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return err
	}
	sw, err := decodeEraSwitch(v)
	if err != nil {
		return err
	}
	if sw.Slot <= slot {
		return nil
	}
	k := epochKey(sw.Epoch)
	item, err = txn.Get(k)
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
	case err != nil:
		return err
	default:
		ev, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		row, err := decodeEpoch(k, ev)
		if err != nil {
			return err
		}
		if row.Era == sw.ToEra {
			row.Era = sw.FromEra
			if err := putEpoch(txn, row); err != nil {
				return err
			}
		}
	}
	return txn.Delete(eraSwitchKey)
}

// eachPrefix calls fn with every key and value under prefix, in key order.
func eachPrefix(
	txn *badger.Txn,
	prefix []byte,
	fn func(k, v []byte) error,
) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		if err := fn(it.Item().KeyCopy(nil), v); err != nil {
			return err
		}
	}
	return nil
}

func deleteKeys(txn *badger.Txn, keys [][]byte) error {
	for _, k := range keys {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func applySnapshotOp(txn *badger.Txn, op SnapshotOp) error {
	switch op.Kind {
	case SnapshotPut:
		return txn.Set(snapshotKey(op.Row), op.Row.Value)
	case SnapshotDeleteEpochType:
		var keys [][]byte
		err := eachPrefix(txn, snapshotTypePrefix(op.Row.Epoch, op.Row.Type),
			func(k, _ []byte) error { keys = append(keys, k); return nil })
		if err != nil {
			return err
		}
		return deleteKeys(txn, keys)
	case SnapshotDeleteBefore:
		var keys [][]byte
		err := eachPrefix(txn, []byte{SnapshotTag}, func(k, _ []byte) error {
			if binary.BigEndian.Uint64(k[1:9]) < op.Row.Epoch {
				keys = append(keys, k)
			}
			return nil
		})
		if err != nil {
			return err
		}
		return deleteKeys(txn, keys)
	default:
		return fmt.Errorf("badgerutxo: unknown snapshot op %d", op.Kind)
	}
}

// rollbackLedgerState removes what a rollback to slot undoes: the nonces,
// network states, donations and protocol parameters after slot, the account
// changes after slot, the epochs
// that start after slot, an era switch made after slot, and the snapshots of epochs after the
// newest epoch left, as the metadata store's truncation does.
func rollbackLedgerState(txn *badger.Txn, slot uint64) error {
	if err := bumpRollbackSeq(txn); err != nil {
		return err
	}
	if err := deleteNoncesAfter(txn, slot); err != nil {
		return err
	}
	for _, tag := range []byte{NetworkStateTag, DonationTag, RewardRecordTag} {
		if err := deleteSlotKeysAfter(txn, tag, slot); err != nil {
			return err
		}
	}
	if err := deletePParamsAfter(txn, slot); err != nil {
		return err
	}
	if err := rollbackRows(txn, slot); err != nil {
		return err
	}
	var gone [][]byte
	var newest uint64
	found := false
	err := eachPrefix(txn, []byte{EpochTag}, func(k, v []byte) error {
		e, err := decodeEpoch(k, v)
		if err != nil {
			return err
		}
		if e.StartSlot > slot {
			gone = append(gone, k)
			return nil
		}
		if !found || e.Epoch > newest {
			newest, found = e.Epoch, true
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := deleteKeys(txn, gone); err != nil {
		return err
	}
	if err := revertEraSwitchAfter(txn, slot); err != nil {
		return err
	}
	if err := revertSyntheticV2After(txn, newest, found); err != nil {
		return err
	}
	if !found {
		// The metadata store keeps the snapshots and summaries when no
		// epoch row is left.
		return nil
	}
	if err := deleteSummariesAfter(txn, newest); err != nil {
		return err
	}
	var snaps [][]byte
	err = eachPrefix(txn, []byte{SnapshotTag}, func(k, _ []byte) error {
		if binary.BigEndian.Uint64(k[1:9]) > newest {
			snaps = append(snaps, k)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return deleteKeys(txn, snaps)
}

// Epochs returns every epoch row, in epoch order.
func (s *Store) Epochs() ([]EpochRow, error) {
	var out []EpochRow
	err := s.view(func(txn *badger.Txn) error {
		return eachPrefix(txn, []byte{EpochTag}, func(k, v []byte) error {
			e, err := decodeEpoch(k, v)
			if err != nil {
				return err
			}
			out = append(out, e)
			return nil
		})
	})
	return out, err
}

// Snapshots returns the snapshot rows of epoch, of typ when it is not
// empty, in key order.
func (s *Store) Snapshots(epoch uint64, typ string) ([]SnapshotRow, error) {
	prefix := snapshotEpochPrefix(epoch)
	if typ != "" {
		prefix = snapshotTypePrefix(epoch, typ)
	}
	var out []SnapshotRow
	err := s.view(func(txn *badger.Txn) error {
		return eachPrefix(txn, prefix, func(k, v []byte) error {
			r, err := decodeSnapshotKey(k, v)
			if err != nil {
				return err
			}
			out = append(out, r)
			return nil
		})
	})
	return out, err
}

// LoadEpochState replaces the store's epoch and snapshot rows and its era
// switch record with the given ones and marks them loaded, for a store from
// before it kept them.
func (s *Store) LoadEpochState(
	epochs []EpochRow,
	snapshots []SnapshotRow,
	eraSwitch *EraSwitch,
) error {
	if err := s.dropPrefix(
		[]byte{EpochTag}, []byte{SnapshotTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop epoch state: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	for _, e := range epochs {
		if err := wb.Set(epochKey(e.Epoch), encodeEpoch(e)); err != nil {
			return fmt.Errorf("badgerutxo: load epochs: %w", err)
		}
	}
	for _, r := range snapshots {
		if err := wb.Set(snapshotKey(r), r.Value); err != nil {
			return fmt.Errorf("badgerutxo: load snapshots: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load epoch state: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		if eraSwitch != nil {
			if err := txn.Set(eraSwitchKey, eraSwitch.encode()); err != nil {
				return err
			}
		} else if !s.merging.Load() {
			// A merge keeps the last switch the set recorded.
			if err := txn.Delete(eraSwitchKey); err != nil {
				return err
			}
		}
		if err := txn.Delete(legacyEpochStateKey); err != nil {
			return err
		}
		return txn.Set(epochStateKey, []byte{1})
	})
}

// EraSwitch returns the recorded mid-epoch era switch, or nil.
func (s *Store) EraSwitch() (*EraSwitch, error) {
	var out *EraSwitch
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(eraSwitchKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		v, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		sw, err := decodeEraSwitch(v)
		if err != nil {
			return err
		}
		out = &sw
		return nil
	})
	return out, err
}

// EpochStateLoaded reports whether the store holds every epoch and snapshot
// row up to its tip.
func (s *Store) EpochStateLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(epochStateKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// bumpRollbackSeq increments the rollback counter. Every rollback reaches
// rollbackLedgerState at least once, so the counter changes whenever one
// has run; a rollback that undoes several batches changes it several times.
func bumpRollbackSeq(txn *badger.Txn) error {
	seq, err := readRollbackSeq(txn)
	if err != nil {
		return err
	}
	return txn.Set(rollbackSeqKey, binary.BigEndian.AppendUint64(nil, seq+1))
}

func readRollbackSeq(txn *badger.Txn) (uint64, error) {
	item, err := txn.Get(rollbackSeqKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var seq uint64
	err = item.Value(func(v []byte) error {
		if len(v) != 8 {
			return errors.New("badgerutxo: malformed rollback counter")
		}
		seq = binary.BigEndian.Uint64(v)
		return nil
	})
	return seq, err
}

// RollbackSeq returns the rollback counter: it differs from a value read
// earlier exactly when a rollback has run in between.
func (s *Store) RollbackSeq() (uint64, error) {
	var seq uint64
	err := s.view(func(txn *badger.Txn) error {
		var err error
		seq, err = readRollbackSeq(txn)
		return err
	})
	return seq, err
}
