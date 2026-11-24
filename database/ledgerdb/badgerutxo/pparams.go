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
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/dgraph-io/badger/v4"
)

// Protocol parameter keys:
//
//	p <era:8 BE> <epoch:8 BE> <seq:8 BE>   added slot, then the row's CBOR
//	q <added slot:8 BE> <seq:8 BE>         era and epoch of row seq
//
// The metadata store keeps every write as a new row and reads the newest by
// row ID, so seq numbers the writes in order in place of the row ID.
// PParamsRow is one protocol parameter row. Seq orders the rows as the
// metadata store's row IDs do; it is assigned by the store.
type PParamsRow struct {
	Era       uint64
	Epoch     uint64
	AddedSlot uint64
	Seq       uint64
	Cbor      []byte
}

func pparamsKeyOf(era, epoch, seq uint64) []byte {
	k := binary.BigEndian.AppendUint64([]byte{PParamsTag}, era)
	k = binary.BigEndian.AppendUint64(k, epoch)
	return binary.BigEndian.AppendUint64(k, seq)
}

func decodePParams(k, v []byte) (PParamsRow, error) {
	if len(k) != 25 || len(v) < 8 {
		return PParamsRow{}, errors.New("badgerutxo: malformed pparams entry")
	}
	return PParamsRow{
		Era:       binary.BigEndian.Uint64(k[1:]),
		Epoch:     binary.BigEndian.Uint64(k[9:]),
		Seq:       binary.BigEndian.Uint64(k[17:]),
		AddedSlot: binary.BigEndian.Uint64(v),
		Cbor:      v[8:],
	}, nil
}

func readPParamsSeq(txn *badger.Txn) (uint64, error) {
	item, err := txn.Get(pparamsSeqKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("badgerutxo: malformed pparams sequence")
	}
	return binary.BigEndian.Uint64(v), nil
}

// setPParams writes r under the given sequence number, with its slot index.
func setPParams(
	set func(k, v []byte) error,
	r PParamsRow,
	seq uint64,
) error {
	v := binary.BigEndian.AppendUint64(nil, r.AddedSlot)
	if err := set(pparamsKeyOf(r.Era, r.Epoch, seq), append(v, r.Cbor...)); err != nil {
		return err
	}
	idx := binary.BigEndian.AppendUint64([]byte{PParamsSlotTag}, r.AddedSlot)
	idx = binary.BigEndian.AppendUint64(idx, seq)
	return set(idx, binary.BigEndian.AppendUint64(
		binary.BigEndian.AppendUint64(nil, r.Era), r.Epoch,
	))
}

// putPParams appends rows after every row the store holds.
func putPParams(txn *badger.Txn, rows []PParamsRow) error {
	if len(rows) == 0 {
		return nil
	}
	seq, err := readPParamsSeq(txn)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := setPParams(txn.Set, r, seq); err != nil {
			return err
		}
		seq++
	}
	return txn.Set(pparamsSeqKey, binary.BigEndian.AppendUint64(nil, seq))
}

// deletePParamsAfter deletes the rows added after slot.
func deletePParamsAfter(txn *badger.Txn, slot uint64) error {
	var keys [][]byte
	err := func() error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{PParamsSlotTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(slotKey(PParamsSlotTag, slot+1)); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(k) != 17 || len(v) != 16 {
				return errors.New("badgerutxo: malformed pparams index entry")
			}
			keys = append(keys, k, pparamsKeyOf(
				binary.BigEndian.Uint64(v),
				binary.BigEndian.Uint64(v[8:]),
				binary.BigEndian.Uint64(k[9:]),
			))
		}
		return nil
	}()
	if err != nil {
		return err
	}
	return deleteKeys(txn, keys)
}

// PParamsAt returns era's newest row of the latest epoch at or before
// epoch, as the metadata store's GetPParams picks it.
func (s *Store) PParamsAt(era, epoch uint64) (PParamsRow, bool, error) {
	var row PParamsRow
	var found bool
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = binary.BigEndian.AppendUint64([]byte{PParamsTag}, era)
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(pparamsKeyOf(era, epoch, math.MaxUint64))
		if !it.Valid() {
			return nil
		}
		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		row, err = decodePParams(it.Item().KeyCopy(nil), v)
		found = err == nil
		return err
	})
	return row, found, err
}

// PParamsForEra returns era's rows in the order they were written.
func (s *Store) PParamsForEra(era uint64) ([]PParamsRow, error) {
	var out []PParamsRow
	err := s.view(func(txn *badger.Txn) error {
		return eachPrefix(txn,
			binary.BigEndian.AppendUint64([]byte{PParamsTag}, era),
			func(k, v []byte) error {
				r, err := decodePParams(k, v)
				if err != nil {
					return err
				}
				out = append(out, r)
				return nil
			},
		)
	})
	slices.SortFunc(out, func(a, b PParamsRow) int { return cmp.Compare(a.Seq, b.Seq) })
	return out, err
}

// LoadPParams replaces the store's protocol parameter rows with rows, in
// the order the metadata store wrote them, and marks them loaded, for a
// store from before it kept them.
func (s *Store) LoadPParams(rows []PParamsRow) error {
	if err := s.dropPrefix(
		[]byte{PParamsTag}, []byte{PParamsSlotTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop pparams: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	for i, r := range rows {
		if err := setPParams(wb.Set, r, uint64(i)); err != nil {
			return fmt.Errorf("badgerutxo: load pparams: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load pparams: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		err := txn.Set(
			pparamsSeqKey,
			binary.BigEndian.AppendUint64(nil, uint64(len(rows))),
		)
		if err != nil {
			return err
		}
		return txn.Set(pparamsKey, []byte{1})
	})
}

// PParamsLoaded reports whether the store holds every protocol parameter
// row up to its tip.
func (s *Store) PParamsLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(pparamsKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}
