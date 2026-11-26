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

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

// Nonce is a block's nonce at its point. Checkpoint nonces survive pruning.
type Nonce struct {
	Point      ocommon.Point
	Nonce      []byte
	Checkpoint bool
}

func nonceKey(p ocommon.Point) []byte {
	k := make([]byte, 0, 9+len(p.Hash))
	k = append(k, NonceTag)
	k = binary.BigEndian.AppendUint64(k, p.Slot)
	return append(k, p.Hash...)
}

func nonceSlotPrefix(slot uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{NonceTag}, slot)
}

func encodeNonce(n Nonce) []byte {
	v := make([]byte, 0, 1+len(n.Nonce))
	if n.Checkpoint {
		v = append(v, 1)
	} else {
		v = append(v, 0)
	}
	return append(v, n.Nonce...)
}

func decodeNonce(key, val []byte) (Nonce, error) {
	if len(key) < 9 || len(val) < 1 {
		return Nonce{}, errors.New("badgerutxo: malformed nonce entry")
	}
	return Nonce{
		Point: ocommon.Point{
			Slot: binary.BigEndian.Uint64(key[1:9]),
			Hash: bytes.Clone(key[9:]),
		},
		Checkpoint: val[0] == 1,
		Nonce:      bytes.Clone(val[1:]),
	}, nil
}

// putNonces writes nonces. A nonce rewritten for a block keeps its
// checkpoint flag once set, as the metadata store's upsert does: a block
// applied again, after a restart or a rollback, may not be the first its
// batch writes in the epoch.
func putNonces(txn *badger.Txn, nonces []Nonce) error {
	for _, n := range nonces {
		key := nonceKey(n.Point)
		if !n.Checkpoint {
			item, err := txn.Get(key)
			switch {
			case errors.Is(err, badger.ErrKeyNotFound):
			case err != nil:
				return err
			default:
				err := item.Value(func(v []byte) error {
					n.Checkpoint = len(v) > 0 && v[0] == 1
					return nil
				})
				if err != nil {
					return err
				}
			}
		}
		if err := txn.Set(key, encodeNonce(n)); err != nil {
			return err
		}
	}
	return nil
}

// eachNonceFrom calls fn for every nonce at or after slot, in slot order,
// until fn returns false or an error.
func eachNonceFrom(
	txn *badger.Txn,
	slot uint64,
	fn func(key []byte, n Nonce) (bool, error),
) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{NonceTag}
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(nonceSlotPrefix(slot)); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		n, err := decodeNonce(key, val)
		if err != nil {
			return err
		}
		more, err := fn(key, n)
		if err != nil || !more {
			return err
		}
	}
	return nil
}

// deleteNoncesAfter deletes the nonces of blocks after slot, which a
// rollback to slot leaves unapplied.
func deleteNoncesAfter(txn *badger.Txn, slot uint64) error {
	var keys [][]byte
	if err := eachNonceFrom(txn, slot+1,
		func(key []byte, _ Nonce) (bool, error) {
			keys = append(keys, key)
			return true, nil
		},
	); err != nil {
		return err
	}
	for _, k := range keys {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// eachPrunableNonce calls fn with the key of every non-checkpoint nonce
// before slot, in slot order, until fn returns an error.
func eachPrunableNonce(
	txn *badger.Txn,
	slot uint64,
	fn func(key []byte) error,
) error {
	var ferr error
	if err := eachNonceFrom(txn, 0,
		func(key []byte, n Nonce) (bool, error) {
			if n.Point.Slot >= slot {
				return false, nil
			}
			if !n.Checkpoint {
				ferr = fn(key)
			}
			return ferr == nil, ferr
		},
	); err != nil {
		return err
	}
	return ferr
}

// pruneNonces deletes the non-checkpoint nonces before slot.
func pruneNonces(txn *badger.Txn, slot uint64) error {
	var keys [][]byte
	if err := eachPrunableNonce(txn, slot, func(key []byte) error {
		keys = append(keys, key)
		return nil
	}); err != nil {
		return err
	}
	for _, k := range keys {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// pruneNoncesBulk deletes the non-checkpoint nonces before slot in as many
// transactions as they need. Nothing bounds how many a prune finds: an epoch
// rollover prunes every nonce written since the last prune that committed,
// which a set that never pruned, or a heal that wrote one per block of a
// gap, leaves above what one transaction holds. A crash part way leaves some
// of them, which the same prune, run again, deletes.
func (s *Store) pruneNoncesBulk(slot uint64) error {
	wb := s.newWriteBatch()
	defer wb.Cancel()
	if err := s.view(func(txn *badger.Txn) error {
		return eachPrunableNonce(txn, slot, wb.Delete)
	}); err != nil {
		return err
	}
	return wb.Flush()
}

// Nonce returns the nonce of the block at p. ok is false when the store has
// none.
func (s *Store) Nonce(p ocommon.Point) ([]byte, bool, error) {
	var out []byte
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(nonceKey(p))
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
		if len(v) < 1 {
			return errors.New("badgerutxo: malformed nonce entry")
		}
		ok = true
		out = v[1:]
		return nil
	})
	return out, ok, err
}

// NoncesInSlotRange returns the nonces of blocks in [start, end), in slot
// order.
func (s *Store) NoncesInSlotRange(start, end uint64) ([]Nonce, error) {
	var out []Nonce
	err := s.view(func(txn *badger.Txn) error {
		return eachNonceFrom(txn, start,
			func(_ []byte, n Nonce) (bool, error) {
				if n.Point.Slot >= end {
					return false, nil
				}
				out = append(out, n)
				return true, nil
			},
		)
	})
	return out, err
}

// LatestNonce returns the nonce of the highest-slot block the store holds.
func (s *Store) LatestNonce() (Nonce, bool, error) {
	var out Nonce
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{NonceTag}
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		// A reverse seek starts at the largest key not above the target.
		it.Seek([]byte{NonceTag, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
		if !it.Valid() {
			return nil
		}
		item := it.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		out, err = decodeNonce(item.KeyCopy(nil), val)
		ok = err == nil
		return err
	})
	return out, ok, err
}

// LoadNonces stores the nonces of every block up to the store's tip and
// marks them loaded, for a store bootstrapped or written before it kept
// nonces. It replaces any nonces the store already holds.
func (s *Store) LoadNonces(nonces []Nonce) error {
	if err := s.dropPrefix([]byte{NonceTag}); err != nil {
		return fmt.Errorf("badgerutxo: drop nonces: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	for _, n := range nonces {
		if err := wb.Set(nonceKey(n.Point), encodeNonce(n)); err != nil {
			return fmt.Errorf("badgerutxo: load nonces: %w", err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load nonces: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Delete(legacyNoncesKey); err != nil {
			return err
		}
		return txn.Set(noncesKey, []byte{1})
	})
}

// NoncesLoaded reports whether the store holds every block nonce up to its
// tip.
func (s *Store) NoncesLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(noncesKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}
