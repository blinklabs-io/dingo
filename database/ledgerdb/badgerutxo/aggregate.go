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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

const aggTag = 'a'

// Aggregator assigns a live output to a running sum: the sum's key and the
// amount the output adds to it. ok is false for an output that adds to no
// sum.
type Aggregator func(cbor []byte) (key []byte, amount uint64, ok bool)

// Option configures a Store.
type Option func(*Store)

// WithAggregator keeps a running sum per key over the live outputs, updated
// in the same transaction as the outputs themselves.
func WithAggregator(agg Aggregator) Option {
	return func(s *Store) { s.agg = agg }
}

func aggKey(key []byte) []byte {
	return append([]byte{aggTag}, key...)
}

// aggDeltas collects one transaction's changes to the sums.
type aggDeltas map[string]int64

func (s *Store) addOutput(d aggDeltas, cbor []byte, sign int64) {
	if s.agg == nil {
		return
	}
	key, amount, ok := s.agg(cbor)
	if !ok || amount == 0 {
		return
	}
	d[string(key)] += sign * int64(amount) //nolint:gosec // lovelace fits in int64
}

func applyAggDeltas(txn *badger.Txn, d aggDeltas) error {
	for key, delta := range d {
		if delta == 0 {
			continue
		}
		k := aggKey([]byte(key))
		cur, err := readUint64(txn, k)
		if err != nil {
			return err
		}
		next := int64(cur) + delta //nolint:gosec // lovelace fits in int64
		if next < 0 {
			return fmt.Errorf("badgerutxo: sum %x would go negative", key)
		}
		if next == 0 {
			if err := txn.Delete(k); err != nil {
				return err
			}
			continue
		}
		if err := txn.Set(k, binary.BigEndian.AppendUint64(nil, uint64(next))); err != nil {
			return err
		}
	}
	return nil
}

func readUint64(txn *badger.Txn, k []byte) (uint64, error) {
	item, err := txn.Get(k)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var n uint64
	err = item.Value(func(v []byte) error {
		n = binary.BigEndian.Uint64(v)
		return nil
	})
	return n, err
}

// Sum returns the running sum for key, zero when no live output adds to it.
func (s *Store) Sum(key []byte) (uint64, error) {
	var n uint64
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		n, err = readUint64(txn, aggKey(key))
		return err
	})
	return n, err
}

// EachSum calls fn for every non-zero running sum, in key order, until fn
// returns an error.
func (s *Store) EachSum(fn func(key []byte, amount uint64) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{aggTag}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var n uint64
			if err := item.Value(func(v []byte) error {
				n = binary.BigEndian.Uint64(v)
				return nil
			}); err != nil {
				return err
			}
			if err := fn(item.KeyCopy(nil)[1:], n); err != nil {
				return err
			}
		}
		return nil
	})
}
