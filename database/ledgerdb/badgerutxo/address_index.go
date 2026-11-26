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
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// The output index keys every live output by the lookup keys an Indexer
// derives from its CBOR:
//
//	mai <body> <ref:36> -> empty
//
// A body is whatever the Indexer returns, so a reader asking for body b walks
// the rows under "mai"+b in ref order. Every body of one kind has one length,
// except a kind holding whole addresses, so a row under a longer body that
// starts with b is told apart by its length. The index is derived from the
// outputs and is written, deleted and rebuilt with them: each site that sets
// or deletes an output row sets or deletes its index rows in the same
// transaction, so a rollback or a journal replay restores them with the
// output, and a bootstrap drops them with it. A reader loads the output named
// by an index row and never trusts the row alone.

// addressIndexBuildBatch bounds the outputs one build transaction indexes.
const addressIndexBuildBatch = 500

// ErrAddressIndexUnavailable reports a read of the output index of a store
// that has no complete one: it was opened without an Indexer, or its contents
// are not finished.
var ErrAddressIndexUnavailable = errors.New(
	"badgerutxo: the output index is not available",
)

// Indexer returns the index bodies of an output: the lookup keys it is found
// under. An output whose CBOR cannot be read has none.
type Indexer func(cbor []byte) [][]byte

// WithIndexer keeps an index of the live outputs under the bodies ix returns,
// updated in the same transaction as the outputs themselves.
func WithIndexer(ix Indexer) Option {
	return func(s *Store) { s.indexer = ix }
}

func addressIndexKey(body []byte, r Ref) []byte {
	k := make([]byte, 0, len(addressIndexPrefix)+len(body)+refLen)
	k = append(k, addressIndexPrefix...)
	k = append(k, body...)
	return append(k, r[:]...)
}

// indexKeys returns the index rows of the output r with CBOR cbor.
func (s *Store) indexKeys(r Ref, cbor []byte) [][]byte {
	if s.indexer == nil {
		return nil
	}
	bodies := s.indexer(cbor)
	keys := make([][]byte, 0, len(bodies))
	for _, b := range bodies {
		keys = append(keys, addressIndexKey(b, r))
	}
	return keys
}

// setIndex writes the index rows of the output r with CBOR cbor.
func (s *Store) setIndex(txn *badger.Txn, r Ref, cbor []byte) error {
	for _, k := range s.indexKeys(r, cbor) {
		if err := txn.Set(k, nil); err != nil {
			return err
		}
	}
	return nil
}

// deleteIndex deletes the index rows of the output r with CBOR cbor.
func (s *Store) deleteIndex(txn *badger.Txn, r Ref, cbor []byte) error {
	for _, k := range s.indexKeys(r, cbor) {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// ensureAddressIndex gives every live output its index rows, for a store
// written before the index existed, and marks it done. Each transaction
// indexes a batch of outputs and records the last of them with the rows, so a
// stop between transactions leaves a prefix indexed and the next run
// continues after it; an output indexed twice writes the same rows.
func (s *Store) ensureAddressIndex() error {
	if s.indexer == nil {
		return nil
	}
	var after []byte
	var built bool
	batches := 0
	if err := s.db.View(func(txn *badger.Txn) error {
		if err := checkBootstrapComplete(txn); err != nil {
			return err
		}
		if _, err := txn.Get(addressIndexedKey); err == nil {
			built = true
			return nil
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		item, err := txn.Get(addressIndexAfterKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		after, err = item.ValueCopy(nil)
		return err
	}); err != nil {
		if errors.Is(err, ErrBootstrapIncomplete) {
			return nil
		}
		return err
	}
	if built {
		return nil
	}
	for {
		done := false
		err := s.db.Update(func(txn *badger.Txn) error {
			type row struct {
				ref  Ref
				cbor []byte
			}
			var rows []row
			it := txn.NewIterator(badger.IteratorOptions{
				Prefix:         []byte{UTxOTag},
				PrefetchValues: true,
				PrefetchSize:   addressIndexBuildBatch,
			})
			start := []byte{UTxOTag}
			if after != nil {
				start = append(bytes.Clone(after), 0)
			}
			for it.Seek(start); it.Valid() && len(rows) < addressIndexBuildBatch; it.Next() {
				v, err := it.Item().ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}
				var r Ref
				copy(r[:], it.Item().Key()[1:])
				rows = append(rows, row{r, v})
			}
			it.Close()
			done = len(rows) < addressIndexBuildBatch
			for _, r := range rows {
				if err := s.setIndex(txn, r.ref, r.cbor); err != nil {
					return err
				}
			}
			if len(rows) == 0 {
				return nil
			}
			after = utxoKey(rows[len(rows)-1].ref)
			return txn.Set(addressIndexAfterKey, after)
		})
		if err != nil {
			return fmt.Errorf("badgerutxo: index outputs: %w", err)
		}
		batches++
		if s.afterIndexBatch != nil {
			if err := s.afterIndexBatch(batches); err != nil {
				return err
			}
		}
		if done {
			break
		}
	}
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(addressIndexedKey, []byte{1}); err != nil {
			return err
		}
		return txn.Delete(addressIndexAfterKey)
	})
}

// markAddressIndexed records, in txn, that every live output has its index
// rows; a bootstrap writes them with the outputs.
func (s *Store) markAddressIndexed(txn *badger.Txn) error {
	if s.indexer == nil {
		return nil
	}
	return txn.Set(addressIndexedKey, []byte{1})
}

func checkAddressIndexed(txn *badger.Txn) error {
	_, err := txn.Get(addressIndexedKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ErrAddressIndexUnavailable
	}
	return err
}

// EachIndexed calls fn for the live outputs found under any of bodies, in
// ref order and each once, starting after the output after when it is not
// nil. fn returns true to stop. The outputs and the index are read from one
// snapshot.
func (s *Store) EachIndexed(
	bodies [][]byte,
	after *Ref,
	fn func(Output) (stop bool, err error),
) error {
	return s.view(func(txn *badger.Txn) error {
		if err := checkAddressIndexed(txn); err != nil {
			return err
		}
		type cursor struct {
			it     *badger.Iterator
			prefix []byte
			ref    Ref
			ok     bool
		}
		cursors := make([]*cursor, 0, len(bodies))
		defer func() {
			for _, c := range cursors {
				c.it.Close()
			}
		}()
		// load settles c on its next row of the right length, past after.
		load := func(c *cursor) {
			c.ok = false
			for ; c.it.Valid(); c.it.Next() {
				k := c.it.Item().Key()
				if len(k) != len(c.prefix)+refLen {
					continue
				}
				copy(c.ref[:], k[len(c.prefix):])
				if after != nil && bytes.Compare(c.ref[:], after[:]) <= 0 {
					continue
				}
				c.ok = true
				return
			}
		}
		for _, b := range bodies {
			prefix := append(bytes.Clone(addressIndexPrefix), b...)
			c := &cursor{
				it: txn.NewIterator(badger.IteratorOptions{
					Prefix: prefix,
				}),
				prefix: prefix,
			}
			cursors = append(cursors, c)
			if after != nil {
				c.it.Seek(append(bytes.Clone(prefix), after[:]...))
			} else {
				c.it.Rewind()
			}
			load(c)
		}
		for {
			var next *cursor
			for _, c := range cursors {
				if c.ok && (next == nil ||
					bytes.Compare(c.ref[:], next.ref[:]) < 0) {
					next = c
				}
			}
			if next == nil {
				return nil
			}
			ref := next.ref
			for _, c := range cursors {
				if c.ok && c.ref == ref {
					c.it.Next()
					load(c)
				}
			}
			item, err := txn.Get(utxoKey(ref))
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			stop, err := fn(Output{Ref: ref, Cbor: v})
			if err != nil || stop {
				return err
			}
		}
	})
}

// EachAfter calls fn for every live output after the output after, or from
// the first when after is nil, in ref order until fn returns true.
func (s *Store) EachAfter(
	after *Ref,
	fn func(Output) (stop bool, err error),
) error {
	return s.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:         []byte{UTxOTag},
			PrefetchValues: true,
		})
		defer it.Close()
		if after != nil {
			it.Seek(utxoKey(*after))
		} else {
			it.Rewind()
		}
		for ; it.Valid(); it.Next() {
			var r Ref
			copy(r[:], it.Item().Key()[1:])
			if after != nil && r == *after {
				continue
			}
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			stop, err := fn(Output{Ref: r, Cbor: v})
			if err != nil || stop {
				return err
			}
		}
		return nil
	})
}

// AddressIndexed reports whether the store keeps a complete output index.
func (s *Store) AddressIndexed() (bool, error) {
	err := s.view(checkAddressIndexed)
	if errors.Is(err, ErrAddressIndexUnavailable) {
		return false, nil
	}
	return err == nil, err
}
