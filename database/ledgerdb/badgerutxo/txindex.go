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

// TxIndexKey is the key of a transaction's index row.
func TxIndexKey(hash []byte) []byte {
	return append([]byte{TxIndexTag}, hash...)
}

// LoadTxIndex replaces the store's transaction index with the rows feed
// supplies, and marks it loaded.
func (s *Store) LoadTxIndex(
	feed func(put func(k, v []byte) error) error,
) error {
	return s.loadFamily(TxIndexTag, txIndexKey, "transaction index", feed)
}

// TxHashesChangedAfter lists the hash of every transaction whose index row
// changed after slot, as far back as the rollback journal reaches.
func (s *Store) TxHashesChangedAfter(slot uint64) ([][]byte, error) {
	keys, err := s.RowKeysChangedAfter([]byte{TxIndexTag}, slot)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(keys))
	for _, k := range keys {
		out = append(out, k[1:])
	}
	return out, nil
}

// TxIndexLoaded reports whether the store holds the transaction index up to
// its tip.
func (s *Store) TxIndexLoaded() (bool, error) {
	return s.hasMarker(txIndexKey)
}
