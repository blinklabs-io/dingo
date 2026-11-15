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
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Committee and constitution rows, journaled like the other computed rows:
//
//	C <slot:8> <tag:1> <cold hash>   a committee member row, by its added slot
//	Q <slot:8>                       a committee quorum row
//	N <slot:8>                       a constitution row
//	H <tag:1> <cold credential>      a cold credential's latest hot-key authorization
//	R <tag:1> <cold credential>      a cold credential's latest resignation slot
//	mcommittee                       set once these rows are complete
//
// LoadCommittee replaces the store's committee and constitution rows with
// the ones feed supplies, each as a primary key and value, and marks them
// loaded, for a store from before it kept them.
func (s *Store) LoadCommittee(feed func(put func(k, v []byte) error) error) error {
	if err := s.dropPrefix(
		[]byte{CommitteeMemberTag}, []byte{CommitteeQuorumTag}, []byte{ConstitutionTag},
		[]byte{HotAuthTag}, []byte{ResignTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop committee rows: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || (k[0] != CommitteeMemberTag && k[0] != CommitteeQuorumTag &&
			k[0] != ConstitutionTag && k[0] != HotAuthTag && k[0] != ResignTag) {
			return fmt.Errorf("badgerutxo: %x is not a committee key", k)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load committee rows: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load committee rows: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(committeeKey, []byte{1})
	})
}

// CommitteeLoaded reports whether the store holds the committee and
// constitution rows up to its tip.
func (s *Store) CommitteeLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(committeeKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}
