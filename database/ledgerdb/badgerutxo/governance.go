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

// Governance proposal and vote rows, journaled like the other computed rows:
//
//	P <tx hash:32> <action index:4 BE>              a governance proposal
//	V <tx hash:32> <action index:4 BE> <voter ...>  a vote on that proposal
//	mgov2                                           set once they are complete
//
// A vote's key ends with its voter type, credential tag and credential.
// LoadGovernance replaces the store's proposal and vote rows with the ones
// feed supplies, each as a primary key and value, and marks them loaded, for
// a store from before it kept them. Every proposal and vote value begins with
// its 8-byte big-endian row ID, which the ID sequence is derived from.
func (s *Store) LoadGovernance(feed func(put func(k, v []byte) error) error) error {
	if err := s.dropPrefix([]byte{ProposalTag}, []byte{VoteTag}); err != nil {
		return fmt.Errorf("badgerutxo: drop governance rows: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || (k[0] != ProposalTag && k[0] != VoteTag) {
			return fmt.Errorf("badgerutxo: %x is not a governance key", k)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load governance rows: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load governance rows: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Delete(legacyGovernanceKey); err != nil {
			return err
		}
		// The loaded rows are the whole history, so the sequence restarts
		// above their highest IDs, which the first read derives from them.
		if err := txn.Delete(governanceSeqKey); err != nil {
			return err
		}
		return txn.Set(governanceKey, []byte{1})
	})
}

// GovernanceIDs are governance row IDs: the highest one a batch carries when
// it is a batch's field, the next one to allocate when the store returns it.
type GovernanceIDs struct {
	Proposal, Vote uint64
}

// maxGovernanceIDs scans the proposal and vote rows for their highest IDs.
func maxGovernanceIDs(txn *badger.Txn) (GovernanceIDs, error) {
	var out GovernanceIDs
	for _, c := range []struct {
		tag byte
		to  *uint64
	}{{ProposalTag, &out.Proposal}, {VoteTag, &out.Vote}} {
		err := eachPrefix(txn, []byte{c.tag}, func(_, v []byte) error {
			if len(v) < 8 {
				return errors.New("badgerutxo: malformed governance row")
			}
			*c.to = max(*c.to, binary.BigEndian.Uint64(v))
			return nil
		})
		if err != nil {
			return GovernanceIDs{}, err
		}
	}
	return out, nil
}

// readGovernanceSeq returns the next proposal and vote IDs. A store that
// predates the sequence derives them from its rows.
func readGovernanceSeq(txn *badger.Txn) (GovernanceIDs, error) {
	item, err := txn.Get(governanceSeqKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		highest, err := maxGovernanceIDs(txn)
		return GovernanceIDs{highest.Proposal + 1, highest.Vote + 1}, err
	}
	if err != nil {
		return GovernanceIDs{}, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return GovernanceIDs{}, err
	}
	if len(v) != 16 {
		return GovernanceIDs{}, errors.New(
			"badgerutxo: malformed governance sequence",
		)
	}
	return GovernanceIDs{
		binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:]),
	}, nil
}

// raiseGovernanceSeq makes the next IDs exceed the ones in highest. A store
// without a sequence derives it from the rows first, which the batch's own
// rows already are part of.
func raiseGovernanceSeq(txn *badger.Txn, highest *GovernanceIDs) error {
	if highest == nil {
		return nil
	}
	cur, err := readGovernanceSeq(txn)
	if err != nil {
		return err
	}
	next := GovernanceIDs{
		max(cur.Proposal, highest.Proposal+1),
		max(cur.Vote, highest.Vote+1),
	}
	v := binary.BigEndian.AppendUint64(nil, next.Proposal)
	return txn.Set(
		governanceSeqKey,
		binary.BigEndian.AppendUint64(v, next.Vote),
	)
}

// GovernanceNextIDs returns the next proposal and vote row IDs, which exceed
// every ID the store ever held a row under since its rows were loaded.
func (s *Store) GovernanceNextIDs() (GovernanceIDs, error) {
	var out GovernanceIDs
	err := s.view(func(txn *badger.Txn) error {
		var err error
		out, err = readGovernanceSeq(txn)
		return err
	})
	return out, err
}

// GovernanceLoaded reports whether the store holds the proposal and vote
// rows up to its tip.
func (s *Store) GovernanceLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(governanceKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}

// InvalidateGovernance removes the marker that says the store holds every
// proposal and vote row, so the next load from the metadata store replaces
// them. The rows themselves stay until that load.
func (s *Store) InvalidateGovernance() error {
	return s.update(func(txn *badger.Txn) error {
		if err := txn.Delete(governanceKey); err != nil {
			return err
		}
		return txn.Delete(legacyGovernanceKey)
	})
}
