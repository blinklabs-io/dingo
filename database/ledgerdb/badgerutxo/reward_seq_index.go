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

// The reward sequence index keys every credit record by its sequence number:
//
//	mrwq <seq:8 BE> -> the record's primary key
//
// so a reader that resumes after the last sequence number it took seeks to it
// instead of reading every record dated in its range. It is derived from the
// records and written, deleted and rebuilt with them; a page verifies each
// entry against the record it names, so a leftover entry only costs a skip.
// Withdrawals are not indexed: no reader pages them.

const (
	rewardSeqIndexKeyLen = 4 + 8
	// rewardIndexBuildBatch bounds the records one build transaction
	// rewrites.
	rewardIndexBuildBatch = 1000
)

func rewardSeqIndexKey(seq uint64) []byte {
	return binary.BigEndian.AppendUint64(bytes.Clone(rewardSeqIndexPrefix), seq)
}

// rewardKeyIsCredit reports whether a record key names a credit.
func rewardKeyIsCredit(k []byte) bool {
	return len(k) > 9 && k[9] == 0
}

// setRewardSeqIndex indexes the credit at key k with sequence number seq.
func setRewardSeqIndex(txn *badger.Txn, k []byte, seq uint64) error {
	if !rewardKeyIsCredit(k) {
		return nil
	}
	return txn.Set(rewardSeqIndexKey(seq), bytes.Clone(k))
}

// deleteRewardSeqIndex removes the index entry of the record held under k
// with value v.
func deleteRewardSeqIndex(txn *badger.Txn, k, v []byte) error {
	if !rewardKeyIsCredit(k) || len(v) != rewardRecordLen {
		return nil
	}
	return txn.Delete(rewardSeqIndexKey(binary.BigEndian.Uint64(v[9:])))
}

// deleteRewardSeqIndexFor removes the index entries of the records under keys,
// before the records themselves are deleted.
func deleteRewardSeqIndexFor(txn *badger.Txn, keys [][]byte) error {
	for _, k := range keys {
		item, err := txn.Get(k)
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
		if err := deleteRewardSeqIndex(txn, k, v); err != nil {
			return err
		}
	}
	return nil
}

// dropRewardSeqIndex deletes every index entry.
func (s *Store) dropRewardSeqIndex() error {
	if s.snap != nil {
		return ErrSnapshotReadOnly
	}
	return s.db.DropPrefix(rewardSeqIndexPrefix)
}

// ensureRewardSeqIndex gives every reward record a sequence number and an
// index entry, for a store written before either existed, and marks it done.
// A record without a number takes the next one in key order, which is slot
// order. Each transaction numbers and indexes its records together with the
// counter, so a stop between transactions leaves a prefix numbered and the
// next run continues with the rest; a record that already has both is
// written again unchanged.
func (s *Store) ensureRewardSeqIndex() error {
	var built bool
	if err := s.db.View(func(txn *badger.Txn) error {
		if err := checkBootstrapComplete(txn); err != nil {
			return err
		}
		_, err := txn.Get(rewardIndexedKey)
		built = err == nil
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
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
	var after []byte
	for {
		var done bool
		err := s.db.Update(func(txn *badger.Txn) error {
			next, err := readRewardSeq(txn)
			if err != nil {
				return err
			}
			type row struct{ k, v []byte }
			var rows []row
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte{RewardRecordTag}
			it := txn.NewIterator(opts)
			start := []byte{RewardRecordTag}
			if after != nil {
				start = append(bytes.Clone(after), 0)
			}
			for it.Seek(start); it.Valid() && len(rows) < rewardIndexBuildBatch; it.Next() {
				v, err := it.Item().ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}
				rows = append(rows, row{it.Item().KeyCopy(nil), v})
			}
			it.Close()
			done = len(rows) < rewardIndexBuildBatch
			numbered := false
			for _, r := range rows {
				v := r.v
				if len(v) == rewardRecordAmountLen {
					v = binary.BigEndian.AppendUint64(append(v, 0), next)
					next++
					numbered = true
					if err := txn.Set(r.k, v); err != nil {
						return err
					}
				}
				if len(v) != rewardRecordLen {
					return fmt.Errorf("badgerutxo: malformed reward record %x", r.k)
				}
				if err := setRewardSeqIndex(txn, r.k, binary.BigEndian.Uint64(v[9:])); err != nil {
					return err
				}
			}
			if len(rows) > 0 {
				after = rows[len(rows)-1].k
			}
			if numbered {
				return txn.Set(rewardSeqKey, binary.BigEndian.AppendUint64(nil, next))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("badgerutxo: index reward records: %w", err)
		}
		if done {
			break
		}
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(rewardIndexedKey, []byte{1})
	})
}

// RewardIndexVisited returns how many index entries credit pages have
// examined since the store opened. It exists to show that a page's cost does
// not grow with the records it skips.
func (s *Store) RewardIndexVisited() uint64 {
	return s.rewardIndexVisited.Load()
}

// RewardCreditsAfter returns the credits dated in [from, through] in the order
// the store wrote them, those numbered above cursor when cursor is positive.
// It returns at most limit of them, and more is true when another follows,
// except that a record numbered zero is always returned: zero is also the
// cursor that starts a read, so a page cannot end on it.
func (s *Store) RewardCreditsAfter(
	from, through, cursor uint64,
	limit int,
) (recs []RewardRecord, more bool, err error) {
	start := uint64(0)
	if cursor > 0 {
		start = cursor + 1
	}
	err = s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = rewardSeqIndexPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(rewardSeqIndexKey(start)); it.Valid(); it.Next() {
			s.rewardIndexVisited.Add(1)
			ik := it.Item().Key()
			if len(ik) != rewardSeqIndexKeyLen {
				continue
			}
			seq := binary.BigEndian.Uint64(ik[4:])
			pk, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(pk) < 11 {
				continue
			}
			slot := binary.BigEndian.Uint64(pk[1:9])
			if slot < from || slot > through {
				continue
			}
			rec, ok, err := readIndexedReward(txn, pk, seq)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if len(recs) >= limit && rec.Seq > 0 {
				more = true
				return nil
			}
			recs = append(recs, rec)
		}
		return nil
	})
	return recs, more, err
}

// readIndexedReward returns the credit under pk when it still carries seq.
func readIndexedReward(
	txn *badger.Txn,
	pk []byte,
	seq uint64,
) (RewardRecord, bool, error) {
	item, err := txn.Get(pk)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return RewardRecord{}, false, nil
	}
	if err != nil {
		return RewardRecord{}, false, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return RewardRecord{}, false, err
	}
	if len(v) != rewardRecordLen || binary.BigEndian.Uint64(v[9:]) != seq {
		return RewardRecord{}, false, nil
	}
	srcLen := int(pk[10])
	if len(pk) < 11+srcLen+1 {
		return RewardRecord{}, false, fmt.Errorf(
			"badgerutxo: malformed reward record key %x", pk,
		)
	}
	return RewardRecord{
		Slot:         binary.BigEndian.Uint64(pk[1:9]),
		Source:       bytes.Clone(pk[11 : 11+srcLen]),
		Tag:          pk[11+srcLen],
		Key:          bytes.Clone(pk[11+srcLen+1:]),
		Amount:       binary.BigEndian.Uint64(v),
		PostSnapshot: v[8] == 1,
		Seq:          seq,
	}, true, nil
}
