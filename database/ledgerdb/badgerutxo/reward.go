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

// Reward rows. The set keeps the credit-eligible, unfolded reward account
// outputs, the credited rounds and the post-snapshot credits, each a primary
// row of the state journal:
//
//	O <tag:1> <keylen:1> <key> <epoch:8 BE> <poollen:1> <pool> <typelen:1> <type>
//	                              amount, captured slot, boundary slot, 8 BE each
//	F <epoch:8 BE> <tag:1> <keylen:1> <key> <poollen:1> <pool> <typelen:1> <type>
//	                              empty: the epoch index of the output above
//	W <epoch:8 BE>                boundary slot, 8 BE
//	X <slot:8 BE> <tag:1> <keylen:1> <key> <kind:1> <source>
//	                              amount, 8 BE
//
// rewardOutputsKey is set once all four are loaded.
// RewardOutputKey is the identity of a reward account output apart from its
// epoch and credential tag and key.
type RewardOutputKey struct {
	Tag   uint8
	Key   []byte
	Epoch uint64
	Pool  []byte
	Type  string
}

func appendLenPrefixed(b, v []byte) []byte {
	b = append(b, byte(len(v))) //nolint:gosec // a caller bounds the part to a byte
	return append(b, v...)
}

// ValidRewardOutputKey reports whether every variable part of k fits the
// key's one-byte length prefix.
func ValidRewardOutputKey(k RewardOutputKey) bool {
	return len(k.Key) <= 0xff && len(k.Pool) <= 0xff && len(k.Type) <= 0xff
}

// RewardCredentialPrefix is the prefix of every output row of one credential.
func RewardCredentialPrefix(tag uint8, key []byte) []byte {
	return appendLenPrefixed([]byte{RewardOutputTag, tag}, key)
}

func rewardOutputTail(k RewardOutputKey) []byte {
	b := binary.BigEndian.AppendUint64(nil, k.Epoch)
	b = appendLenPrefixed(b, k.Pool)
	return appendLenPrefixed(b, []byte(k.Type))
}

// RewardOutputRowKey is the primary key of an output row.
func RewardOutputRowKey(k RewardOutputKey) []byte {
	return append(RewardCredentialPrefix(k.Tag, k.Key), rewardOutputTail(k)...)
}

// RewardEpochPrefix is the prefix of every epoch index row of epoch.
func RewardEpochPrefix(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{RewardEpochTag}, epoch)
}

// RewardEpochRowKey is the key of an output's epoch index row.
func RewardEpochRowKey(k RewardOutputKey) []byte {
	b := appendLenPrefixed([]byte{k.Tag}, k.Key)
	b = appendLenPrefixed(b, k.Pool)
	b = appendLenPrefixed(b, []byte(k.Type))
	return append(RewardEpochPrefix(k.Epoch), b...)
}

// RewardOutputValue encodes an output row's value.
func RewardOutputValue(amount, captured, boundary uint64) []byte {
	b := binary.BigEndian.AppendUint64(nil, amount)
	b = binary.BigEndian.AppendUint64(b, captured)
	return binary.BigEndian.AppendUint64(b, boundary)
}

// DecodeRewardOutputValue decodes an output row's value.
func DecodeRewardOutputValue(v []byte) (amount, captured, boundary uint64, err error) {
	if len(v) != 24 {
		return 0, 0, 0, errors.New("badgerutxo: malformed reward output row")
	}
	return binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:]),
		binary.BigEndian.Uint64(v[16:]), nil
}

type rewardKeyReader struct {
	b   []byte
	bad bool
}

func (r *rewardKeyReader) take(n int) []byte {
	if r.bad || len(r.b) < n {
		r.bad = true
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *rewardKeyReader) prefixed() []byte {
	n := r.take(1)
	if n == nil {
		return nil
	}
	return r.take(int(n[0]))
}

func (r *rewardKeyReader) epoch() uint64 {
	e := r.take(8)
	if e == nil {
		return 0
	}
	return binary.BigEndian.Uint64(e)
}

// DecodeRewardOutputRowKey decodes an output row's primary key.
func DecodeRewardOutputRowKey(pk []byte) (RewardOutputKey, error) {
	var k RewardOutputKey
	if len(pk) == 0 || pk[0] != RewardOutputTag {
		return k, errors.New("badgerutxo: not a reward output key")
	}
	r := rewardKeyReader{b: pk[1:]}
	if t := r.take(1); t != nil {
		k.Tag = t[0]
	}
	k.Key = append([]byte(nil), r.prefixed()...)
	k.Epoch = r.epoch()
	k.Pool = append([]byte(nil), r.prefixed()...)
	k.Type = string(r.prefixed())
	if r.bad || len(r.b) != 0 {
		return k, errors.New("badgerutxo: malformed reward output key")
	}
	return k, nil
}

// DecodeRewardEpochRowKey decodes an epoch index row's key.
func DecodeRewardEpochRowKey(pk []byte) (RewardOutputKey, error) {
	var k RewardOutputKey
	if len(pk) == 0 || pk[0] != RewardEpochTag {
		return k, errors.New("badgerutxo: not a reward epoch key")
	}
	r := rewardKeyReader{b: pk[1:]}
	k.Epoch = r.epoch()
	if t := r.take(1); t != nil {
		k.Tag = t[0]
	}
	k.Key = append([]byte(nil), r.prefixed()...)
	k.Pool = append([]byte(nil), r.prefixed()...)
	k.Type = string(r.prefixed())
	if r.bad || len(r.b) != 0 {
		return k, errors.New("badgerutxo: malformed reward epoch key")
	}
	return k, nil
}

// RewardRoundRowKey is the key of a credited round.
func RewardRoundRowKey(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{RewardRoundTag}, epoch)
}

// RewardRoundPrefix is the prefix of every credited round.
func RewardRoundPrefix() []byte { return []byte{RewardRoundTag} }

// PostSnapshotCreditPrefix is the prefix of the credits at slot.
func PostSnapshotCreditPrefix(slot uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{RewardPostSnapTag}, slot)
}

// PostSnapshotCreditRowKey is the key of one credit at slot to the
// credential (tag, key). A loaded total has kind 0 and no source.
func PostSnapshotCreditRowKey(
	slot uint64,
	tag uint8,
	key []byte,
	kind uint8,
	source []byte,
) []byte {
	k := PostSnapshotCreditPrefix(slot)
	k = append(k, tag)
	k = appendLenPrefixed(k, key)
	k = append(k, kind)
	return append(k, source...)
}

// DecodePostSnapshotCreditRowKey decodes a credit row's key.
func DecodePostSnapshotCreditRowKey(
	pk []byte,
) (slot uint64, tag uint8, key []byte, kind uint8, source []byte, err error) {
	if len(pk) == 0 || pk[0] != RewardPostSnapTag {
		return 0, 0, nil, 0, nil, errors.New("badgerutxo: not a post-snapshot credit key")
	}
	r := rewardKeyReader{b: pk[1:]}
	slot = r.epoch()
	if t := r.take(1); t != nil {
		tag = t[0]
	}
	key = append([]byte(nil), r.prefixed()...)
	if kd := r.take(1); kd != nil {
		kind = kd[0]
	}
	if r.bad {
		return 0, 0, nil, 0, nil, errors.New("badgerutxo: malformed post-snapshot credit key")
	}
	return slot, tag, key, kind, append([]byte(nil), r.b...), nil
}

// wipeRewardEpoch deletes every output row of epoch and its index rows,
// without a journal entry: the metadata store's delete of an epoch's outputs
// is never undone by a rollback.
func wipeRewardEpoch(txn *badger.Txn, epoch uint64) error {
	var keys [][]byte
	opts := badger.DefaultIteratorOptions
	opts.Prefix = RewardEpochPrefix(epoch)
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	it.Close()
	for _, idx := range keys {
		k, err := DecodeRewardEpochRowKey(idx)
		if err != nil {
			return err
		}
		if err := txn.Delete(RewardOutputRowKey(k)); err != nil {
			return err
		}
		if err := txn.Delete(idx); err != nil {
			return err
		}
	}
	return nil
}

// RewardEpochs returns the epochs that have an output row, oldest first.
func (s *Store) RewardEpochs() ([]uint64, error) {
	var out []uint64
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{RewardEpochTag}
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); {
			k := it.Item().Key()
			if len(k) < 9 {
				return errors.New("badgerutxo: malformed reward epoch key")
			}
			epoch := binary.BigEndian.Uint64(k[1:9])
			out = append(out, epoch)
			if epoch == ^uint64(0) {
				return nil
			}
			it.Seek(RewardEpochPrefix(epoch + 1))
		}
		return nil
	})
	return out, err
}

// LoadRewardOutputs replaces the store's reward rows with the ones feed
// supplies, each as a primary key and value, and marks them loaded, for a
// store from before it kept them.
func (s *Store) LoadRewardOutputs(feed func(put func(k, v []byte) error) error) error {
	if err := s.dropPrefix(
		[]byte{RewardOutputTag}, []byte{RewardEpochTag},
		[]byte{RewardRoundTag}, []byte{RewardPostSnapTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop reward rows: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || (k[0] != RewardOutputTag && k[0] != RewardEpochTag &&
			k[0] != RewardRoundTag && k[0] != RewardPostSnapTag) {
			return fmt.Errorf("badgerutxo: %x is not a reward key", k)
		}
		return wb.Set(k, v)
	})
	if err != nil {
		return fmt.Errorf("badgerutxo: load reward rows: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load reward rows: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(rewardOutputsKey, []byte{1})
	})
}

// RewardOutputsLoaded reports whether the store holds every reward row up
// to its tip.
func (s *Store) RewardOutputsLoaded() (bool, error) {
	var ok bool
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(rewardOutputsKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		ok = err == nil
		return err
	})
	return ok, err
}
