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

// Derived reward rows. A precompute derives the pool and account outputs of
// an epoch, and the cursor that resumes it, from the epoch's snapshot inputs,
// so every row of an epoch starts with its tag and the epoch and dropping an
// epoch is a prefix delete:
//
//	U <epoch> 'p' <pool>                                    one pool's output
//	U <epoch> 'a' <tag:1> <keylen:1> <key> <poollen:1> <pool> <typelen:1> <type>
//	                                                        one account output,
//	                                                        credential first
//	Z <epoch>                                               the epoch's cursor
//
// The rows of an epoch are written under a range tombstone and carry no
// journal entry of their own, so a rollback past the epoch's capture drops
// them with one entry; a replace wipes them without one. A credential's
// outputs of an epoch share a prefix, so one credential's eligibility is read
// without a scan of the epoch. rewardDerivedKey is set once the rows are
// loaded.
const (
	// RewardPoolOutputKind and RewardAccountOutputKind are the kind bytes of
	// a U row.
	RewardPoolOutputKind    byte = 0x70
	RewardAccountOutputKind byte = 0x61
)

// RewardDerivedEpochPrefix is the prefix of every U row of epoch.
func RewardDerivedEpochPrefix(epoch uint64) []byte {
	return rewardEpochPrefix(RewardDerivedTag, epoch)
}

// RewardCursorKey keys the precompute cursor of epoch.
func RewardCursorKey(epoch uint64) []byte {
	return rewardEpochPrefix(RewardCursorTag, epoch)
}

// RewardPoolOutputPrefix is the prefix of every pool output row of epoch.
func RewardPoolOutputPrefix(epoch uint64) []byte {
	return append(RewardDerivedEpochPrefix(epoch), RewardPoolOutputKind)
}

// RewardPoolOutputKey keys the output of pool in epoch.
func RewardPoolOutputKey(epoch uint64, pool []byte) []byte {
	return append(RewardPoolOutputPrefix(epoch), pool...)
}

// DecodeRewardPoolOutputKey splits a pool output row key.
func DecodeRewardPoolOutputKey(k []byte) (uint64, []byte, error) {
	if len(k) < 10 || k[0] != RewardDerivedTag ||
		k[9] != RewardPoolOutputKind {
		return 0, nil, errors.New(
			"badgerutxo: malformed reward pool output key",
		)
	}
	return binary.BigEndian.Uint64(k[1:9]), k[10:], nil
}

// RewardAccountOutputPrefix is the prefix of every account output row of
// epoch.
func RewardAccountOutputPrefix(epoch uint64) []byte {
	return append(RewardDerivedEpochPrefix(epoch), RewardAccountOutputKind)
}

// RewardAccountCredentialPrefix is the prefix of every account output row of
// one credential in epoch.
func RewardAccountCredentialPrefix(
	epoch uint64,
	tag uint8,
	key []byte,
) []byte {
	return appendLenPrefixed(append(RewardAccountOutputPrefix(epoch), tag), key)
}

// RewardAccountOutputKey keys one account output of epoch.
func RewardAccountOutputKey(k RewardOutputKey) []byte {
	b := RewardAccountCredentialPrefix(k.Epoch, k.Tag, k.Key)
	b = appendLenPrefixed(b, k.Pool)
	return appendLenPrefixed(b, []byte(k.Type))
}

// DecodeRewardAccountOutputKey splits an account output row key.
func DecodeRewardAccountOutputKey(pk []byte) (RewardOutputKey, error) {
	var k RewardOutputKey
	if len(pk) < 10 || pk[0] != RewardDerivedTag ||
		pk[9] != RewardAccountOutputKind {
		return k, errors.New("badgerutxo: not a reward account output key")
	}
	k.Epoch = binary.BigEndian.Uint64(pk[1:9])
	r := rewardKeyReader{b: pk[10:]}
	if t := r.take(1); t != nil {
		k.Tag = t[0]
	}
	k.Key = append([]byte(nil), r.prefixed()...)
	k.Pool = append([]byte(nil), r.prefixed()...)
	k.Type = string(r.prefixed())
	if r.bad || len(r.b) != 0 {
		return k, errors.New("badgerutxo: malformed reward account output key")
	}
	return k, nil
}

// LoadRewardDerived replaces the store's derived reward rows with the ones
// feed supplies, each as a primary key and value, and marks them loaded, for
// a store from before it kept them.
func (s *Store) LoadRewardDerived(
	feed func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error,
) error {
	if err := s.dropPrefix(
		[]byte{RewardDerivedTag}, []byte{RewardCursorTag},
	); err != nil {
		return fmt.Errorf("badgerutxo: drop derived reward rows: %w", err)
	}
	wb := s.newWriteBatch()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 ||
			(k[0] != RewardDerivedTag && k[0] != RewardCursorTag) {
			return fmt.Errorf("badgerutxo: %x is not a derived reward key", k)
		}
		return wb.Set(k, v)
	}, loadTombstone(wb, []byte{RewardDerivedTag, RewardCursorTag}))
	if err != nil {
		return fmt.Errorf("badgerutxo: load derived reward rows: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load derived reward rows: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(rewardDerivedKey, []byte{1})
	})
}

// RewardDerivedLoaded reports whether the store holds the derived reward
// rows up to its tip.
func (s *Store) RewardDerivedLoaded() (bool, error) {
	return s.hasMarker(rewardDerivedKey)
}
