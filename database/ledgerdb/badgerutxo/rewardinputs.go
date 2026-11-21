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
	"slices"

	"github.com/dgraph-io/badger/v4"
)

// Reward input rows. Every row of an epoch starts with its tag and the epoch,
// so dropping an epoch is a prefix delete:
//
//	I <epoch> <pool:28> <tag:1> <credential:28>   one credential's stake
//	J <epoch> <pool:28>                           one pool's inputs
//	S <epoch> 0 <type>                            a snapshot's header
//	S <epoch> 1 <type>                            an imported basis failure
//	S <epoch> 2                                   the epoch's stake inputs
//	                                              are only partly written
//	A <epoch>                                     the epoch's ADA pots
//
// The stake rows of an epoch are written under a range tombstone and carry
// no journal entry of their own, so a rollback past the epoch's capture drops
// the epoch with one entry. The other rows are journaled one by one.
// rewardInputsKey is set once the rows are loaded.
const (
	// RewardSnapshotHeader and RewardSnapshotFailure are the kind bytes of an
	// S row.
	RewardSnapshotHeader  byte = 0
	RewardSnapshotFailure byte = 1
	// RewardSnapshotPending is the kind byte of an S row that marks the
	// epoch's stake rows as only partly written. Its value is the boundary
	// slot of the snapshot, 8 bytes BE.
	RewardSnapshotPending byte = 2

	// RewardHashLen is the length of a pool key hash and of a stake
	// credential in a reward input key.
	RewardHashLen = 28
)

func rewardEpochPrefix(tag byte, epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{tag}, epoch)
}

// RewardStakeInputEpochPrefix is the prefix of every stake row of epoch.
func RewardStakeInputEpochPrefix(epoch uint64) []byte {
	return rewardEpochPrefix(RewardStakeInputTag, epoch)
}

// RewardPoolInputEpochPrefix is the prefix of every pool row of epoch.
func RewardPoolInputEpochPrefix(epoch uint64) []byte {
	return rewardEpochPrefix(RewardPoolInputTag, epoch)
}

// RewardSnapshotEpochPrefix is the prefix of every snapshot row of epoch.
func RewardSnapshotEpochPrefix(epoch uint64) []byte {
	return rewardEpochPrefix(RewardSnapshotTag, epoch)
}

// RewardStakeInputKey keys one credential's stake in epoch under pool. It
// fails when pool or credential is not RewardHashLen bytes.
func RewardStakeInputKey(
	epoch uint64,
	pool []byte,
	credTag uint8,
	cred []byte,
) ([]byte, error) {
	if len(pool) != RewardHashLen || len(cred) != RewardHashLen {
		return nil, fmt.Errorf(
			"badgerutxo: reward stake input key needs %d-byte pool and "+
				"credential hashes, got %d and %d",
			RewardHashLen, len(pool), len(cred),
		)
	}
	k := RewardStakeInputEpochPrefix(epoch)
	k = append(k, pool...)
	k = append(k, credTag)
	return append(k, cred...), nil
}

// DecodeRewardStakeInputKey splits a stake row key.
func DecodeRewardStakeInputKey(
	k []byte,
) (epoch uint64, pool []byte, credTag uint8, cred []byte, err error) {
	if len(k) != 1+8+RewardHashLen+1+RewardHashLen ||
		k[0] != RewardStakeInputTag {
		return 0, nil, 0, nil, errors.New(
			"badgerutxo: malformed reward stake input key",
		)
	}
	return binary.BigEndian.Uint64(k[1:9]), k[9 : 9+RewardHashLen],
		k[9+RewardHashLen], k[10+RewardHashLen:], nil
}

// RewardPoolInputKey keys one pool's inputs in epoch.
func RewardPoolInputKey(epoch uint64, pool []byte) ([]byte, error) {
	if len(pool) != RewardHashLen {
		return nil, fmt.Errorf(
			"badgerutxo: reward pool input key needs a %d-byte pool hash, "+
				"got %d", RewardHashLen, len(pool),
		)
	}
	return append(RewardPoolInputEpochPrefix(epoch), pool...), nil
}

// DecodeRewardPoolInputKey splits a pool row key.
func DecodeRewardPoolInputKey(k []byte) (uint64, []byte, error) {
	if len(k) != 1+8+RewardHashLen || k[0] != RewardPoolInputTag {
		return 0, nil, errors.New("badgerutxo: malformed reward pool input key")
	}
	return binary.BigEndian.Uint64(k[1:9]), k[9:], nil
}

// RewardSnapshotKey keys the snapshot header or failure of epoch and type.
func RewardSnapshotKey(epoch uint64, kind byte, typ string) []byte {
	return append(append(RewardSnapshotEpochPrefix(epoch), kind), typ...)
}

// RewardStakePendingKey keys the marker that the stake rows of epoch are
// incomplete.
func RewardStakePendingKey(epoch uint64) []byte {
	return RewardSnapshotKey(epoch, RewardSnapshotPending, "")
}

// DecodeRewardSnapshotKey splits an S row key.
func DecodeRewardSnapshotKey(
	k []byte,
) (epoch uint64, kind byte, typ string, err error) {
	if len(k) < 10 || k[0] != RewardSnapshotTag {
		return 0, 0, "", errors.New("badgerutxo: malformed reward snapshot key")
	}
	return binary.BigEndian.Uint64(k[1:9]), k[9], string(k[10:]), nil
}

// RewardPotsKey keys the ADA pots of epoch.
func RewardPotsKey(epoch uint64) []byte {
	return rewardEpochPrefix(RewardPotsTag, epoch)
}

// DecodeRewardPotsKey returns the epoch of a pots row key.
func DecodeRewardPotsKey(k []byte) (uint64, error) {
	if len(k) != 9 || k[0] != RewardPotsTag {
		return 0, errors.New("badgerutxo: malformed reward pots key")
	}
	return binary.BigEndian.Uint64(k[1:]), nil
}

// Reward likelihood rows. The states sit apart from the pools, so a read of
// the states does not scan every pool row of every epoch:
//
//	2 's' <epoch>          the state a reward update installed for epoch
//	2 'p' <epoch> <pool>   one pool's likelihood in that state
const (
	rewardLikelihoodStateKind byte = 0x73
	rewardLikelihoodPoolKind  byte = 0x70
)

// RewardLikelihoodStatePrefix is the prefix of every likelihood state row.
func RewardLikelihoodStatePrefix() []byte {
	return []byte{RewardLikelihoodTag, rewardLikelihoodStateKind}
}

// RewardLikelihoodStateKey keys the likelihood state of epoch.
func RewardLikelihoodStateKey(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64(RewardLikelihoodStatePrefix(), epoch)
}

// RewardLikelihoodPoolPrefix is the prefix of every pool likelihood row of
// epoch.
func RewardLikelihoodPoolPrefix(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64(
		[]byte{RewardLikelihoodTag, rewardLikelihoodPoolKind}, epoch,
	)
}

// RewardLikelihoodPoolKey keys pool's likelihood in the state of epoch. It
// fails when pool is not RewardHashLen bytes.
func RewardLikelihoodPoolKey(epoch uint64, pool []byte) ([]byte, error) {
	if len(pool) != RewardHashLen {
		return nil, fmt.Errorf(
			"badgerutxo: reward likelihood key needs a %d-byte pool hash, "+
				"got %d", RewardHashLen, len(pool),
		)
	}
	return append(RewardLikelihoodPoolPrefix(epoch), pool...), nil
}

// DecodeRewardLikelihoodKey splits a likelihood row key: the epoch, and the
// pool hash, nil for a state row.
func DecodeRewardLikelihoodKey(k []byte) (uint64, []byte, error) {
	if len(k) < 10 || k[0] != RewardLikelihoodTag {
		return 0, nil, errors.New("badgerutxo: malformed reward likelihood key")
	}
	epoch := binary.BigEndian.Uint64(k[2:10])
	switch {
	case k[1] == rewardLikelihoodStateKind && len(k) == 10:
		return epoch, nil, nil
	case k[1] == rewardLikelihoodPoolKind && len(k) == 10+RewardHashLen:
		return epoch, k[10:], nil
	}
	return 0, nil, errors.New("badgerutxo: malformed reward likelihood key")
}

// LoadRewardInputs replaces the store's reward input rows with the ones feed
// supplies, each as a primary key and value, and marks them loaded, for a
// store from before it kept them.
func (s *Store) LoadRewardInputs(
	feed func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error,
) error {
	tags := []byte{
		RewardStakeInputTag, RewardPoolInputTag, RewardSnapshotTag,
		RewardPotsTag,
	}
	prefixes := make([][]byte, 0, len(tags))
	for _, tag := range tags {
		prefixes = append(prefixes, []byte{tag})
	}
	if err := s.dropPrefix(prefixes...); err != nil {
		return fmt.Errorf("badgerutxo: drop reward inputs: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || !slices.Contains(tags, k[0]) {
			return fmt.Errorf("badgerutxo: %x is not a reward input key", k)
		}
		return wb.Set(k, v)
	}, wb.tombstoneFor(tags))
	if err != nil {
		return fmt.Errorf("badgerutxo: load reward inputs: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load reward inputs: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(rewardInputsKey, []byte{1})
	})
}

// RewardInputsLoaded reports whether the store holds the reward input rows up
// to its tip.
func (s *Store) RewardInputsLoaded() (bool, error) {
	return s.hasMarker(rewardInputsKey)
}
