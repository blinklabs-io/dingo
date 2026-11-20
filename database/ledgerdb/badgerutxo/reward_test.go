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

package badgerutxo_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

func rewardRef(epoch uint64, key byte, typ string) badgerutxo.RewardOutputKey {
	return badgerutxo.RewardOutputKey{
		Tag: 0, Key: bytes.Repeat([]byte{key}, 28), Epoch: epoch,
		Pool: bytes.Repeat([]byte{0xd1}, 28), Type: typ,
	}
}

func rewardPuts(slot uint64, refs ...badgerutxo.RewardOutputKey) []badgerutxo.RowPut {
	var out []badgerutxo.RowPut
	for _, r := range refs {
		out = append(out,
			badgerutxo.RowPut{
				Slot: slot, Key: badgerutxo.RewardOutputRowKey(r),
				Value: badgerutxo.RewardOutputValue(5, 1, 2),
			},
			badgerutxo.RowPut{Slot: slot, Key: badgerutxo.RewardEpochRowKey(r)},
		)
	}
	return out
}

// An output's primary and epoch index keys decode to the output they were
// built from, and a credential's prefix selects exactly its own rows even
// beside a credential whose key extends it.
func TestRewardKeysRoundTripAndSeparateCredentials(t *testing.T) {
	t.Parallel()
	ref := rewardRef(7, 0x11, "member")
	got, err := badgerutxo.DecodeRewardOutputRowKey(badgerutxo.RewardOutputRowKey(ref))
	require.NoError(t, err)
	require.Equal(t, ref, got)
	got, err = badgerutxo.DecodeRewardEpochRowKey(badgerutxo.RewardEpochRowKey(ref))
	require.NoError(t, err)
	require.Equal(t, ref, got)

	longer := ref
	longer.Key = append(bytes.Clone(ref.Key), 0x99)
	prefix := badgerutxo.RewardCredentialPrefix(ref.Tag, ref.Key)
	require.True(t, bytes.HasPrefix(badgerutxo.RewardOutputRowKey(ref), prefix))
	require.False(t, bytes.HasPrefix(badgerutxo.RewardOutputRowKey(longer), prefix),
		"the key length keeps one credential's rows out of another's prefix")

	slot, tag, key, kind, source, err := badgerutxo.DecodePostSnapshotCreditRowKey(
		badgerutxo.PostSnapshotCreditRowKey(42, 1, ref.Key, 1, []byte{0xa1, 0xa2}),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(42), slot)
	require.Equal(t, uint8(1), tag)
	require.Equal(t, ref.Key, key)
	require.Equal(t, uint8(1), kind)
	require.Equal(t, []byte{0xa1, 0xa2}, source)

	_, err = badgerutxo.DecodeRewardOutputRowKey([]byte{'O', 0, 28})
	require.Error(t, err)
}

// An epoch wipe removes the epoch's output and index rows in the batch's
// order and is not undone by a rollback: the metadata store's delete of an
// epoch's outputs is not.
func TestRewardEpochWipeIsNotJournaled(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	apply := func(slot uint64, rows ...badgerutxo.RowPut) {
		tip := point(slot, byte(slot))
		_, err := s.ApplyLedgerBatch(&tip, badgerutxo.LedgerBatch{Rows: rows})
		require.NoError(t, err)
	}
	apply(10, rewardPuts(10, rewardRef(3, 0x11, "member"), rewardRef(3, 0x12, "member"),
		rewardRef(9, 0x11, "member"), rewardRef(200, 0x11, "leader"))...)
	epochs, err := s.RewardEpochs()
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 9, 200}, epochs, "one entry per epoch, not per row")

	// The wipe orders with the puts that follow it in the batch.
	apply(20, append(
		[]badgerutxo.RowPut{{
			WipeRewardEpoch: true, Key: binary.BigEndian.AppendUint64(nil, 3),
		}},
		rewardPuts(20, rewardRef(3, 0x13, "member"))...,
	)...)
	epochs, err = s.RewardEpochs()
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 9, 200}, epochs)
	_, ok, err := s.Row(badgerutxo.RewardOutputRowKey(rewardRef(3, 0x11, "member")))
	require.NoError(t, err)
	require.False(t, ok, "the wipe removed the epoch's earlier rows")
	_, ok, err = s.Row(badgerutxo.RewardOutputRowKey(rewardRef(3, 0x13, "member")))
	require.NoError(t, err)
	require.True(t, ok, "and not the row written after it")
	_, ok, err = s.Row(badgerutxo.RewardEpochRowKey(rewardRef(3, 0x12, "member")))
	require.NoError(t, err)
	require.False(t, ok, "its index row went with it")

	apply(30, badgerutxo.RowPut{
		WipeRewardEpoch: true, Key: binary.BigEndian.AppendUint64(nil, 9),
	})
	_, err = s.Rollback(point(10, 0x0a))
	require.NoError(t, err)
	_, ok, err = s.Row(badgerutxo.RewardOutputRowKey(rewardRef(9, 0x11, "member")))
	require.NoError(t, err)
	require.False(t, ok, "a rollback does not bring a wiped row back")
	_, ok, err = s.Row(badgerutxo.RewardOutputRowKey(rewardRef(3, 0x13, "member")))
	require.NoError(t, err)
	require.False(t, ok, "it does remove one written after its target")
	epochs, err = s.RewardEpochs()
	require.NoError(t, err)
	require.Equal(t, []uint64{200}, epochs)
}

// The reward rows load under a marker that a bootstrap clears with the rows.
func TestLoadRewardOutputsMarksLoadedAndBootstrapClearsIt(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	bootstrap(t, s, point(1, 0x01))
	loaded, err := s.RewardOutputsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)

	ref := rewardRef(4, 0x21, "member")
	err = s.LoadRewardOutputs(func(put func(k, v []byte) error) error {
		for _, r := range rewardPuts(0, ref) {
			if err := put(r.Key, r.Value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	loaded, err = s.RewardOutputsLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	_, ok, err := s.Row(badgerutxo.RewardOutputRowKey(ref))
	require.NoError(t, err)
	require.True(t, ok)

	err = s.LoadRewardOutputs(func(put func(k, v []byte) error) error {
		return put([]byte("gnotreward"), nil)
	})
	require.Error(t, err, "a key of another family is refused")

	bootstrap(t, s, point(2, 0x02))
	loaded, err = s.RewardOutputsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	_, ok, err = s.Row(badgerutxo.RewardOutputRowKey(ref))
	require.NoError(t, err)
	require.False(t, ok)
	epochs, err := s.RewardEpochs()
	require.NoError(t, err)
	require.Empty(t, epochs)
}
