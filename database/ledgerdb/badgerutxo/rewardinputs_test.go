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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/stretchr/testify/require"
)

func TestRewardInputKeysRoundTripAndSortByEpochThenPool(t *testing.T) {
	t.Parallel()
	pool, cred := bytes.Repeat([]byte{1}, 28), bytes.Repeat([]byte{2}, 28)
	k, err := badgerutxo.RewardStakeInputKey(7, pool, 1, cred)
	require.NoError(t, err)
	e, p, tag, c, err := badgerutxo.DecodeRewardStakeInputKey(k)
	require.NoError(t, err)
	require.Equal(t, uint64(7), e)
	require.Equal(t, pool, p)
	require.Equal(t, uint8(1), tag)
	require.Equal(t, cred, c)
	require.True(
		t,
		bytes.HasPrefix(k, badgerutxo.RewardStakeInputEpochPrefix(7)),
	)

	_, err = badgerutxo.RewardStakeInputKey(7, pool[:27], 0, cred)
	require.Error(t, err)
	_, err = badgerutxo.RewardPoolInputKey(7, cred[:3])
	require.Error(t, err)

	k8, err := badgerutxo.RewardStakeInputKey(8, pool, 0, cred)
	require.NoError(t, err)
	require.Negative(t, bytes.Compare(k, k8))

	sk := badgerutxo.RewardSnapshotKey(
		9,
		badgerutxo.RewardSnapshotFailure,
		"mark",
	)
	e, kind, typ, err := badgerutxo.DecodeRewardSnapshotKey(sk)
	require.NoError(t, err)
	require.Equal(t, uint64(9), e)
	require.Equal(t, badgerutxo.RewardSnapshotFailure, kind)
	require.Equal(t, "mark", typ)
	pe, err := badgerutxo.DecodeRewardPotsKey(badgerutxo.RewardPotsKey(3))
	require.NoError(t, err)
	require.Equal(t, uint64(3), pe)
}

func TestLoadRewardInputsReplacesRowsAndSetsTheMarker(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	loaded, err := s.RewardInputsLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	require.NoError(
		t,
		s.LoadRewardInputs(
			func(put func(k, v []byte) error, _ func(uint64, []byte) error) error {
				return put(badgerutxo.RewardPotsKey(1), []byte{1})
			},
		),
	)
	require.NoError(
		t,
		s.LoadRewardInputs(
			func(put func(k, v []byte) error, _ func(uint64, []byte) error) error {
				return put(badgerutxo.RewardPotsKey(2), []byte{2})
			},
		),
	)
	_, ok, err := s.Row(badgerutxo.RewardPotsKey(1))
	require.NoError(t, err)
	require.False(t, ok, "a reload replaces")
	_, ok, err = s.Row(badgerutxo.RewardPotsKey(2))
	require.NoError(t, err)
	require.True(t, ok)
	loaded, err = s.RewardInputsLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	require.Error(
		t,
		s.LoadRewardInputs(
			func(put func(k, v []byte) error, _ func(uint64, []byte) error) error {
				return put([]byte("Zx"), nil)
			},
		),
	)
}
