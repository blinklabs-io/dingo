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

func TestRewardDerivedKeysRoundTripAndGroupByCredential(t *testing.T) {
	t.Parallel()
	pool, cred := bytes.Repeat([]byte{1}, 28), bytes.Repeat([]byte{2}, 28)
	k := badgerutxo.RewardOutputKey{
		Tag: 1, Key: cred, Epoch: 7, Pool: pool, Type: "member",
	}
	pk := badgerutxo.RewardAccountOutputKey(k)
	got, err := badgerutxo.DecodeRewardAccountOutputKey(pk)
	require.NoError(t, err)
	require.Equal(t, k, got)
	require.True(t, bytes.HasPrefix(pk, badgerutxo.RewardDerivedEpochPrefix(7)))
	require.True(
		t,
		bytes.HasPrefix(
			pk,
			badgerutxo.RewardAccountCredentialPrefix(7, 1, cred),
		),
	)
	require.False(
		t,
		bytes.HasPrefix(
			pk,
			badgerutxo.RewardAccountCredentialPrefix(7, 0, cred),
		),
	)
	e, p, err := badgerutxo.DecodeRewardPoolOutputKey(
		badgerutxo.RewardPoolOutputKey(7, pool),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(7), e)
	require.Equal(t, pool, p)
	_, err = badgerutxo.DecodeRewardAccountOutputKey(
		badgerutxo.RewardPoolOutputKey(7, pool),
	)
	require.Error(t, err)
	require.NotEqual(
		t,
		badgerutxo.RewardCursorKey(7)[0],
		badgerutxo.RewardDerivedEpochPrefix(7)[0],
	)
}

func TestLoadRewardDerivedReplacesRowsAndSetsTheMarker(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir())
	defer s.Close()
	bootstrap(t, s, point(0, 0))
	loaded, err := s.RewardDerivedLoaded()
	require.NoError(t, err)
	require.False(t, loaded)
	pool := bytes.Repeat([]byte{1}, 28)
	require.NoError(
		t,
		s.LoadRewardDerived(
			func(put func(k, v []byte) error, _ func(uint64, []byte) error) error {
				return put(badgerutxo.RewardPoolOutputKey(1, pool), []byte{1})
			},
		),
	)
	require.NoError(
		t,
		s.LoadRewardDerived(
			func(put func(k, v []byte) error, _ func(uint64, []byte) error) error {
				return put(badgerutxo.RewardCursorKey(2), []byte{2})
			},
		),
	)
	_, ok, err := s.Row(badgerutxo.RewardPoolOutputKey(1, pool))
	require.NoError(t, err)
	require.False(t, ok, "a reload replaces")
	_, ok, err = s.Row(badgerutxo.RewardCursorKey(2))
	require.NoError(t, err)
	require.True(t, ok)
	loaded, err = s.RewardDerivedLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	require.Error(
		t,
		s.LoadRewardDerived(
			func(put func(k, v []byte) error, _ func(uint64, []byte) error) error {
				return put([]byte("Ox"), nil)
			},
		),
	)
}
