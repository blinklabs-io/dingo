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

package indexerdb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

type fakeState map[string]string

func (f fakeState) GetSyncState(k string, _ *database.Txn) (string, error) {
	return f[k], nil
}

func (f fakeState) SetSyncState(k, v string, _ *database.Txn) error {
	f[k] = v
	return nil
}

func (f fakeState) DeleteSyncState(k string, _ *database.Txn) error {
	delete(f, k)
	return nil
}

func (f fakeState) ListSyncStateKeysByPrefix(
	prefix string, _ *database.Txn,
) ([]string, error) {
	var out []string
	for k := range f {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func TestTipRoundTrip(t *testing.T) {
	t.Parallel()
	state := fakeState{}
	s, err := NewStore(nil, state, 10)
	require.NoError(t, err)
	got, err := s.Tip(nil)
	require.NoError(t, err)
	require.True(t, got.IsZero())

	want := Tip{
		Slot:        4242,
		BlockNumber: 77,
		Hash:        []byte{0xde, 0xad, 0xbe, 0xef},
	}
	require.NoError(t, s.SetTip(want, &database.Txn{}))
	got, err = s.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestSetTipWritesTheIndexerMetadataTipRow(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeAPI,
		Role:        database.RoleIndexer,
	})
	require.NoError(t, err)
	s, err := NewStore(db, db, 10)
	require.NoError(t, err)
	want := Tip{
		Slot:        4242,
		BlockNumber: 77,
		Hash:        bytes.Repeat([]byte{0xde}, 32),
	}
	txn := db.Transaction(t.Context(), true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return s.SetTip(want, txn)
	}))
	got, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, want.Slot, got.Point.Slot)
	require.Equal(t, want.Hash, got.Point.Hash)
	require.Equal(t, want.BlockNumber, got.BlockNumber)
	indexed, err := s.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, want, indexed)
}

func TestSetTipRefusesNoTransaction(t *testing.T) {
	t.Parallel()
	s, err := NewStore(nil, fakeState{}, 10)
	require.NoError(t, err)
	require.Error(t, s.SetTip(Tip{Slot: 1}, nil))
}

func TestTipMalformed(t *testing.T) {
	t.Parallel()
	for _, v := range []string{"1:2", "x:2:aa", "1:y:aa", "1:2:zz"} {
		s, err := NewStore(nil, fakeState{TipSyncKey: v}, 10)
		require.NoError(t, err)
		_, err = s.Tip(nil)
		require.Error(t, err, v)
	}
}

func TestReadyBoundaries(t *testing.T) {
	t.Parallel()
	const maxLag = 100
	tests := []struct {
		name            string
		kind            Kind
		indexed, ledger uint64
		want            bool
	}{
		{"history far behind", KindHistory, 100, 10_000, true},
		{"current far behind", KindCurrent, 100, 10_000, false},
		{"lag exactly max", KindCurrent, 900, 1000, true},
		{"lag one past max", KindCurrent, 899, 1000, false},
		{"indexer ahead of ledger", KindCurrent, 1001, 1000, true},
		{"caught up", KindCurrent, 5, 5, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := NewStore(nil, fakeState{
				TipSyncKey: Tip{BlockNumber: tt.indexed}.encode(),
			}, maxLag)
			require.NoError(t, err)
			got, err := s.Ready(tt.kind, tt.ledger)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCheckpointsAreNewestFirstPrunedAndDroppedOnRollback(t *testing.T) {
	t.Parallel()
	s, err := NewStore(nil, fakeState{}, 10)
	require.NoError(t, err)
	txn := &database.Txn{}
	tipAt := func(n uint64) Tip {
		return Tip{Slot: n * 10, BlockNumber: n, Hash: []byte{byte(n)}}
	}
	for _, n := range []uint64{5, 700, 9} {
		require.NoError(t, s.AddCheckpoint(tipAt(n), txn))
	}
	cps, err := s.Checkpoints(txn)
	require.NoError(t, err)
	require.Equal(t, []uint64{700, 9, 5}, []uint64{
		cps[0].BlockNumber, cps[1].BlockNumber, cps[2].BlockNumber,
	}, "newest block number first, not insertion order")

	require.NoError(t, s.DropCheckpointsAbove(9, txn))
	cps, err = s.Checkpoints(txn)
	require.NoError(t, err)
	require.Len(t, cps, 2)
	require.Equal(t, uint64(9), cps[0].BlockNumber)

	// A checkpoint far enough below a new tip is pruned with it.
	require.NoError(t, s.AddCheckpoint(tipAt(CheckpointRetainBlocks+6), txn))
	cps, err = s.Checkpoints(txn)
	require.NoError(t, err)
	require.Len(t, cps, 2, "block 5 is out of retention, block 9 is not")
	require.NoError(t, s.AddCheckpoint(tipAt(CheckpointRetainBlocks+10), txn))
	cps, err = s.Checkpoints(txn)
	require.NoError(t, err)
	require.Len(t, cps, 2, "block 9 is now out of retention too")
}
