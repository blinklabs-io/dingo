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
	for _, role := range []string{database.RoleIndexer, ""} {
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     t.TempDir(),
			StorageMode: types.StorageModeAPI,
			Role:        role,
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
		if role == database.RoleIndexer {
			require.Equal(t, want.Slot, got.Point.Slot)
			require.Equal(t, want.Hash, got.Point.Hash)
			require.Equal(t, want.BlockNumber, got.BlockNumber)
		} else {
			// The ledger's tip row is not the IndexerDB's to write.
			require.Zero(t, got.BlockNumber)
		}
		indexed, err := s.Tip(nil)
		require.NoError(t, err)
		require.Equal(t, want, indexed)
	}
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
		name                  string
		kind                  Kind
		indexed, ledger, anch uint64
		want                  bool
	}{
		{"below anchor current", KindCurrent, 99, 100, 100, false},
		{"below anchor history", KindHistory, 99, 100, 100, false},
		{"at anchor history far behind", KindHistory, 100, 10_000, 100, true},
		{"at anchor current far behind", KindCurrent, 100, 10_000, 100, false},
		{"lag exactly max", KindCurrent, 900, 1000, 0, true},
		{"lag one past max", KindCurrent, 899, 1000, 0, false},
		{"indexer ahead of ledger", KindCurrent, 1001, 1000, 0, true},
		{"no anchor caught up", KindCurrent, 5, 5, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := NewStore(nil, fakeState{
				TipSyncKey: Tip{BlockNumber: tt.indexed}.encode(),
			}, maxLag)
			require.NoError(t, err)
			got, err := s.Ready(tt.kind, tt.ledger, tt.anch)
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

func TestAnchorAndBootstrapPhaseRoundTrip(t *testing.T) {
	t.Parallel()
	state := fakeState{}
	s, err := NewStore(nil, state, 10)
	require.NoError(t, err)
	txn := &database.Txn{}

	anchor, err := s.Anchor(nil)
	require.NoError(t, err)
	require.True(t, anchor.IsZero(), "a chain from genesis has no anchor")
	phase, err := s.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, PhaseNone, phase)

	want := Tip{Slot: 90, BlockNumber: 9, Hash: []byte{0xaa, 0xbb}}
	require.NoError(t, s.SetAnchor(want, txn))
	require.NoError(t, s.SetBootstrap(PhaseIndex, txn))
	got, err := s.Anchor(nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
	phase, err = s.Bootstrap(nil)
	require.NoError(t, err)
	require.Equal(t, PhaseIndex, phase)

	require.Error(
		t,
		s.SetAnchor(want, nil),
		"the anchor is written with the writer's transaction",
	)
	require.Error(t, s.SetBootstrap(PhaseDone, nil))
	state[BootstrapSyncKey] = "sideways"
	_, err = s.Bootstrap(nil)
	require.Error(t, err, "an unknown phase is refused, not read as done")
}

func TestReplayTransactionsAccumulate(t *testing.T) {
	t.Parallel()
	s, err := NewStore(nil, fakeState{}, 10)
	require.NoError(t, err)
	txn := &database.Txn{}
	require.NoError(t, s.AddReplayTransactions(3, txn))
	require.NoError(t, s.AddReplayTransactions(4, txn))
	n, err := s.ReplayTransactions(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(7), n)
}

func TestReadyServingWaitsForTheAnchorAndTheBootstrap(t *testing.T) {
	t.Parallel()
	const maxLag = 100
	anchor := Tip{Slot: 90, BlockNumber: 100, Hash: []byte{1}}
	for _, tt := range []struct {
		name    string
		state   fakeState
		indexed uint64
		want    bool
	}{
		{"no anchor", fakeState{}, 5, true},
		{"replaying", fakeState{
			AnchorSyncKey: anchor.encode(), BootstrapSyncKey: string(PhaseReplay),
		}, 100, false},
		{"anchor reached, building indexes", fakeState{
			AnchorSyncKey: anchor.encode(), BootstrapSyncKey: string(PhaseIndex),
		}, 100, false},
		{"done at the anchor", fakeState{
			AnchorSyncKey: anchor.encode(), BootstrapSyncKey: string(PhaseDone),
		}, 100, true},
		{"done but below the anchor", fakeState{
			AnchorSyncKey: anchor.encode(), BootstrapSyncKey: string(PhaseDone),
		}, 99, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.state[TipSyncKey] = Tip{BlockNumber: tt.indexed}.encode()
			s, err := NewStore(nil, tt.state, maxLag)
			require.NoError(t, err)
			got, err := s.ReadyServing(KindHistory, 100)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// listCountingState counts checkpoint listings.
type listCountingState struct {
	fakeState
	lists int
}

func (l *listCountingState) ListSyncStateKeysByPrefix(
	prefix string, txn *database.Txn,
) ([]string, error) {
	l.lists++
	return l.fakeState.ListSyncStateKeysByPrefix(prefix, txn)
}

func TestAddCheckpointListsCheckpointsOnlyWhenPruning(t *testing.T) {
	t.Parallel()
	state := &listCountingState{fakeState: fakeState{}}
	s, err := NewStore(nil, state, 10)
	require.NoError(t, err)
	txn := &database.Txn{}
	tipAt := func(n uint64) Tip {
		return Tip{Slot: n * 10, BlockNumber: n, Hash: []byte{byte(n)}}
	}
	for n := uint64(1); n <= 200; n++ {
		require.NoError(t, s.AddCheckpoint(tipAt(n), txn))
	}
	require.LessOrEqual(t, state.lists, 1,
		"batches inside the retention window do not list checkpoints")

	require.NoError(t, s.AddCheckpoint(tipAt(CheckpointRetainBlocks+50), txn))
	cps, err := s.Checkpoints(txn)
	require.NoError(t, err)
	for _, cp := range cps {
		require.GreaterOrEqual(t, cp.BlockNumber, uint64(50),
			"blocks out of retention are pruned")
	}
	require.Equal(t, uint64(CheckpointRetainBlocks+50), cps[0].BlockNumber)
}
