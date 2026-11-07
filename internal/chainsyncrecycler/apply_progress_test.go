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

package chainsyncrecycler

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

func TestTickDefersPlateauWhileBlockProcessingAdvances(t *testing.T) {
	t.Parallel()
	conn := testConnId(3)
	active := conn
	ledger := &fakeLedger{
		tip: testTip(100, 50), primaryChainTipSlot: 500,
	}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(conn, 100),
		}, activeConn: &active,
	}
	selector := plateauSelector(conn, 1_700_000)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})
	now := time.Unix(1_700_000_000, 0)
	st := newTestTickState(100, now)
	st.lastPrimaryChainTipSlot = 500
	live := LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}
	for step := 1; step <= 5; step++ {
		now = now.Add(90 * time.Second)
		ledger.blockProcessingSlot = 100 + uint64(step)
		r.tick(now, st, live, 100)
		require.Empty(
			t,
			pub.byType(event.ChainsyncResyncEventType),
			"advancing local work must protect deep catch-up despite unchanged published tips",
		)
		require.Equal(t, now, st.lastProgressAt)
	}
	// The worker may still exist but a hung fetch, commit or repeated failed
	// prefix supplies no new high-water slot. The normal deadline must still fire.
	now = now.Add(4 * time.Minute)
	r.tick(now, st, live, 100)
	require.Empty(t, pub.byType(event.ChainsyncResyncEventType))
	r.tick(now.Add(time.Nanosecond), st, live, 100)
	require.Len(t, pub.byType(event.ChainsyncResyncEventType), 1)
}

func TestTickResetsBlockProcessingBaselineAfterLedgerReplacement(t *testing.T) {
	t.Parallel()
	conn := testConnId(4)
	active := conn
	old := &fakeLedger{
		tip:                 testTip(100, 50),
		primaryChainTipSlot: 500,
		blockProcessingSlot: 10000,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(conn, 100)},
		activeConn: &active,
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(
		t,
		old,
		state,
		plateauSelector(conn, 1700000),
		pub,
		Config{},
	)
	now := time.Unix(1700000000, 0)
	st := newTestTickState(100, now)
	st.lastPrimaryChainTipSlot = 500
	live := LiveComponents{
		Ledger:         old,
		ChainsyncState: state,
		ChainSelector:  plateauSelector(conn, 1700000),
	}
	r.tick(now, st, live, 100)
	replacement := &fakeLedger{
		tip:                 old.tip,
		primaryChainTipSlot: 500,
		blockProcessingSlot: 1,
	}
	live.Ledger = replacement
	for step := 1; step <= 5; step++ {
		now = now.Add(90 * time.Second)
		replacement.blockProcessingSlot = uint64(step)
		r.tick(now, st, live, 100)
		require.Equal(
			t,
			now,
			st.lastProgressAt,
			"a rebuilt ledger must not inherit the previous ledger's processing high-water slot",
		)
		require.Empty(t, pub.byType(event.ChainsyncResyncEventType))
	}
	now = now.Add(4 * time.Minute)
	r.tick(now, st, live, 100)
	require.Empty(t, pub.byType(event.ChainsyncResyncEventType))
	r.tick(now.Add(time.Nanosecond), st, live, 100)
	require.Len(t, pub.byType(event.ChainsyncResyncEventType), 1)
}

func TestTickRecognizesFirstSlotZeroBlockOnlyOnce(t *testing.T) {
	t.Parallel()
	conn := testConnId(5)
	active := conn
	ledger := &fakeLedger{tip: testTip(0, 0)}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(conn, 0)},
		activeConn: &active,
	}
	pub := newFakePublisher()
	selector := plateauSelector(conn, 1700000)
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})
	now := time.Unix(1700000000, 0)
	st := newTestTickState(0, now)
	live := LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}
	r.tick(now, st, live, 0)
	now = now.Add(90 * time.Second)
	ledger.blockProcessingSeen = true
	r.tick(now, st, live, 0)
	require.Equal(
		t,
		now,
		st.lastProgressAt,
		"the first completed slot-zero block must renew progress",
	)
	require.Empty(t, pub.byType(event.ChainsyncResyncEventType))
	r.tick(now.Add(4*time.Minute), st, live, 0)
	require.Equal(
		t,
		now,
		st.lastProgressAt,
		"repeating slot zero cannot renew progress",
	)
	require.Empty(t, pub.byType(event.ChainsyncResyncEventType))
	r.tick(now.Add(4*time.Minute+time.Nanosecond), st, live, 0)
	require.Len(t, pub.byType(event.ChainsyncResyncEventType), 1)
}
