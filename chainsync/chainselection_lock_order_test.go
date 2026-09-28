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

package chainsync_test

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestChainSelectorChainsyncLockOrderDoesNotDeadlock is the regression guard
// for blinklabs-io/dingo#4070: chainselection.ChainSelector.mutex and
// chainsync.State.clientConnIdMutex are taken in opposite order on two paths
// that production (node.go) wires together via function-valued callbacks.
//
// Path A: ChainSelector.UpdatePeerTip -> updatePeerTipObservedPraosView
// (cs.mutex held for the whole call) -> comparePeerTips ->
// comparePeerTipsPraos -> the injected BlockfetchLatency callback ->
// chainsync.State.BlockfetchLatency (clientConnIdMutex.RLock).
//
// Path B: chainsync.State.RecordObservedHeader (clientConnIdMutex.RLock held
// for the whole call) -> observedHeaderLimit -> the injected
// ObservedHeaderLimitFunc callback -> ChainSelector.GenesisSelectionState
// (cs.mutex.RLock).
//
// Neither call conflicts on its own: clientConnIdMutex.RLock vs
// clientConnIdMutex.RLock never blocks. The cycle only closes because Go's
// sync.RWMutex is write-preferring -- a third party queuing
// clientConnIdMutex.Lock() (TryAddClientConnIdWithDirection /
// RemoveClientConnId in production) blocks Path A's later RLock even though
// the only active holder is a reader (Path B). This test forces that exact
// interleaving with channels, wired through the real callbacks and the real
// locks, so the deadlock is deterministic rather than timing-dependent.
func TestChainSelectorChainsyncLockOrderDoesNotDeadlock(t *testing.T) {
	bus := newTestEventBus(t)

	// cs and st forward-reference each other, mirroring the closures node.go
	// wires: ObservedHeaderLimitFunc (node.go ~1090) calls into the chain
	// selector, and BlockfetchLatency (node.go ~1159) calls into chainsync
	// state.
	var (
		cs *chainselection.ChainSelector
		st *chainsync.State
	)

	var (
		bReachedCallback = make(chan struct{})
		releaseB         = make(chan struct{})
		aReachedCallback = make(chan struct{})
		releaseA         = make(chan struct{})
		bOnce, aOnce     sync.Once
	)

	cfg := chainsync.DefaultConfig()
	cfg.ObservedHeaderLimitFunc = func() int {
		bOnce.Do(func() {
			close(bReachedCallback)
			<-releaseB
		})
		if cs == nil {
			return 0
		}
		active, window := cs.GenesisSelectionState()
		if !active {
			return 0
		}
		return int(window) //nolint:gosec // test-only, window is small
	}
	st = newTestState(t, bus, cfg)

	csCfg := chainselection.ChainSelectorConfig{
		BlockfetchLatency: func(
			connId ouroboros.ConnectionId,
		) (time.Duration, bool) {
			aOnce.Do(func() {
				close(aReachedCallback)
				<-releaseA
			})
			if st == nil {
				return 0, false
			}
			return st.BlockfetchLatency(connId)
		},
	}
	cs = chainselection.NewChainSelector(csCfg)

	connA1 := newTestConnId(1)
	connA2 := newTestConnId(2)
	connB := newTestConnId(3)
	connWriter := newTestConnId(4)

	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, []byte("tip-hash")),
		BlockNumber: 10,
	}
	require.True(t, cs.UpdatePeerTip(connA1, tip, nil))
	cs.EvaluateAndSwitch()
	best := cs.GetBestPeer()
	require.NotNil(t, best, "connA1 must be the incumbent before Path A runs")
	require.Equal(t, connA1, *best)

	require.True(t, st.AddClientConnId(connB))

	hash := []byte("observed-hash")
	prev := []byte("observed-prev")
	header := testBlockHeader{
		hash:        lcommon.NewBlake2b256(hash),
		prevHash:    lcommon.NewBlake2b256(prev),
		blockNumber: 1,
		slot:        1,
	}

	// Path B: RecordObservedHeader takes clientConnIdMutex.RLock() (plus
	// observedHeadersMutex.Lock()) and parks inside the injected
	// ObservedHeaderLimitFunc callback, still holding both.
	doneB := make(chan struct{})
	go func() {
		defer close(doneB)
		st.RecordObservedHeader(chainsync.ObservedHeader{
			ConnectionId: connB,
			BlockHeader:  header,
			Point:        ocommon.NewPoint(1, hash),
			BlockNumber:  1,
			Type:         1,
		})
	}()
	select {
	case <-bReachedCallback:
	case <-time.After(5 * time.Second):
		t.Fatal("RecordObservedHeader never reached ObservedHeaderLimitFunc")
	}

	// Path A: UpdatePeerTip for a second peer with the identical tip forces
	// comparePeerTipsPraos into its latency tiebreak. It takes cs.mutex.Lock()
	// and parks inside the injected BlockfetchLatency callback, still holding
	// it.
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		cs.UpdatePeerTip(connA2, tip, nil)
	}()
	select {
	case <-aReachedCallback:
	case <-time.After(5 * time.Second):
		t.Fatal("UpdatePeerTip never reached BlockfetchLatency")
	}

	// A third party (a new connection) queues a write lock on
	// clientConnIdMutex while Path B's read lock is still held. Go's
	// write-preferring RWMutex now blocks any later RLock behind this writer,
	// which is the mechanism that turns two never-conflicting reads into a
	// real cycle.
	doneWriter := make(chan struct{})
	go func() {
		defer close(doneWriter)
		st.AddClientConnId(connWriter)
	}()
	// Confirm the writer is genuinely parked on clientConnIdMutex before
	// releasing Path A, by reading its stack rather than by waiting out a
	// fixed interval: a wall-clock window cannot distinguish a goroutine
	// blocked on the lock from one the scheduler has not run yet, and a
	// writer that queued late would let Path A's RLock succeed and the test
	// pass even with the inversion present.
	require.Eventually(
		t,
		goroutineBlockedAddingTrackedClient,
		5*time.Second,
		time.Millisecond,
		"the writer connection add must block on clientConnIdMutex behind "+
			"Path B's held read lock for this test to exercise the "+
			"write-preference inversion",
	)
	select {
	case <-doneWriter:
		t.Fatal("the writer connection add completed instead of blocking")
	default:
	}

	// Release Path A first: its clientConnIdMutex.RLock() (inside
	// BlockfetchLatency) now queues behind the pending writer instead of
	// completing immediately, even though it does not itself conflict with
	// Path B's held read lock.
	close(releaseA)
	// Release Path B: its GenesisSelectionState call, unfixed, takes
	// cs.mutex.RLock() and blocks because Path A still holds cs.mutex for the
	// write. Fixed, GenesisSelectionState never touches cs.mutex, so Path B
	// returns immediately, releases clientConnIdMutex, the queued writer
	// proceeds, and Path A's queued read then completes.
	close(releaseB)

	allDone := make(chan struct{})
	go func() {
		<-doneB
		<-doneA
		<-doneWriter
		close(allDone)
	}()
	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal(
			"chainselection/chainsync lock order inversion deadlocked: " +
				"Path A (ChainSelector.mutex -> clientConnIdMutex) and " +
				"Path B (clientConnIdMutex -> ChainSelector.mutex) formed a " +
				"cycle once a writer queued on clientConnIdMutex (#4070)",
		)
	}
}

// goroutineBlockedAddingTrackedClient reports whether some goroutine is parked
// acquiring chainsync.State.clientConnIdMutex for writing inside the tracked
// client add path. sync.RWMutex exposes no queue-depth introspection, so a
// stack dump is the only way to positively confirm the writer is queued on the
// lock rather than merely not scheduled yet.
func goroutineBlockedAddingTrackedClient() bool {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	for stack := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(stack, "sync.(*RWMutex).Lock(") &&
			strings.Contains(stack, "AddClientConnId") {
			return true
		}
	}
	return false
}
