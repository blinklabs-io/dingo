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

package ledger

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type appliedAdmissionFixture struct {
	*chainsyncRollbackFixture
	pending     pendingPublishes
	update      *chainselection.PeerTipUpdateEvent
	admitted    chan bool
	invalidated atomic.Int32
	child       mockHeader
}

func newAppliedAdmissionFixture(t *testing.T) *appliedAdmissionFixture {
	t.Helper()
	f := &appliedAdmissionFixture{
		chainsyncRollbackFixture: newChainsyncRollbackFixture(t),
		admitted:                 make(chan bool, 1),
	}
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(f.currentTip.Point.Hash),
		prevHash:    lcommon.NewBlake2b256(f.ancestorTip.Point.Hash),
		blockNumber: f.currentTip.BlockNumber,
		slot:        f.currentTip.Point.Slot,
	}
	generation := f.ls.CurrentHeaderCryptoVerificationGeneration()
	require.NotZero(t, generation)
	f.update = &chainselection.PeerTipUpdateEvent{
		ConnectionId:                       f.connId,
		AdmissionID:                        1,
		ObservedTip:                        f.currentTip,
		Tip:                                f.currentTip,
		HeaderCryptoVerificationGeneration: generation,
	}
	require.NoError(
		t,
		f.ls.handleEventChainsyncBlockHeaderWithPending(ChainsyncEvent{
			ConnectionId:                       f.connId,
			BlockHeader:                        header,
			Point:                              f.currentTip.Point,
			Tip:                                f.currentTip,
			HeaderCryptoVerificationGeneration: generation,
			PeerTipUpdate:                      f.update,
			PeerTipAdmission:                   func(ok bool) { f.admitted <- ok },
			InvalidatePeerHeaderHistory:        func() { f.invalidated.Add(1) },
		}, &f.pending),
	)
	require.Empty(
		t,
		f.admitted,
		"admission must remain unresolved until the pending callback runs",
	)
	f.child = mockHeader{
		hash: lcommon.NewBlake2b256(
			testHashBytes("post-admission-child"),
		),
		prevHash:    header.hash,
		blockNumber: header.blockNumber + 1,
		slot:        header.slot + 10,
	}
	require.NoError(
		t,
		f.ls.chain.AddBlockHeaderWithCryptoVerificationGeneration(
			context.Background(), f.child, generation,
		),
	)
	require.Equal(t, 1, f.ls.chain.HeaderCount())
	return f
}

func requireAppliedAdmissionRecovered(
	t *testing.T,
	f *appliedAdmissionFixture,
) {
	t.Helper()
	admitted := testutil.RequireReceive(t, f.admitted, testutil.AsyncWait,
		"the deferred admission callback must resolve")
	require.True(
		t,
		admitted,
		"an already-applied header must keep its admission across a local publication",
	)
	require.Zero(
		t,
		f.invalidated.Load(),
		"an applied header must not invalidate a newer peer suffix",
	)
	require.Equal(t, f.ls.CurrentHeaderCryptoVerificationGeneration(),
		f.update.HeaderCryptoVerificationGeneration,
		"selector admission must receive the fresh pinned generation")
	require.Equal(
		t,
		1,
		f.ls.chain.HeaderCount(),
		"a late callback must not discard newer queued work",
	)
	require.True(t, f.ls.chain.FirstHeaderMatchesPoint(
		ocommon.NewPoint(f.child.slot, f.child.hash.Bytes()),
	))
	require.False(t, f.ls.queuedHeaderCryptoVerdictCurrent(
		ocommon.NewPoint(f.child.slot, f.child.hash.Bytes()),
	), "preserving the child must not refresh its old verification verdict")
}

func TestAppliedHeaderAdmissionSurvivesPublicationBeforeCallback(t *testing.T) {
	t.Parallel()
	f := newAppliedAdmissionFixture(t)
	f.ls.publishSnapshotsLocked()
	f.pending.flush()
	requireAppliedAdmissionRecovered(t, f)
}

func TestAppliedHeaderAdmissionWaitsForValidationTransition(t *testing.T) {
	t.Parallel()
	f := newAppliedAdmissionFixture(t)
	finish := f.ls.beginTxValidationTransition()
	entered := make(chan struct{})
	f.pending.afterUnlock = append(
		[]func(){func() { close(entered) }},
		f.pending.afterUnlock...)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.pending.flush()
	}()
	t.Cleanup(func() {
		finish()
		testutil.RequireReceive(
			t,
			done,
			testutil.AsyncWait,
			"admission callback cleanup",
		)
	})
	testutil.RequireReceive(
		t,
		entered,
		testutil.AsyncWait,
		"pending admission callback entry",
	)
	testutil.RequireNoReceive(t, f.admitted, 100*time.Millisecond,
		"a local transition must settle before the callback resolves admission")
	f.ls.Lock()
	f.ls.publishSnapshotsLocked()
	f.ls.Unlock()
	finish()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"settled admission callback",
	)
	requireAppliedAdmissionRecovered(t, f)
}

func TestAppliedHeaderAdmissionRejectsReplacedCanonicalPoint(t *testing.T) {
	t.Parallel()
	f := newAppliedAdmissionFixture(t)
	require.NoError(
		t,
		f.ls.chain.Rollback(context.Background(), f.ancestorTip.Point),
	)
	require.NoError(
		t,
		f.ls.chain.AddRawBlocks(context.Background(), []chain.RawBlock{{
			Slot: f.currentTip.Point.Slot, BlockNumber: f.currentTip.BlockNumber,
			Hash: testHashBytes(
				"replacement-canonical-block",
			), PrevHash: f.ancestorTip.Point.Hash,
			Type: 1, Cbor: []byte{0x80},
		}}),
	)
	f.ls.currentTip = f.ls.chain.Tip()
	f.ls.publishSnapshotsLocked()
	f.pending.flush()
	admitted := testutil.RequireReceive(t, f.admitted, testutil.AsyncWait,
		"the rejected deferred admission callback must resolve")
	require.False(
		t,
		admitted,
		"a replaced block hash must not regain admission from an equal-height tip",
	)
	require.EqualValues(
		t,
		1,
		f.invalidated.Load(),
		"replaced ancestry must retain strict invalidation",
	)
}

func TestAppliedHeaderAdmissionTransitionWaitCancels(t *testing.T) {
	t.Parallel()
	f := newAppliedAdmissionFixture(t)
	finish := f.ls.beginTxValidationTransition()
	t.Cleanup(finish)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	header := mockHeader{
		hash: lcommon.NewBlake2b256(
			f.currentTip.Point.Hash,
		), slot: f.currentTip.Point.Slot,
	}
	release, ok := f.ls.pinSettledHeaderAdmission(ctx, &ChainsyncEvent{
		BlockHeader: header, Point: f.currentTip.Point, PeerTipUpdate: f.update,
	}, f.update.HeaderCryptoVerificationGeneration)
	if release != nil {
		release()
	}
	require.False(
		t,
		ok,
		"cancellation must not admit a header during an active transition",
	)
}

func TestAppliedHeaderAdmissionDoesNotRefreshQueuedHeader(t *testing.T) {
	t.Parallel()
	f := newAppliedAdmissionFixture(t)
	generation := f.update.HeaderCryptoVerificationGeneration
	f.ls.publishSnapshotsLocked()
	update := *f.update
	update.ObservedTip.Point = ocommon.NewPoint(
		f.child.slot,
		f.child.hash.Bytes(),
	)
	update.ObservedTip.BlockNumber = f.child.blockNumber
	update.Tip = update.ObservedTip
	e := ChainsyncEvent{
		BlockHeader: f.child, Point: update.ObservedTip.Point,
		HeaderCryptoVerificationGeneration: generation, PeerTipUpdate: &update,
	}
	release, ok := f.ls.pinSettledHeaderAdmission(
		context.Background(),
		&e,
		generation,
	)
	if release != nil {
		release()
	}
	require.False(t, ok, "a queued header is not proof of ledger application")
	require.Equal(t, generation, e.HeaderCryptoVerificationGeneration)
	require.Equal(t, generation, update.HeaderCryptoVerificationGeneration,
		"an unapplied point must not refresh the selector's verdict")
}

func TestAppliedHeaderAdmissionBindsEventPointToHeader(t *testing.T) {
	t.Parallel()
	f := newAppliedAdmissionFixture(t)
	generation := f.update.HeaderCryptoVerificationGeneration
	f.ls.publishSnapshotsLocked()
	e := ChainsyncEvent{
		BlockHeader: mockHeader{
			hash: lcommon.NewBlake2b256(
				f.currentTip.Point.Hash,
			), slot: f.currentTip.Point.Slot,
		},
		Point: ocommon.NewPoint(
			f.currentTip.Point.Slot,
			testHashBytes("unbound-admission-point"),
		),
		HeaderCryptoVerificationGeneration: generation, PeerTipUpdate: f.update,
	}
	release, ok := f.ls.pinSettledHeaderAdmission(
		context.Background(),
		&e,
		generation,
	)
	if release != nil {
		release()
	}
	require.False(
		t,
		ok,
		"an applied header must not authorize a different event point",
	)
	require.Equal(t, generation, f.update.HeaderCryptoVerificationGeneration)
}

func TestAppliedHeaderAdmissionTransitionWaitStopsOnClose(t *testing.T) {
	t.Parallel()
	f := newAppliedAdmissionFixture(t)
	finish := f.ls.beginTxValidationTransition()
	entered := make(chan struct{})
	f.pending.afterUnlock = append(
		[]func(){func() { close(entered) }},
		f.pending.afterUnlock...)
	f.ls.replayWG.Add(1)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		defer f.ls.replayWG.Done()
		f.pending.flush()
	}()
	closeDone := make(chan error, 1)
	closeStarted := false
	t.Cleanup(func() {
		finish()
		testutil.RequireReceive(
			t,
			workerDone,
			testutil.AsyncWait,
			"admission shutdown worker cleanup",
		)
		if closeStarted {
			require.NoError(
				t,
				testutil.RequireReceive(
					t,
					closeDone,
					testutil.AsyncWait,
					"ledger close cleanup",
				),
			)
		}
	})
	testutil.RequireReceive(
		t,
		entered,
		testutil.AsyncWait,
		"pending admission callback entry",
	)
	testutil.RequireNoReceive(t, f.admitted, 100*time.Millisecond,
		"the active transition must keep admission pending before shutdown")
	closeStarted = true
	go func() { closeDone <- f.ls.Close() }()
	// Shutdown must cancel the wait rather than consume its 30-second busy budget.
	testutil.RequireReceive(
		t,
		workerDone,
		5*time.Second,
		"ledger shutdown must release admission without waiting for the transition",
	)
	admitted := testutil.RequireReceive(t, f.admitted, testutil.AsyncWait,
		"shutdown must resolve deferred admission")
	require.False(
		t,
		admitted,
		"shutdown must not authorize an unsettled header",
	)
}

func TestAppliedHeaderAdmissionMembershipHonorsCancellation(t *testing.T) {
	t.Parallel()
	f := newAppliedAdmissionFixture(t)
	f.ls.publishSnapshotsLocked()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.ls.ctx = ctx
	f.pending.flush()
	admitted := testutil.RequireReceive(t, f.admitted, testutil.AsyncWait,
		"cancelled membership read must resolve deferred admission")
	require.False(t, admitted,
		"a cancelled membership read must not restore expired admission")
	require.EqualValues(t, 1, f.invalidated.Load(),
		"cancelled recovery must retain strict rejection")
}

type admissionMembershipMutationStore struct {
	blob.BlobStore
	mutated atomic.Bool
	reads   atomic.Int32
	mutate  func()
}

func (s *admissionMembershipMutationStore) Get(txn dbtypes.Txn, key []byte) ([]byte, error) {
	value, err := s.BlobStore.Get(txn, key)
	s.reads.Add(1)
	if s.mutated.CompareAndSwap(false, true) {
		s.mutate()
	}
	return value, err
}

func TestAppliedHeaderAdmissionRetriesUnsettledMembership(t *testing.T) {
	t.Parallel()
	f := newChainsyncRollbackFixture(t)
	old := f.ls.CurrentHeaderCryptoVerificationGeneration()
	f.ls.publishSnapshotsLocked()
	current := f.ls.CurrentHeaderCryptoVerificationGeneration()
	base := f.ls.db.Blob()
	store := &admissionMembershipMutationStore{BlobStore: base, mutate: func() {
		require.NoError(t, f.ls.chain.AddRawBlocks(t.Context(), []chain.RawBlock{{Slot: 30, BlockNumber: 3, Hash: testHashBytes("membership-race-child"), PrevHash: f.currentTip.Point.Hash, Type: 1, Cbor: []byte{0x80}}}))
	}}
	_, drain := f.ls.db.SetBlobStore(store)
	drain()
	t.Cleanup(func() { _, drain := f.ls.db.SetBlobStore(base); drain() })
	e := ChainsyncEvent{Point: f.currentTip.Point, BlockHeader: mockHeader{hash: lcommon.NewBlake2b256(f.currentTip.Point.Hash), slot: f.currentTip.Point.Slot, blockNumber: f.currentTip.BlockNumber}}
	release, ok := f.ls.pinSettledHeaderAdmission(t.Context(), &e, old)
	if release != nil {
		defer release()
	}
	require.True(t, store.mutated.Load(), "the membership read must overlap a real chain mutation")
	require.Equal(t, current, f.ls.CurrentHeaderCryptoVerificationGeneration(), "stored block progress must not publish ledger state in this control")
	require.True(t, ok, "unknown membership during an append must be read again before rejecting admission")
	require.Equal(t, current, e.HeaderCryptoVerificationGeneration)
	require.GreaterOrEqual(t, store.reads.Load(), int32(4), "both membership reads must reach persistent indexes")
}
