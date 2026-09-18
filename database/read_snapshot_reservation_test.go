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

package database

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// reservingSnapshotMetadata records the order of the reservation, begin, and
// tip steps. ReadTransaction is recorded too, so a regression that stops using
// the reservation shows up as a changed event list rather than as silence.
type reservingSnapshotMetadata struct {
	metadata.MetadataStore
	mu         sync.Mutex
	events     []string
	tip        ochainsync.Tip
	reserveErr error
}

func (s *reservingSnapshotMetadata) record(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *reservingSnapshotMetadata) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

func (s *reservingSnapshotMetadata) ReserveRead(
	context.Context,
) (types.ReadReservation, error) {
	s.record("reserve read")
	if s.reserveErr != nil {
		return nil, s.reserveErr
	}
	return &recordingReservation{owner: s}, nil
}

func (*reservingSnapshotMetadata) ReadSnapshotLimit() int { return 1 }

func (s *reservingSnapshotMetadata) ReadTransaction(
	context.Context,
) types.Txn {
	s.record("metadata transaction")
	return &commitFailingTxn{}
}

func (s *reservingSnapshotMetadata) GetTip(
	types.Txn,
) (ochainsync.Tip, error) {
	s.record("metadata tip")
	return s.tip, nil
}

// Transaction and SetCommitTimestamp exist only so the tests below can open
// and commit an ordinary read-write Txn against this store.
func (s *reservingSnapshotMetadata) Transaction(context.Context) types.Txn {
	return &commitFailingTxn{}
}

func (*reservingSnapshotMetadata) SetCommitTimestamp(
	int64,
	types.Txn,
) error {
	return nil
}

// reservingSnapshotBlob records into the same ordered event list as the
// metadata store, so one list shows the full construction order.
type reservingSnapshotBlob struct {
	blob.BlobStore
	owner *reservingSnapshotMetadata
}

func (b *reservingSnapshotBlob) NewTransaction(readWrite bool) types.Txn {
	if !readWrite {
		b.owner.record("blob transaction")
	}
	return &commitFailingTxn{}
}

func (*reservingSnapshotBlob) SetCommitTimestamp(int64, types.Txn) error {
	return nil
}

func (*reservingSnapshotBlob) Sync() error { return nil }

// recordingReservation mirrors the sqlstore reservation's contract: Release is
// a no-op once Begin has handed the connection to the transaction.
type recordingReservation struct {
	owner *reservingSnapshotMetadata
	begun bool
}

func (r *recordingReservation) Begin() types.Txn {
	r.begun = true
	r.owner.record("begin reserved")
	return &commitFailingTxn{}
}

func (r *recordingReservation) Release() {
	if r.begun {
		return
	}
	r.owner.record("release reservation")
}

func TestNewReadSnapshotContextBeginsTheReservedReadTransaction(t *testing.T) {
	t.Parallel()

	wantTip := ochainsync.Tip{BlockNumber: 42}
	store := &reservingSnapshotMetadata{tip: wantTip}
	db := &Database{
		metadata: store,
		blobRef:  newBlobStoreRef(&reservingSnapshotBlob{owner: store}),
	}

	txn, tip, err := NewReadSnapshotContext(t.Context(), db)
	require.NoError(t, err)
	t.Cleanup(txn.Release)
	require.Equal(t, wantTip, tip)
	require.Equal(
		t,
		[]string{
			"reserve read",
			"begin reserved",
			"metadata tip",
			"blob transaction",
		},
		store.recorded(),
		"a reserving store must be anchored through its reservation, not through ReadTransaction",
	)
	requireCommitBarrierFree(t, db)
	requireDestructiveTransitionBarrierFree(t, db)
}

// TestNewReadSnapshotContextReservesBeforeTakingCommitBarrier pins the
// property the reservation exists for. Beginning the read transaction is what
// waits on the metadata read pool, and that wait is unbounded while long-lived
// readers hold every connection. It must therefore complete before the commit
// barrier is acquired: the barrier's exclusive side blocks construction of
// every read-write Txn, so waiting for the pool while holding it stalls block
// application for as long as the pool stays full.
func TestNewReadSnapshotContextReservesBeforeTakingCommitBarrier(
	t *testing.T,
) {
	t.Parallel()

	store := &reservingSnapshotMetadata{}
	db := &Database{
		metadata: store,
		blobRef:  newBlobStoreRef(&reservingSnapshotBlob{owner: store}),
	}

	// An open read-write Txn holds the barrier's shared side, so the
	// snapshot's exclusive acquire cannot complete until it finishes.
	writer := NewTxnContext(t.Context(), db, true)

	resultCh := make(chan error, 1)
	go func() {
		txn, _, err := NewReadSnapshotContext(t.Context(), db)
		if txn != nil {
			txn.Release()
		}
		resultCh <- err
	}()

	testutil.WaitForCondition(
		t,
		func() bool {
			recorded := store.recorded()
			return len(recorded) > 0 && recorded[0] == "reserve read"
		},
		5*time.Second,
		"the read connection must be reserved while the commit barrier is still held by a writer",
	)
	require.Equal(
		t,
		[]string{"reserve read"},
		store.recorded(),
		"nothing past the reservation may run until the commit barrier is acquired",
	)

	require.NoError(t, writer.Commit())
	require.NoError(
		t,
		testutil.RequireReceive(
			t,
			resultCh,
			5*time.Second,
			"read snapshot must finish once the writer releases the barrier",
		),
	)
	require.Equal(
		t,
		[]string{
			"reserve read",
			"begin reserved",
			"metadata tip",
			"blob transaction",
		},
		store.recorded(),
	)
	requireCommitBarrierFree(t, db)
	requireDestructiveTransitionBarrierFree(t, db)
}

func TestNewReadSnapshotContextReleasesReservationWhenBarrierAbandoned(
	t *testing.T,
) {
	t.Parallel()

	store := &reservingSnapshotMetadata{}
	db := &Database{
		metadata: store,
		blobRef:  newBlobStoreRef(&reservingSnapshotBlob{owner: store}),
	}

	// A held destructive transition keeps the snapshot from acquiring its
	// barriers, so the cancelled ctx aborts construction after the
	// reservation has already been taken.
	finish := db.BeginDestructiveTransition()
	defer finish()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	txn, _, err := NewReadSnapshotContext(ctx, db)
	require.Nil(t, txn)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(
		t,
		[]string{"reserve read", "release reservation"},
		store.recorded(),
		"an abandoned construction must return its reserved connection to the pool",
	)
}

func TestNewReadSnapshotContextPropagatesReservationFailure(t *testing.T) {
	t.Parallel()

	reserveErr := errors.New("read pool unavailable")
	store := &reservingSnapshotMetadata{reserveErr: reserveErr}
	db := &Database{
		metadata: store,
		blobRef:  newBlobStoreRef(&reservingSnapshotBlob{owner: store}),
	}

	txn, _, err := NewReadSnapshotContext(t.Context(), db)
	require.Nil(t, txn)
	require.ErrorIs(t, err, reserveErr)
	require.ErrorContains(
		t,
		err,
		"reserve metadata read connection for read snapshot",
	)
	requireCommitBarrierFree(t, db)
	requireDestructiveTransitionBarrierFree(t, db)
}

// reservingDestructiveMetadata runs the destructive-commit straddle scenario
// through the reservation path. Begin defers to the same ReadTransaction the
// non-reserving test uses, so the metadata view is still fixed at the moment
// the snapshot opens it inside the commit barrier.
type reservingDestructiveMetadata struct {
	*destructiveReadSnapshotMetadata
}

func (m *reservingDestructiveMetadata) ReserveRead(
	ctx context.Context,
) (types.ReadReservation, error) {
	return &destructiveReservation{store: m.destructiveReadSnapshotMetadata, ctx: ctx}, nil
}

func (*reservingDestructiveMetadata) ReadSnapshotLimit() int { return 1 }

type destructiveReservation struct {
	store *destructiveReadSnapshotMetadata
	ctx   context.Context
}

func (r *destructiveReservation) Begin() types.Txn {
	return r.store.ReadTransaction(r.ctx)
}

func (*destructiveReservation) Release() {}

// TestNewReadSnapshotContextReservedPathDoesNotStraddleDestructiveCommit is
// TestNewReadSnapshotContextDoesNotStraddleDestructiveCommit for a store that
// reserves its read connection. Reserving moves only the pool wait out of the
// barrier; the commit boundary the metadata and blob views share must be
// unchanged.
func TestNewReadSnapshotContextReservedPathDoesNotStraddleDestructiveCommit(
	t *testing.T,
) {
	t.Parallel()

	destructiveCommitDone := make(chan struct{})
	var finishCommit sync.Once
	finishDestructiveCommit := func() {
		finishCommit.Do(func() { close(destructiveCommitDone) })
	}
	defer finishDestructiveCommit()
	state := &destructiveReadSnapshotState{
		metadataReferencesCBOR: true,
		blobCBORPresent:        true,
		metadataSnapshotOpened: make(chan struct{}),
	}
	blobStore := &destructiveReadSnapshotBlob{
		state:                 state,
		destructiveCommitDone: destructiveCommitDone,
	}
	db := &Database{
		metadata: &reservingDestructiveMetadata{
			destructiveReadSnapshotMetadata: &destructiveReadSnapshotMetadata{
				state: state,
			},
		},
		blobRef: newBlobStoreRef(blobStore),
	}

	writer := NewTxnContext(t.Context(), db, true)
	t.Cleanup(writer.Release)

	resultCh := make(chan destructiveReadSnapshotResult, 1)
	go func() {
		txn, tip, err := NewReadSnapshotContext(t.Context(), db)
		resultCh <- destructiveReadSnapshotResult{txn: txn, tip: tip, err: err}
	}()

	testutil.WaitForCondition(
		t,
		func() bool {
			select {
			case <-state.metadataSnapshotOpened:
				return true
			default:
			}
			db.commitBarrier.mu.Lock()
			defer db.commitBarrier.mu.Unlock()
			return db.commitBarrier.writerWaiting
		},
		5*time.Second,
		"read snapshot must either wait at the commit barrier or open metadata",
	)

	commitErr := writer.Commit()
	finishDestructiveCommit()
	require.NoError(t, commitErr)

	result := testutil.RequireReceive(
		t,
		resultCh,
		5*time.Second,
		"read snapshot must finish after the destructive commit",
	)
	require.NoError(t, result.err)
	require.NotNil(t, result.txn)
	t.Cleanup(result.txn.Release)

	_, blobErr := blobStore.Get(result.txn.Blob(), []byte("referenced-cbor"))
	require.ErrorIs(
		t,
		blobErr,
		types.ErrBlobKeyNotFound,
		"the destructive commit must be visible in the blob snapshot",
	)
	require.Zero(
		t,
		result.tip.BlockNumber,
		"metadata snapshot retained a reference to CBOR deleted before the blob snapshot opened",
	)
	requireCommitBarrierFree(t, db)
	requireDestructiveTransitionBarrierFree(t, db)
}
