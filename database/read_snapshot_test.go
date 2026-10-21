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

type orderedSnapshotMetadata struct {
	metadata.MetadataStore
	events *[]string
	tip    ochainsync.Tip
	tipErr error
}

type anchorRollbackTxn struct {
	anchorErr   error
	rollbackErr error
}

func (t *anchorRollbackTxn) Commit() error { return nil }

func (t *anchorRollbackTxn) Rollback() error { return t.rollbackErr }

type anchorRollbackMetadata struct {
	metadata.MetadataStore
	txn *anchorRollbackTxn
}

func (m *anchorRollbackMetadata) ReadTransaction(context.Context) types.Txn {
	return m.txn
}

func (m *anchorRollbackMetadata) GetTip(types.Txn) (ochainsync.Tip, error) {
	return ochainsync.Tip{}, m.txn.anchorErr
}

func (s *orderedSnapshotMetadata) ReadTransaction(context.Context) types.Txn {
	*s.events = append(*s.events, "metadata transaction")
	return &commitFailingTxn{}
}

func (s *orderedSnapshotMetadata) GetTip(
	types.Txn,
) (ochainsync.Tip, error) {
	*s.events = append(*s.events, "metadata tip")
	return s.tip, s.tipErr
}

type orderedSnapshotBlob struct {
	blob.BlobStore
	events *[]string
}

func (s *orderedSnapshotBlob) NewTransaction(bool) types.Txn {
	*s.events = append(*s.events, "blob transaction")
	return &commitFailingTxn{}
}

func requireDestructiveTransitionBarrierFree(t *testing.T, db *Database) {
	t.Helper()
	started := make(chan func(), 1)
	go func() {
		started <- db.BeginDestructiveTransition()
	}()
	finish := testutil.RequireReceive(
		t,
		started,
		5*time.Second,
		"destructive transition barrier must be free after snapshot construction",
	)
	finish()
}

func TestNewReadSnapshotContextAnchorsMetadataBeforeBlob(t *testing.T) {
	t.Parallel()

	events := []string{}
	wantTip := ochainsync.Tip{BlockNumber: 42}
	db := &Database{
		metadata: &orderedSnapshotMetadata{events: &events, tip: wantTip},
		blobRef:  newBlobStoreRef(&orderedSnapshotBlob{events: &events}),
	}

	txn, tip, err := NewReadSnapshotContext(t.Context(), db)
	require.NoError(t, err)
	t.Cleanup(txn.Release)
	require.Equal(t, wantTip, tip)
	require.Equal(
		t,
		[]string{"metadata transaction", "metadata tip", "blob transaction"},
		events,
	)
	requireCommitBarrierFree(t, db)
	requireDestructiveTransitionBarrierFree(t, db)
}

func TestNewReadSnapshotContextReleasesBarrierOnAnchorError(t *testing.T) {
	t.Parallel()

	anchorErr := errors.New("tip unavailable")
	events := []string{}
	db := &Database{
		metadata: &orderedSnapshotMetadata{
			events: &events,
			tipErr: anchorErr,
		},
		blobRef: newBlobStoreRef(&orderedSnapshotBlob{events: &events}),
	}

	txn, _, err := NewReadSnapshotContext(t.Context(), db)
	require.Nil(t, txn)
	require.ErrorIs(t, err, anchorErr)
	require.ErrorContains(t, err, "anchor metadata read snapshot")
	require.Equal(
		t,
		[]string{"metadata transaction", "metadata tip"},
		events,
	)
	requireCommitBarrierFree(t, db)
	requireDestructiveTransitionBarrierFree(t, db)
}

func TestNewReadSnapshotContextJoinsAnchorAndRollbackErrors(t *testing.T) {
	t.Parallel()

	anchorErr := errors.New("tip unavailable")
	rollbackErr := errors.New("rollback unavailable")
	db := &Database{
		metadata: &anchorRollbackMetadata{
			txn: &anchorRollbackTxn{
				anchorErr: anchorErr, rollbackErr: rollbackErr,
			},
		},
	}

	txn, _, err := NewReadSnapshotContext(t.Context(), db)
	require.Nil(t, txn)
	require.ErrorIs(t, err, anchorErr)
	require.ErrorIs(t, err, rollbackErr)
}

type destructiveReadSnapshotState struct {
	mu sync.Mutex

	metadataReferencesCBOR bool
	blobCBORPresent        bool

	metadataSnapshotOpened chan struct{}
	metadataSnapshotOnce   sync.Once
}

type destructiveReadSnapshotTxn struct {
	commit func()

	metadataReferencesCBOR bool
	blobCBORPresent        bool
}

func (t *destructiveReadSnapshotTxn) Commit() error {
	if t.commit != nil {
		t.commit()
	}
	return nil
}

func (*destructiveReadSnapshotTxn) Rollback() error {
	return nil
}

type destructiveReadSnapshotMetadata struct {
	metadata.MetadataStore
	state *destructiveReadSnapshotState
}

func (s *destructiveReadSnapshotMetadata) Transaction(
	context.Context,
) types.Txn {
	return &destructiveReadSnapshotTxn{
		commit: func() {
			s.state.mu.Lock()
			s.state.metadataReferencesCBOR = false
			s.state.mu.Unlock()
		},
	}
}

func (s *destructiveReadSnapshotMetadata) ReadTransaction(
	context.Context,
) types.Txn {
	s.state.mu.Lock()
	referencesCBOR := s.state.metadataReferencesCBOR
	s.state.mu.Unlock()
	s.state.metadataSnapshotOnce.Do(func() {
		close(s.state.metadataSnapshotOpened)
	})
	return &destructiveReadSnapshotTxn{
		metadataReferencesCBOR: referencesCBOR,
	}
}

func (s *destructiveReadSnapshotMetadata) GetTip(
	txn types.Txn,
) (ochainsync.Tip, error) {
	snapshot, ok := txn.(*destructiveReadSnapshotTxn)
	if !ok {
		return ochainsync.Tip{}, types.ErrTxnWrongType
	}
	if snapshot.metadataReferencesCBOR {
		return ochainsync.Tip{BlockNumber: 42}, nil
	}
	return ochainsync.Tip{}, nil
}

func (*destructiveReadSnapshotMetadata) SetCommitTimestamp(
	int64,
	types.Txn,
) error {
	return nil
}

// destructiveReadSnapshotBlob deletes the referenced CBOR when a read-write
// transaction commits. beforeCommit runs at the start of that blob commit,
// after the commit timestamp is written and before metadata commits;
// beforeReadView runs after the snapshot fixed its metadata view and before
// it opens the blob view. Either may block to hold the other side at the
// disputed boundary.
type destructiveReadSnapshotBlob struct {
	blob.BlobStore
	state          *destructiveReadSnapshotState
	beforeCommit   func()
	beforeReadView func()
}

func (s *destructiveReadSnapshotBlob) NewTransaction(
	readWrite bool,
) types.Txn {
	if readWrite {
		return &destructiveReadSnapshotTxn{
			commit: func() {
				if s.beforeCommit != nil {
					s.beforeCommit()
				}
				s.state.mu.Lock()
				s.state.blobCBORPresent = false
				s.state.mu.Unlock()
			},
		}
	}

	if s.beforeReadView != nil {
		s.beforeReadView()
	}
	s.state.mu.Lock()
	present := s.state.blobCBORPresent
	s.state.mu.Unlock()
	return &destructiveReadSnapshotTxn{blobCBORPresent: present}
}

func (*destructiveReadSnapshotBlob) SetCommitTimestamp(
	int64,
	types.Txn,
) error {
	return nil
}

func (*destructiveReadSnapshotBlob) Sync() error {
	return nil
}

func (*destructiveReadSnapshotBlob) Get(
	txn types.Txn,
	_ []byte,
) ([]byte, error) {
	snapshot, ok := txn.(*destructiveReadSnapshotTxn)
	if !ok {
		return nil, types.ErrTxnWrongType
	}
	if !snapshot.blobCBORPresent {
		return nil, types.ErrBlobKeyNotFound
	}
	return []byte{0x80}, nil
}

type destructiveReadSnapshotResult struct {
	txn *Txn
	tip ochainsync.Tip
	err error
}

type observedDoneContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

// TestNewReadSnapshotContextDoesNotStraddleDestructiveCommit guards the
// exact-commit boundary. Opening metadata before blob is sufficient for an
// additive blob-before-metadata commit, but not for a destructive commit: its
// blob deletion can land after the metadata view is fixed and before the blob
// view opens, leaving retained metadata that refers to missing CBOR.
func TestNewReadSnapshotContextDoesNotStraddleDestructiveCommit(t *testing.T) {
	t.Parallel()
	testReadSnapshotDoesNotStraddleDestructiveCommit(
		t,
		func(
			state *destructiveReadSnapshotState,
			blobStore *destructiveReadSnapshotBlob,
		) *Database {
			return &Database{
				metadata: &destructiveReadSnapshotMetadata{state: state},
				blobRef:  newBlobStoreRef(blobStore),
			}
		},
	)
}

func newDestructiveReadSnapshotState() *destructiveReadSnapshotState {
	return &destructiveReadSnapshotState{
		metadataReferencesCBOR: true,
		blobCBORPresent:        true,
		metadataSnapshotOpened: make(chan struct{}),
	}
}

func requireDestructiveSnapshotView(
	t *testing.T,
	blobStore *destructiveReadSnapshotBlob,
	result destructiveReadSnapshotResult,
	afterCommit bool,
) {
	t.Helper()
	require.NoError(t, result.err)
	require.NotNil(t, result.txn)
	t.Cleanup(result.txn.Release)
	_, blobErr := blobStore.Get(result.txn.Blob(), []byte("referenced-cbor"))
	if afterCommit {
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
		return
	}
	require.NoError(
		t,
		blobErr,
		"blob snapshot saw a commit its metadata snapshot predates",
	)
	require.Equal(t, uint64(42), result.tip.BlockNumber)
}

// testReadSnapshotDoesNotStraddleDestructiveCommit runs both orders in which
// a snapshot and a destructive commit can meet. A commit already in progress
// is waited for, so both views see it; a snapshot that has fixed its metadata
// view holds the commit off until its blob view is open, so neither does.
// Neither order lets an open, not yet committing, read-write Txn delay the
// snapshot.
func testReadSnapshotDoesNotStraddleDestructiveCommit(
	t *testing.T,
	newDB func(*destructiveReadSnapshotState, *destructiveReadSnapshotBlob) *Database,
) {
	t.Run("commit in progress", func(t *testing.T) {
		t.Parallel()
		state := newDestructiveReadSnapshotState()
		entered := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseCommit := func() { releaseOnce.Do(func() { close(release) }) }
		defer releaseCommit()
		blobStore := &destructiveReadSnapshotBlob{
			state: state,
			beforeCommit: func() {
				close(entered)
				<-release
			},
		}
		db := newDB(state, blobStore)

		writer := NewTxnContext(t.Context(), db, true)
		t.Cleanup(writer.Release)
		commitDone := make(chan error, 1)
		go func() { commitDone <- writer.Commit() }()
		testutil.RequireReceive(
			t, entered, 5*time.Second, "the commit must reach its blob commit",
		)

		resultCh := make(chan destructiveReadSnapshotResult, 1)
		go func() {
			txn, tip, err := NewReadSnapshotContext(t.Context(), db)
			resultCh <- destructiveReadSnapshotResult{txn: txn, tip: tip, err: err}
		}()
		testutil.WaitForCondition(
			t,
			func() bool {
				db.commitPhase.mu.Lock()
				defer db.commitPhase.mu.Unlock()
				return db.commitPhase.writerWaiting
			},
			5*time.Second,
			"read snapshot must wait for the commit in progress",
		)
		select {
		case <-state.metadataSnapshotOpened:
			t.Fatal("metadata view opened while a commit was in progress")
		default:
		}

		releaseCommit()
		require.NoError(t, testutil.RequireReceive(
			t, commitDone, 5*time.Second, "commit must finish",
		))
		result := testutil.RequireReceive(
			t, resultCh, 5*time.Second,
			"read snapshot must finish after the destructive commit",
		)
		requireDestructiveSnapshotView(t, blobStore, result, true)
		requireCommitBarrierFree(t, db)
		requireDestructiveTransitionBarrierFree(t, db)
	})

	t.Run("snapshot first", func(t *testing.T) {
		t.Parallel()
		state := newDestructiveReadSnapshotState()
		opening := make(chan struct{})
		proceed := make(chan struct{})
		var proceedOnce sync.Once
		proceedSnapshot := func() { proceedOnce.Do(func() { close(proceed) }) }
		defer proceedSnapshot()
		blobStore := &destructiveReadSnapshotBlob{
			state: state,
			beforeReadView: func() {
				close(opening)
				<-proceed
			},
		}
		db := newDB(state, blobStore)

		// The writer is open before the snapshot starts and stays open
		// while it runs: only its commit, not its lifetime, may hold the
		// snapshot off.
		writer := NewTxnContext(t.Context(), db, true)
		t.Cleanup(writer.Release)

		resultCh := make(chan destructiveReadSnapshotResult, 1)
		go func() {
			txn, tip, err := NewReadSnapshotContext(t.Context(), db)
			resultCh <- destructiveReadSnapshotResult{txn: txn, tip: tip, err: err}
		}()
		testutil.RequireReceive(
			t, opening, 5*time.Second,
			"an open read-write Txn must not keep the snapshot from opening",
		)

		commitDone := make(chan error, 1)
		go func() { commitDone <- writer.Commit() }()
		testutil.RequireNoReceive(
			t, commitDone, 100*time.Millisecond,
			"a commit must not land between the snapshot's two views",
		)
		proceedSnapshot()

		result := testutil.RequireReceive(
			t, resultCh, 5*time.Second, "read snapshot must finish",
		)
		requireDestructiveSnapshotView(t, blobStore, result, false)
		require.NoError(t, testutil.RequireReceive(
			t, commitDone, 5*time.Second,
			"the commit must proceed once the snapshot's views are open",
		))
		requireCommitBarrierFree(t, db)
		requireDestructiveTransitionBarrierFree(t, db)
	})
}

// TestNewReadSnapshotContextWaitsForLogicalDestructiveTransition covers the
// rollback shape that cannot be represented by one combined transaction:
// chain management first removes block CBOR in a blob-only transaction, then
// ledger rollback removes the metadata that references it in a later combined
// transaction. The transition barrier must cover that whole gap without making
// either nested write acquire the transition barrier itself.
func TestNewReadSnapshotContextWaitsForLogicalDestructiveTransition(
	t *testing.T,
) {
	t.Parallel()

	state := &destructiveReadSnapshotState{
		metadataReferencesCBOR: true,
		blobCBORPresent:        true,
		metadataSnapshotOpened: make(chan struct{}),
	}
	db := &Database{
		metadata: &destructiveReadSnapshotMetadata{state: state},
		blobRef: newBlobStoreRef(&destructiveReadSnapshotBlob{
			state: state,
		}),
	}

	finishTransition := db.BeginDestructiveTransition()
	transitionFinished := false
	defer func() {
		if !transitionFinished {
			finishTransition()
		}
	}()

	// This models ChainManager.removeBlockByIndex/RewindPrimaryChainToPoint:
	// the blob-only write commits while the old metadata is still visible.
	blobTxn := NewBlobOnlyTxn(db, true)
	require.NoError(t, blobTxn.Commit())

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := make(chan struct{})
	resultCh := make(chan destructiveReadSnapshotResult, 1)
	go func() {
		txn, tip, err := NewReadSnapshotContext(
			&observedDoneContext{Context: ctx, observed: observed},
			db,
		)
		resultCh <- destructiveReadSnapshotResult{txn: txn, tip: tip, err: err}
	}()

	// RLockContext evaluates Done only once it is actually waiting behind the
	// held destructive transition, giving the test a deterministic handshake
	// instead of relying on a scheduling delay.
	testutil.RequireReceive(
		t,
		observed,
		5*time.Second,
		"read snapshot must wait for the logical destructive transition",
	)

	// The metadata rollback is an ordinary combined write nested beneath the
	// transition. It must remain able to open and commit; making all writes
	// acquire the transition barrier would deadlock here.
	metadataTxn := NewMetadataOnlyTxn(context.Background(), db, true)
	require.NoError(t, metadataTxn.Commit())
	finishTransition()
	transitionFinished = true

	result := testutil.RequireReceive(
		t,
		resultCh,
		5*time.Second,
		"read snapshot must open after the logical rollback completes",
	)
	require.NoError(t, result.err)
	require.NotNil(t, result.txn)
	t.Cleanup(result.txn.Release)
	require.Zero(t, result.tip.BlockNumber)
	_, blobErr := db.Blob().Get(result.txn.Blob(), []byte("referenced-cbor"))
	require.ErrorIs(t, blobErr, types.ErrBlobKeyNotFound)
	requireCommitBarrierFree(t, db)
	requireDestructiveTransitionBarrierFree(t, db)
}
