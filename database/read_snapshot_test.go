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

type destructiveReadSnapshotBlob struct {
	blob.BlobStore
	state                 *destructiveReadSnapshotState
	destructiveCommitDone <-chan struct{}
}

func (s *destructiveReadSnapshotBlob) NewTransaction(
	readWrite bool,
) types.Txn {
	if readWrite {
		return &destructiveReadSnapshotTxn{
			commit: func() {
				s.state.mu.Lock()
				s.state.blobCBORPresent = false
				s.state.mu.Unlock()
			},
		}
	}

	// Force the blob read view to open after the destructive combined commit.
	// Without the construction barrier, the metadata view has already opened
	// by this point and retains the row that references this now-missing CBOR.
	<-s.destructiveCommitDone
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
	metadataStore := &destructiveReadSnapshotMetadata{state: state}
	blobStore := &destructiveReadSnapshotBlob{
		state:                 state,
		destructiveCommitDone: destructiveCommitDone,
	}
	db := &Database{metadata: metadataStore, blobRef: newBlobStoreRef(blobStore)}

	// Construct the writer first so it already holds the shared side of the
	// commit barrier when snapshot construction begins.
	writer := NewTxnContext(t.Context(), db, true)
	t.Cleanup(writer.Release)

	resultCh := make(chan destructiveReadSnapshotResult, 1)
	go func() {
		txn, tip, err := NewReadSnapshotContext(t.Context(), db)
		resultCh <- destructiveReadSnapshotResult{txn: txn, tip: tip, err: err}
	}()

	// This handshake works on both sides of the regression. With the fix, the
	// snapshot is queued on the barrier; without it, the metadata snapshot has
	// already opened. In either case, the destructive commit starts only after
	// snapshot construction has reached the disputed boundary.
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

	// The read transaction remains open, but the construction-only barrier
	// hold must already be gone so unrelated commits can proceed.
	requireCommitBarrierFree(t, db)
	requireDestructiveTransitionBarrierFree(t, db)
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
	destructiveCommitDone := make(chan struct{})
	close(destructiveCommitDone)
	state := &destructiveReadSnapshotState{
		metadataReferencesCBOR: true,
		blobCBORPresent:        true,
		metadataSnapshotOpened: make(chan struct{}),
	}
	db := &Database{
		metadata: &destructiveReadSnapshotMetadata{state: state},
		blobRef: newBlobStoreRef(&destructiveReadSnapshotBlob{
			state:                 state,
			destructiveCommitDone: destructiveCommitDone,
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
	metadataTxn := NewMetadataOnlyTxn(db, true)
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
