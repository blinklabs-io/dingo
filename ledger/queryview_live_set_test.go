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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

type querySnapshotSet struct {
	committedUtxoSet
	failure                error
	snapshotPanic          bool
	snapshotWithoutHistory bool
	releases               *atomic.Int64
}

func (s *querySnapshotSet) ApplyUtxoChanges(
	_ *ocommon.Point,
	changes []UtxoEvent,
) error {
	for _, change := range changes {
		for _, id := range change.Removed {
			delete(s.committedUtxoSet, string(id.Hash)+string(rune(id.Idx)))
		}
		for _, output := range change.Added {
			s.committedUtxoSet[string(output.Ref.Hash)+string(rune(output.Ref.Idx))] = bytes.Clone(
				output.Cbor,
			)
		}
	}
	return nil
}

func (s *querySnapshotSet) SnapshotUtxoSet() (UtxoSetWriter, func(), error) {
	if s.snapshotPanic {
		panic("snapshot provider panic")
	}
	if s.failure != nil {
		return nil, nil, s.failure
	}
	view := &querySnapshotSet{
		committedUtxoSet: maps.Clone(s.committedUtxoSet),
		releases:         s.releases,
	}
	if s.snapshotWithoutHistory {
		return struct {
				UtxoSetWriter
				UtxoSetReader
			}{view, view},
			func() { s.releases.Add(1) }, nil
	}
	return view, func() { s.releases.Add(1) }, nil
}

func TestQueryViewSnapshotPanicReleasesApplyLock(t *testing.T) {
	t.Parallel()
	ls, _ := newDiskTestLedger(t)
	ls.config.UtxoSet = &querySnapshotSet{snapshotPanic: true}
	require.PanicsWithValue(t, "snapshot provider panic", func() {
		_, _ = ls.AcquireQueryView(t.Context(), QueryPoint{})
	})
	unlocked := ls.utxoSetApplyMu.TryLock()
	if unlocked {
		ls.utxoSetApplyMu.Unlock()
	} else {
		// Keep a retained read lock from hanging cleanup on a broken build.
		t.Cleanup(ls.utxoSetApplyMu.RUnlock)
	}
	require.True(t, unlocked, "snapshot panic retained the apply lock")
}

func TestQueryViewContendedAnchorReturnsPending(t *testing.T) {
	t.Parallel()
	ls, db := newDiskTestLedger(t)
	var releases atomic.Int64
	ls.config.UtxoSet = &querySnapshotSet{releases: &releases}
	ls.utxoSetApplyMu.Lock()
	unlock := sync.OnceFunc(ls.utxoSetApplyMu.Unlock)
	done, result := make(chan struct{}), make(chan error, 1)
	t.Cleanup(func() {
		unlock()
		testutil.RequireReceive(t, done, 5*time.Second, "anchor cleanup")
	})
	go func() {
		defer close(done)
		txn, _, err := database.NewReadSnapshotWithAnchorContext(
			t.Context(), db, func(txn *database.Txn) error {
				return ls.pinSetReadsResult(txn, true)
			},
		)
		if txn != nil {
			txn.Release()
		}
		result <- err
	}()
	require.ErrorIs(t, testutil.RequireReceive(
		t, result, 5*time.Second, "contended anchor must release commits",
	), errQueryViewSetPending)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	writer := db.Transaction(ctx, true)
	t.Cleanup(writer.Release)
	require.NoError(t, writer.Commit())
}

func TestQueryViewApplyLockHonorsAcquisitionDeadline(t *testing.T) {
	t.Parallel()
	ls, _ := newDiskTestLedger(t)
	var releases atomic.Int64
	ls.config.UtxoSet = &querySnapshotSet{releases: &releases}
	ls.utxoSetApplyMu.Lock()
	unlock := sync.OnceFunc(ls.utxoSetApplyMu.Unlock)
	done, result := make(chan struct{}), make(chan error, 1)
	t.Cleanup(func() {
		unlock()
		testutil.RequireReceive(t, done, 5*time.Second, "acquisition cleanup")
	})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	go func() {
		defer close(done)
		view, err := ls.AcquireQueryView(ctx, QueryPoint{})
		if view != nil {
			view.Close()
		}
		result <- err
	}()
	require.ErrorIs(t, testutil.RequireReceive(
		t, result, 5*time.Second, "snapshot acquisition deadline",
	), context.DeadlineExceeded)
}

func TestQueryViewRequiresSnapshotCapabilities(t *testing.T) {
	t.Parallel()
	var releases atomic.Int64
	set := &querySnapshotSet{releases: &releases}
	providers := []UtxoSetWriter{
		struct {
			UtxoSetWriter
			UtxoSetReader
			UtxoSetHistory
		}{set, set, set},
		struct {
			UtxoSetWriter
			UtxoSetReader
			UtxoSetSnapshotter
		}{set, set, set},
		&querySnapshotSet{releases: &releases, snapshotWithoutHistory: true},
	}
	for i, provider := range providers {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			ls, _ := newDiskTestLedger(t)
			ls.config.UtxoSet = provider
			view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
			if view != nil {
				view.Close()
			}
			require.ErrorContains(t, err, "query view")
			require.Nil(t, view)
		})
	}
}

func (s *querySnapshotSet) UtxoSetAt(ocommon.Point) (UtxoSetView, error) {
	return querySnapshotUtxoView{s}, nil
}

type querySnapshotUtxoView struct{ *querySnapshotSet }

func (v querySnapshotUtxoView) EachUtxo(
	fn func([]byte, uint32, []byte) error,
) error {
	for key, encoded := range v.committedUtxoSet {
		if err := fn([]byte(key[:32]), uint32(key[32]), encoded); err != nil {
			return err
		}
	}
	return nil
}

func (querySnapshotUtxoView) Close() {}

func TestQueryViewPinsLiveUtxoReads(t *testing.T) {
	t.Parallel()
	ls, db := newDiskTestLedger(t)
	id := seedBabbageUtxo(t, db, 0x71, 0, queryViewTestAddress(t), 2_000_000)
	row, err := ls.UtxoByRef(id, 0)
	require.NoError(t, err)
	var releases atomic.Int64
	set := &querySnapshotSet{
		committedUtxoSet: committedUtxoSet{
			string(id) + string(rune(0)): bytes.Clone(row.Cbor),
		},
		releases: &releases,
	}
	ls.config.UtxoSet = set
	view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)
	t.Cleanup(view.Close)
	require.NoError(
		t,
		db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
			txn.AfterCommit(
				func() { delete(set.committedUtxoSet, string(id)+string(rune(0))) },
			)
			return nil
		}),
	)
	input := gledger.MustNewShelleyTransactionInput(hex.EncodeToString(id), 0)
	query := shelleyBlockQuery(
		&olocalstatequery.ShelleyUtxoByTxinQuery{
			TxIns: []gledger.ShelleyTransactionInput{input},
		},
	)
	live, err := ls.Query(t.Context(), query, QueryPoint{})
	require.NoError(t, err)
	require.Zero(t, utxoMapLen(t, live))
	pinned, err := view.Query(t.Context(), query, 0)
	require.NoError(t, err)
	require.Equal(t, 1, utxoMapLen(t, pinned))
	asOf, err := ls.queryShelleyUtxoByTxIn(
		[]gledger.ShelleyTransactionInput{input},
		QueryPoint{Slot: 300},
		view.txn,
	)
	require.NoError(t, err)
	require.Equal(t, 1, utxoMapLen(t, asOf))
	require.Zero(t, releases.Load())
	view.Close()
	require.EqualValues(t, 1, releases.Load())
}

func TestQueryViewLiveSetSnapshotFailureReleasesTheView(t *testing.T) {
	t.Parallel()
	ls, _ := newDiskTestLedger(t)
	failure := errors.New("live set snapshot unavailable")
	var releases atomic.Int64
	set := &querySnapshotSet{
		committedUtxoSet: committedUtxoSet{},
		failure:          failure,
		releases:         &releases,
	}
	ls.config.UtxoSet = set
	view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
	if view != nil {
		t.Cleanup(view.Close)
	}
	require.ErrorIs(t, err, failure)
	require.Nil(t, view)
	require.Empty(t, ls.utxoSetPins)
	require.Empty(t, ls.utxoSetPinsByMeta)
	set.failure = nil
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	view, err = ls.AcquireQueryView(ctx, QueryPoint{})
	require.NoError(t, err)
	view.Close()
	require.EqualValues(t, 1, releases.Load())
}

func TestQueryViewWaitsForCommittedSetChanges(t *testing.T) {
	t.Parallel()
	ls, db := newDiskTestLedger(t)
	id := seedBabbageUtxo(t, db, 0x72, 0, queryViewTestAddress(t), 2_000_000)
	row, err := ls.UtxoByRef(id, 0)
	require.NoError(t, err)
	var releases atomic.Int64
	set := &querySnapshotSet{
		committedUtxoSet: committedUtxoSet{
			string(id) + string(rune(0)): bytes.Clone(row.Cbor),
		},
		releases: &releases,
	}
	ls.config.UtxoSet = set
	txn := db.Transaction(t.Context(), true)
	t.Cleanup(txn.Release)
	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	resumeCommit := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(resumeCommit)
	txn.AfterCommit(func() { close(entered); <-resume })
	ls.stageUtxoSet(txn, nil, UtxoEvent{Removed: []models.UtxoId{{Hash: id}}})
	committed := make(chan error, 1)
	go func() { committed <- txn.Commit() }()
	select {
	case <-entered:
	case err := <-committed:
		t.Fatalf("writer stopped before after-commit callback: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not reach after-commit callback")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	view, err := ls.AcquireQueryView(ctx, QueryPoint{})
	if view != nil {
		view.Close()
	}
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, view)
	require.Empty(t, ls.utxoSetPins)
	require.Empty(t, ls.utxoSetPinsByMeta)
	resumeCommit()
	require.NoError(t, <-committed)
	view, err = ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)
	t.Cleanup(view.Close)
	input := gledger.MustNewShelleyTransactionInput(hex.EncodeToString(id), 0)
	asOf, err := ls.queryShelleyUtxoByTxIn(
		[]gledger.ShelleyTransactionInput{
			input,
		},
		QueryPoint{Slot: 500},
		view.txn,
	)
	require.NoError(t, err)
	require.Zero(t, utxoMapLen(t, asOf))
}
