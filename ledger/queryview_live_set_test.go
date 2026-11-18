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
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

type querySnapshotSet struct {
	committedUtxoSet
	failure  error
	releases *atomic.Int64
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
	if s.failure != nil {
		return nil, nil, s.failure
	}
	view := &querySnapshotSet{
		committedUtxoSet: maps.Clone(s.committedUtxoSet),
		releases:         s.releases,
	}
	return view, func() { s.releases.Add(1) }, nil
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
	row, err := db.UtxoByRef(t.Context(), id, 0, nil)
	require.NoError(t, err)
	var releases atomic.Int64
	set := &querySnapshotSet{
		committedUtxoSet: committedUtxoSet{
			string(id) + string(rune(0)): bytes.Clone(row.Cbor),
		},
		releases: &releases,
	}
	ls.config.UtxoSet = set
	ls.config.UtxoSetReads = UtxoSetReadsSet
	ls.config.UtxoSetOnly = true
	view, err := ls.AcquireQueryView(t.Context(), QueryPoint{})
	require.NoError(t, err)
	t.Cleanup(view.Close)
	require.NoError(
		t,
		db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
			if err := db.MarkUtxosDeletedAtSlot(t.Context(), txn,
				[]dbtypes.UtxoKey{{TxId: id, OutputIdx: 0}}, 500); err != nil {
				return err
			}
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
		t.Context(),
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
	ls.config.UtxoSetReads = UtxoSetReadsSet
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
	row, err := db.UtxoByRef(t.Context(), id, 0, nil)
	require.NoError(t, err)
	var releases atomic.Int64
	set := &querySnapshotSet{
		committedUtxoSet: committedUtxoSet{
			string(id) + string(rune(0)): bytes.Clone(row.Cbor),
		},
		releases: &releases,
	}
	ls.config.UtxoSet = set
	ls.config.UtxoSetReads = UtxoSetReadsSet
	ls.config.UtxoSetOnly = true
	txn := db.Transaction(t.Context(), true)
	t.Cleanup(txn.Release)
	entered, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	resumeCommit := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(resumeCommit)
	txn.AfterCommit(func() { close(entered); <-resume })
	require.NoError(t, db.MarkUtxosDeletedAtSlot(t.Context(), txn,
		[]dbtypes.UtxoKey{{TxId: id}}, 500))
	ls.stageUtxoSet(txn, nil, UtxoEvent{Removed: []models.UtxoId{{Hash: id}}})
	committed := make(chan error, 1)
	go func() { committed <- txn.Commit() }()
	<-entered
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
		t.Context(),
		[]gledger.ShelleyTransactionInput{
			input,
		},
		QueryPoint{Slot: 500},
		view.txn,
	)
	require.NoError(t, err)
	require.Zero(t, utxoMapLen(t, asOf))
}
