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

package dingo

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

type fakeIndexerWriter struct {
	wakes   chan struct{}
	started atomic.Bool
	stopped atomic.Bool
}

func (f *fakeIndexerWriter) Start(context.Context) error {
	f.started.Store(true)
	return nil
}

func (f *fakeIndexerWriter) RollbackTo(context.Context, indexerdb.Tip) error {
	return nil
}

func (f *fakeIndexerWriter) Wake() {
	select {
	case f.wakes <- struct{}{}:
	default:
	}
}

func (f *fakeIndexerWriter) Stop(context.Context) error {
	f.stopped.Store(true)
	return nil
}

func TestIndexerWriterIsWokenByLedgerTipEventsUntilStopped(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	w := &fakeIndexerWriter{wakes: make(chan struct{}, 8)}
	n := &Node{eventBus: bus, indexerWriter: w}

	require.NoError(t, n.startIndexerWriter(t.Context()))
	require.True(t, w.started.Load())
	// Start wakes once, so blocks applied before the writer existed are
	// picked up without waiting for the next tip event.
	testutil.RequireReceive(t, w.wakes, 5*time.Second, "the start wake")

	bus.Publish(
		ledger.TipEventType,
		event.NewEvent(ledger.TipEventType, ledger.TipEvent{}),
	)
	testutil.RequireReceive(
		t,
		w.wakes,
		5*time.Second,
		"a wake for the ledger tip event",
	)

	require.NoError(t, n.stopIndexerWriter(t.Context()))
	require.True(t, w.stopped.Load())
	bus.Publish(
		ledger.TipEventType,
		event.NewEvent(ledger.TipEventType, ledger.TipEvent{}),
	)
	testutil.RequireNoReceive(
		t,
		w.wakes,
		200*time.Millisecond,
		"a wake after stop",
	)
}

func TestIndexerWriterIsAbsentOnACoreNode(t *testing.T) {
	t.Parallel()
	n := &Node{}
	require.NoError(t, n.openIndexerWriter(t.Context()))
	require.Nil(t, n.indexerDB)
	require.Nil(t, n.indexerWriter)
	require.NoError(t, n.startIndexerWriter(t.Context()))
	require.NoError(t, n.stopIndexerWriter(t.Context()))
}

func TestAPIDatabaseFollowsTheLayout(t *testing.T) {
	t.Parallel()
	consensus := &database.Database{}
	core := &Node{db: consensus}
	require.Same(t, consensus, core.apiDatabase())
	require.Nil(t, core.apiReadiness(), "a core node gates nothing")

	indexer := &database.Database{}
	store, err := indexerdb.NewStore(indexer, indexer, 10)
	require.NoError(t, err)
	split := &Node{db: consensus, indexerDB: indexer, indexerStore: store}
	require.Same(t, indexer, split.apiDatabase())
	require.NotNil(t, split.apiReadiness())
}

func TestMeshReportsTheIndexerTipOnTheSplitLayout(t *testing.T) {
	t.Parallel()
	ledgerDB, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	idx, err := dbtest.NewIndexerDatabase(t, ledgerDB, dbtest.IndexerOptions{})
	require.NoError(t, err)
	store, err := indexerdb.NewStore(idx, idx, 10)
	require.NoError(t, err)
	require.NoError(t, idx.Transaction(t.Context(), true).Do(
		func(txn *database.Txn) error {
			return store.SetTip(indexerdb.Tip{
				Slot: 50, BlockNumber: 5, Hash: []byte("indexer-tip"),
			}, txn)
		},
	))
	n := &Node{indexerDB: idx, indexerStore: store}

	tip := n.meshChain().Tip()
	require.EqualValues(t, 50, tip.Point.Slot)
	require.EqualValues(t, 5, tip.BlockNumber)
	require.NotNil(t, n.apiReadiness())
}

// History expiry stays below the IndexerDB tip, keeping the tip block the
// writer reads again to resume, and expires nothing before the first block is
// indexed. Without an IndexerDB nothing bounds it.
func TestHistoryExpiryCeilingFollowsTheIndexerTip(t *testing.T) {
	t.Parallel()
	ceiling, ok, err := (&Node{}).historyExpiryCeiling(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, ^uint64(0), ceiling)

	ledgerDB, err := dbtest.NewDatabase(t, &database.Config{})
	require.NoError(t, err)
	idx, err := dbtest.NewIndexerDatabase(t, ledgerDB, dbtest.IndexerOptions{})
	require.NoError(t, err)
	store, err := indexerdb.NewStore(idx, idx, 10)
	require.NoError(t, err)
	n := &Node{indexerDB: idx, indexerStore: store}
	_, ok, err = n.historyExpiryCeiling(t.Context())
	require.NoError(t, err)
	require.False(t, ok, "nothing is indexed yet")

	require.NoError(t, idx.Transaction(t.Context(), true).Do(
		func(txn *database.Txn) error {
			return store.SetTip(indexerdb.Tip{
				Slot: 50, BlockNumber: 5, Hash: []byte("indexer-tip"),
			}, txn)
		},
	))
	ceiling, ok, err = n.historyExpiryCeiling(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(49), ceiling)
}

// A core node has no IndexerDB, so the APIs that read UTxOs read the ledger's
// live UTxO set; an API node reads its IndexerDB. Reading the consensus
// database instead finds no UTxO rows on a core node.
func TestCoreNodeReadsUtxosFromTheLiveSet(t *testing.T) {
	t.Parallel()
	core := &Node{ledgerState: &ledger.LedgerState{}}
	require.Equal(t,
		"*utxorpc.ledgerQueryDatabase",
		fmt.Sprintf("%T", core.utxorpcQueryDatabase()),
	)
	require.NotNil(t, core.mcpAddressLookup())

	api := &Node{
		ledgerState: &ledger.LedgerState{},
		indexerDB:   &database.Database{},
	}
	require.Equal(t,
		"*utxorpc.indexerQueryDatabase",
		fmt.Sprintf("%T", api.utxorpcQueryDatabase()),
	)
	require.Nil(t, api.mcpAddressLookup())
}
