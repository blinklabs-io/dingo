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

package indexer

import (
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/stretchr/testify/require"
)

// durableFacts is a FactsSource that copies nothing and records the
// acknowledgements the writer releases.
type durableFacts struct {
	FactsSource
	committed int
	acked     int
	// durableAt is the number of committed batches the IndexerDB has made
	// durable, which an acknowledgement must not exceed.
	durableAt *int
	// ackedBeyondDurable is set when an acknowledgement covers a batch the
	// IndexerDB had not made durable.
	ackedBeyondDurable bool
}

func (f *durableFacts) Boundaries(
	context.Context, uint64, uint64,
) ([]uint64, error) {
	return nil, nil
}

func (f *durableFacts) CopyFacts(
	context.Context, *database.Txn, uint64, uint64,
) error {
	return nil
}

func (f *durableFacts) PatchFacts(
	context.Context, *database.Txn, uint64, uint64,
) error {
	return nil
}

func (f *durableFacts) FactsCommitted() { f.committed++ }

func (f *durableFacts) AckFacts() error {
	f.acked = f.committed
	if f.acked > *f.durableAt {
		f.ackedBeyondDurable = true
	}
	return nil
}

// The writer acknowledges a batch to the ledger only once the IndexerDB has
// made it durable: a store that commits ahead of its durable point must see no
// acknowledgement past that point, and the ledger prunes what is acknowledged.
func TestWriterAcknowledgesOnlyWhatTheIndexerDBMadeDurable(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(10, "a")
	durable := 0
	facts := &durableFacts{durableAt: &durable}
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 2).WithFacts(facts)
	w.syncEveryBatches = 2
	w.commitSync = func(context.Context) error {
		durable = facts.committed
		return nil
	}
	var acksAtBatch []int
	w.afterBatch = func(indexerdb.Tip) { acksAtBatch = append(acksAtBatch, facts.acked) }
	require.NoError(t, w.CatchUp(t.Context()))

	require.False(t, facts.ackedBeyondDurable,
		"an acknowledgement covered a batch the IndexerDB had not made durable")
	require.Equal(t, 5, facts.committed)
	require.Equal(t, 5, facts.acked, "a caught-up pass acknowledges everything")
	require.Equal(t, []int{0, 2, 2, 4, 4}, acksAtBatch,
		"acknowledgements follow the durable points, not the commits")
}

// A sync that fails leaves the acknowledgement pending and the pass
// succeeding; the next successful sync acknowledges it.
func TestWriterKeepsTheAcknowledgementWhenTheSyncFails(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	chain := newFakeChain(6, "a")
	durable := 0
	facts := &durableFacts{durableAt: &durable}
	w := NewWriter(store, chain, &syncRowIndexer{}, nil, 2).WithFacts(facts)
	failing := true
	w.commitSync = func(context.Context) error {
		if failing {
			return errors.New("not durable")
		}
		durable = facts.committed
		return nil
	}
	require.NoError(t, w.CatchUp(t.Context()))
	require.Zero(t, facts.acked)
	failing = false
	w.syncAndAck(t.Context())
	require.Equal(t, 3, facts.acked)
	require.False(t, facts.ackedBeyondDurable)
}
