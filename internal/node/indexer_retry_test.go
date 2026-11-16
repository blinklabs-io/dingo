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

package node

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/stretchr/testify/require"
)

// failingPatchFacts fails PatchFacts once, on its failOn-th call.
type failingPatchFacts struct {
	indexer.FactsSource
	calls  int
	failOn int
}

func (f *failingPatchFacts) PatchFacts(
	ctx context.Context,
	dst *database.Txn,
	from, through uint64,
) error {
	f.calls++
	if f.calls == f.failOn {
		return errors.New("injected patch failure")
	}
	return f.FactsSource.PatchFacts(ctx, dst, from, through)
}

// A failure after IndexBlocks succeeded rolls the batch back; the retry must
// not continue from the running nonce and epoch state of the lost batch.
func TestIndexerWriterRetryAfterLatePatchFailureMatchesLiveApply(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	live, liveRaw := newEquivalenceDB(t)
	dir := copyImmutableChunkPrefix(t, equivalenceChunks)
	require.NoError(t, LoadWithDB(
		ctx, &config.Config{Network: "preview"}, logger, dir, live,
	))
	idxDB, idxRaw := newEquivalenceIndexer(t, live)
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	store, err := indexerdb.NewStore(idxDB, idxDB, 10)
	require.NoError(t, err)
	metaFacts, err := indexer.NewMetadataFacts(live, idxDB)
	require.NoError(t, err)
	facts := &failingPatchFacts{FactsSource: metaFacts, failOn: 3}
	bf := NewBackfill(idxDB, nodeCfg, logger)
	bf.SetDelegatorInactivityEnabled(false)
	w := indexer.NewWriter(
		store, indexer.NewDatabaseSource(live), bf, logger, 700,
	).WithFacts(facts).WithGenesisSeed(bf.SeedGenesis)

	require.Error(t, w.CatchUp(ctx))
	require.NoError(t, w.CatchUp(ctx))
	require.GreaterOrEqual(t, facts.calls, 4)

	requireSameTables(t, dumpTables(t, liveRaw), dumpTables(t, idxRaw))
}
