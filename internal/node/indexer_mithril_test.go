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
	"encoding/hex"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// A node whose ledger state lives in the live UTxO set takes over the state a
// Mithril snapshot imported from the set alone: the imported delegations from
// the set's import baselines, the balances from its reward records, and the
// UTxO set at the anchor from its journal. The rows it holds after following
// the ledger are those an IndexerDB indexed from genesis over the same blocks
// holds.
func TestMithrilBootstrapMatchesAGenesisIndex(t *testing.T) {
	testutil.SkipUnderRace(
		t,
		"it replays real chunks into a reference and a node, "+
			"minutes instrumented; the writer under race is covered by "+
			"TestIndexerWriterRollbackMatchesAFreshIndex",
	)
	t.Parallel()
	// The set journals every block the load applied, so it reaches an anchor
	// at the ledger tip and one behind it.
	for _, tc := range []struct {
		name          string
		anchor, chain int
	}{
		{"anchor at the ledger tip", chainChunks, chainChunks},
		{"anchor behind the ledger tip", anchorChunks, chainChunks},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runLedgerDBMithrilBootstrap(t, tc.anchor, tc.chain)
		})
	}
}

func runLedgerDBMithrilBootstrap(
	t *testing.T,
	anchorAt, chunks int,
) {
	t.Helper()
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cardanoCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	chainDir := copyImmutableChunkPrefix(t, chunks)
	anchorDir := chainDir
	if anchorAt != chunks {
		anchorDir = copyImmutableChunkPrefix(t, anchorAt)
	}

	// The reference: a ledger database loaded to the end of the chain and
	// indexed from genesis.
	ref := newEquivalenceDB(t)
	require.NoError(t, LoadWithDB(
		ctx, equivalenceLoadConfig(), logger, anchorDir, ref,
	))
	anchorTip, err := ref.GetTip(nil)
	require.NoError(t, err)
	if anchorAt != chunks {
		require.NoError(t, LoadWithDB(
			ctx, equivalenceLoadConfig(), logger, chainDir, ref,
		))
	}
	wantTip, err := ref.GetTip(nil)
	require.NoError(t, err)
	refLedger, _ := openFactLedger(t, ref)
	_, refRaw := indexFromGenesis(t, ref, refLedger, nil)
	want := dumpTables(t, refRaw)

	// The node's data directory, as `mithril sync` leaves it: the whole chain
	// applied, the ledger state in the live UTxO set, and the anchor
	// recorded.
	ledgerDBDir := loadParityChain(t, chainDir)
	dbConfig, selections, deps := storeConfig(ledgerDBDir)
	runtime, err := internalplugins.OpenDatabase(
		t.Context(), dbConfig, selections, deps,
	)
	require.NoError(t, err)
	for key, value := range map[string]string{
		"mithril_ledger_slot": strconv.FormatUint(anchorTip.Point.Slot, 10),
		"mithril_ledger_hash": hex.EncodeToString(anchorTip.Point.Hash),
	} {
		require.NoError(t, runtime.Database.SetChainState(key, value, nil))
	}
	require.NoError(t, runtime.Close(t.Context()))

	apiNode := startParityNode(t, "api", ledgerDBDir, cardanoCfg, warnLogger(t))
	wantHash := hex.EncodeToString(wantTip.Point.Hash)
	testutil.WaitForCondition(t, func() bool {
		var got struct {
			Hash string `json:"hash"`
		}
		return apiNode.getJSON(t, "/api/v0/blocks/latest", &got) &&
			got.Hash == wantHash
	}, 10*time.Minute, "the API node serves the chain tip once its IndexerDB is ready")

	indexerRaw := openRaw(t, filepath.Join(
		ledgerDBDir, internalplugins.IndexerDirName, "metadata.sqlite",
	))
	var digests int
	require.NoError(t, indexerRaw.QueryRow(
		`SELECT COUNT(*) FROM sync_state WHERE sync_key = ?`,
		indexerdb.AnchorDigestSyncKey,
	).Scan(&digests))
	require.Equal(
		t,
		1,
		digests,
		"the UTxO digest at the anchor is taken from the set's journal",
	)
	got := dumpTables(t, indexerRaw)
	for _, name := range []string{
		"offchain_metadata", "account_import_baseline", "sqlite_stat1",
	} {
		delete(want, name)
		delete(got, name)
	}
	maskLiveStakeUpdatedSlot(want, got)
	requireSameTables(t, want, got)
}
