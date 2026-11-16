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
	"encoding/hex"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// A split node whose ledger has drained its account rows into the live UTxO
// set takes over the state a Mithril snapshot imported from the set alone: the
// imported delegations from the set's import baselines, the balances from its
// reward records, and the UTxO set at the anchor from its journal. The rows it
// holds after following the ledger are those a shared layout store holds.
func TestSetOnlySplitMithrilBootstrapMatchesTheSharedLayout(t *testing.T) {
	testutil.SkipUnderRace(
		t,
		"it replays real chunks into a shared store and a set-only node, "+
			"minutes instrumented; the writer under race is covered by "+
			"TestIndexerWriterMatchesLiveApplyOnRealBlocks",
	)
	t.Parallel()
	// A set loaded from the metadata store has a journal that starts at its
	// load tip, so only an anchor at that tip is one it can rebuild.
	for _, tc := range []struct {
		name          string
		anchor, chain int
		digest        bool
	}{
		{"anchor at the ledger tip", chainChunks, chainChunks, true},
		{"anchor beyond the set's journal", anchorChunks, chainChunks, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSetOnlyMithrilBootstrap(t, tc.anchor, tc.chain, tc.digest)
		})
	}
}

// idleIndexerWriter is a writer that writes nothing, for a node that only
// loads its live UTxO set and drains its metadata rows.
type idleIndexerWriter struct{}

func (idleIndexerWriter) Start(context.Context) error { return nil }
func (idleIndexerWriter) Wake()                       {}
func (idleIndexerWriter) Stop(context.Context) error  { return nil }
func (idleIndexerWriter) RollbackTo(context.Context, indexerdb.Tip) error {
	return nil
}

func idleIndexerWriterFactory(
	dingo.IndexerWriterDeps,
) (dingo.IndexerWriter, error) {
	return idleIndexerWriter{}, nil
}

func runSetOnlyMithrilBootstrap(
	t *testing.T,
	anchorAt, chunks int,
	wantDigest bool,
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

	// The oracle: blocks applied live to a shared-layout store, read at the
	// end of the chain. The accounts it holds at the anchor stand in for the
	// ones the snapshot imported.
	live, liveRaw := newEquivalenceDB(t)
	require.NoError(t, LoadWithDB(
		ctx, &config.Config{Network: "preview"}, logger, anchorDir, live,
	))
	anchorTip, err := live.GetTip(nil)
	require.NoError(t, err)
	type account struct {
		tag             uint8
		key, pool, drep []byte
		drepType        int64
		active          bool
	}
	var accounts []account
	rs, err := liveRaw.Query(
		`SELECT credential_tag, staking_key, pool, drep, drep_type, active FROM account`,
	)
	require.NoError(t, err)
	for rs.Next() {
		var a account
		require.NoError(t, rs.Scan(
			&a.tag, &a.key, &a.pool, &a.drep, &a.drepType, &a.active,
		))
		accounts = append(accounts, a)
	}
	require.NoError(t, rs.Err())
	require.NoError(t, rs.Close())
	if anchorAt != chunks {
		require.NoError(t, LoadWithDB(
			ctx, &config.Config{Network: "preview"}, logger, chainDir, live,
		))
	}
	wantTip, err := live.GetTip(nil)
	require.NoError(t, err)
	want := dumpTables(t, liveRaw)

	// The node's data directory, as `mithril sync` leaves a split node's:
	// the whole chain applied, the anchor recorded, and the imported accounts
	// with their baselines.
	splitDir := loadParityChain(t, chainDir, true)
	consensus := openRaw(t, filepath.Join(splitDir, "metadata.sqlite"))
	for key, value := range map[string]string{
		"mithril_ledger_slot": strconv.FormatUint(anchorTip.Point.Slot, 10),
		"mithril_ledger_hash": hex.EncodeToString(anchorTip.Point.Hash),
	} {
		_, err := consensus.Exec(
			`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
			ON CONFLICT (sync_key) DO UPDATE SET value = excluded.value`,
			key, value,
		)
		require.NoError(t, err)
	}
	for _, a := range accounts {
		_, err := consensus.Exec(
			`INSERT INTO account_import_baseline
			(credential_tag, staking_key, pool, drep, drep_type, active, added_slot)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			a.tag, a.key, a.pool, a.drep, a.drepType, a.active,
			anchorTip.Point.Slot,
		)
		require.NoError(t, err)
	}
	require.NoError(t, consensus.Close())

	// The node first runs with a writer that writes nothing, as a set-only
	// node that has no IndexerDB yet does: it loads the live UTxO set, with
	// the import baselines and the reward journal, and drains the metadata
	// rows it replaces.
	prime := startParityNode(
		t, "prime", splitDir, true, cardanoCfg,
		append(
			setOnlyOptions(),
			warnLogger(t),
			dingo.WithIndexerWriterFactory(idleIndexerWriterFactory),
		)...,
	)
	prime.stop()
	consensus = openRaw(t, filepath.Join(splitDir, "metadata.sqlite"))
	var baselines int
	require.NoError(t, consensus.QueryRow(
		`SELECT COUNT(*) FROM account_import_baseline`,
	).Scan(&baselines))
	require.Zero(t, baselines, "the metadata store drained the import baselines")
	require.NoError(t, consensus.Close())

	split := startParityNode(
		t, "split", splitDir, true, cardanoCfg,
		append(setOnlyOptions(), warnLogger(t))...,
	)
	wantHash := hex.EncodeToString(wantTip.Point.Hash)
	testutil.WaitForCondition(t, func() bool {
		var got struct {
			Hash string `json:"hash"`
		}
		return split.getJSON(t, "/api/v0/blocks/latest", &got) &&
			got.Hash == wantHash
	}, 10*time.Minute, "the split node serves the chain tip once its IndexerDB is ready")

	indexerRaw := openRaw(t, filepath.Join(
		splitDir, internalplugins.IndexerDirName, "metadata.sqlite",
	))
	var digests int
	require.NoError(t, indexerRaw.QueryRow(
		`SELECT COUNT(*) FROM sync_state WHERE sync_key = ?`,
		indexerdb.AnchorDigestSyncKey,
	).Scan(&digests))
	require.Equal(
		t, wantDigest, digests == 1,
		"the UTxO digest at the anchor is taken from the set's journal when it reaches the anchor",
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
