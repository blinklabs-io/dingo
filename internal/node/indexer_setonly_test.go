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
	"log/slog"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// setOnlyOptions is the configuration of a node whose live UTxO set is the
// only store of the ledger state.
func setOnlyOptions() []dingo.ConfigOptionFunc {
	return []dingo.ConfigOptionFunc{
		dingo.WithLedgerUtxoBadger(true),
		dingo.WithLedgerUtxoBadgerOnly(true),
		dingo.WithLedgerUtxoReads("set"),
		dingo.WithLedgerStateSetOnly(true),
	}
}

// warnLogger sends a node's warnings to the test log.
func warnLogger(t *testing.T) dingo.ConfigOptionFunc {
	return dingo.WithLogger(slog.New(slog.NewTextHandler(
		testLogWriter{t}, &slog.HandlerOptions{Level: slog.LevelWarn},
	)))
}

// testLogWriter writes each log line to the test log.
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// A split node whose ledger drains its SQL state rows fills its IndexerDB
// from the live UTxO set alone, and the rows it holds are those a shared
// layout store holds after applying the same blocks.
func TestSetOnlySplitWriterMatchesTheSharedLayoutRows(t *testing.T) {
	testutil.SkipUnderRace(
		t,
		"it replays 300 real chunks into two nodes, over 40 minutes "+
			"instrumented; the unraced run covers the comparison, and the "+
			"writer under race is covered by "+
			"TestIndexerWriterMatchesLiveApplyOnRealBlocks",
	)
	t.Parallel()
	immutableDir := copyImmutableChunkPrefix(t, parityChunks)
	cardanoCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	sharedDir := loadParityChain(t, immutableDir, false)
	splitDir := loadParityChain(t, immutableDir, true)

	shared := startParityNode(t, "shared", sharedDir, false, cardanoCfg)
	split := startParityNode(
		t, "split", splitDir, true, cardanoCfg,
		append(setOnlyOptions(), warnLogger(t))...,
	)
	waitParityTip(t, shared, split)

	want := dumpTables(
		t, openRaw(t, filepath.Join(sharedDir, "metadata.sqlite")),
	)
	got := dumpTables(t, openRaw(t, filepath.Join(
		splitDir, internalplugins.IndexerDirName, "metadata.sqlite",
	)))
	// Pool metadata documents are fetched by each node on its own schedule,
	// the shared store keeps planner statistics, and only the IndexerDB takes
	// an import baseline.
	for _, name := range []string{
		"offchain_metadata", "account_import_baseline", "sqlite_stat1",
	} {
		delete(want, name)
		delete(got, name)
	}
	maskLiveStakeUpdatedSlot(want, got)
	requireSameTables(t, want, got)
}

// maskLiveStakeUpdatedSlot drops reward_live_stake.updated_slot from the
// dumps. It records the slot of the last write to a credential's row, and the
// ledger's own write path and the IndexerDB's patch of the delegations it
// copies write a row at different slots; no read orders or filters by it.
func maskLiveStakeUpdatedSlot(tables ...map[string][]string) {
	for _, t := range tables {
		rows := t["reward_live_stake"]
		for i, row := range rows {
			rows[i] = liveStakeUpdatedSlot.ReplaceAllString(row, "")
		}
		slices.Sort(rows)
	}
}

var liveStakeUpdatedSlot = regexp.MustCompile(`updated_slot=[^|]*\|`)
