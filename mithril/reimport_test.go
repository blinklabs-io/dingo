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

package mithril

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/drainfix"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// reimportFixture is a drained database whose set holds the ledger state at
// slot 1000, with a fake aggregator listing the artifacts a re-import tries.
type reimportFixture struct {
	db       *database.Database
	set      *utxoset.Set
	nodeCfg  *cardano.CardanoNodeConfig
	pinned   []string
	starts   []uint64
	onChain  map[uint64]bool
	states   map[string]uint64
	artifact []CardanoDatabaseSnapshotListItem
}

func stateHash(slot uint64) []byte {
	return bytes.Repeat([]byte{byte(slot / 100)}, 32)
}

func newReimportFixture(t *testing.T) *reimportFixture {
	t.Helper()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview", cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	set, err := utxoset.Open(utxoset.Config{
		Dir: t.TempDir(), Only: true, Authoritative: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	f := &reimportFixture{
		db: db, set: set, nodeCfg: nodeCfg,
		onChain: map[uint64]bool{},
		states:  map[string]uint64{},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, _, _, err = importLedgerState(
		t.Context(), db, logger, nodeCfg,
		verifiedAncillaryStateAt(t, "1000", minimalLedgerState(t, 1000, stateHash(1000))),
		false, 1000, set, nil,
	)
	require.NoError(t, err)
	drainfix.Drain(t, db, set)
	return f
}

// addArtifact lists an artifact whose ancillary tree holds the ledger state at
// slot, newest first as the aggregator lists them.
func (f *reimportFixture) addArtifact(hash string, slot uint64) {
	f.artifact = append(f.artifact, CardanoDatabaseSnapshotListItem{
		Hash:   hash,
		Beacon: Beacon{ImmutableFileNumber: slot / 100},
	})
	f.states[hash] = slot
}

func (f *reimportFixture) config(t *testing.T, localTip uint64) ReimportConfig {
	t.Helper()
	return ReimportConfig{
		Bootstrap: BootstrapConfig{
			Network: "preview", GenesisVerificationKey: f.nodeCfg.MithrilGenesisVerificationKey,
		},
		DB:                f.db,
		CardanoNodeConfig: f.nodeCfg,
		UtxoSet:           f.set,
		LocalTip:          ocommon.Point{Slot: localTip, Hash: stateHash(localTip)},
		OnChain: func(_ context.Context, p ocommon.Point) (bool, error) {
			return f.onChain[p.Slot], nil
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		list: func(
			context.Context, BootstrapConfig, string,
		) ([]CardanoDatabaseSnapshotListItem, error) {
			return f.artifact, nil
		},
		bootstrap: func(
			_ context.Context, cfg BootstrapConfig,
		) (*BootstrapResult, error) {
			f.pinned = append(f.pinned, cfg.PinnedDigest)
			f.starts = append(f.starts, cfg.StartImmutable)
			slot := f.states[cfg.PinnedDigest]
			return verifiedAncillaryStateAt(
				t, strconv.FormatUint(slot, 10),
				minimalLedgerState(t, slot, stateHash(slot)),
			), nil
		},
	}
}

// TestReimportLedgerStateImportsTheNewestStateOnTheLocalChain requires a
// re-import to skip a state that is not a block of the local chain, load the
// next one into the drained database's set, download no immutable file, and
// leave the latch and the marks as an import does.
func TestReimportLedgerStateImportsTheNewestStateOnTheLocalChain(t *testing.T) {
	t.Parallel()
	f := newReimportFixture(t)
	f.addArtifact("fork", 2200)
	f.addArtifact("good", 2000)
	f.addArtifact("older", 1800)
	f.onChain[2000], f.onChain[1800] = true, true
	latch, err := f.db.DrainedFamilies(t.Context())
	require.NoError(t, err)

	point, err := ReimportLedgerState(t.Context(), f.config(t, 2500))
	require.NoError(t, err)

	require.Equal(t, uint64(2000), point.Slot)
	require.Equal(t, []string{"fork", "good"}, f.pinned, "newest first, stopping at the first usable state")
	require.Equal(t, []uint64{23, 21}, f.starts, "no immutable file is downloaded")
	tip, ok, err := f.set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(2000), tip.Slot)
	sqlTip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), sqlTip.Point.Slot)
	mask, err := f.db.DrainedFamilies(t.Context())
	require.NoError(t, err)
	require.Equal(t, latch, mask, "the latch must be kept")
	_, pending, err := f.db.DrainedImportPending(t.Context())
	require.NoError(t, err)
	require.False(t, pending)
	for _, key := range []string{"sync_status", database.LedgerReimportSyncKey} {
		v, err := f.db.GetSyncState(key, nil)
		require.NoError(t, err)
		require.Empty(t, v, key)
	}
	boundary, err := f.db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "2000", boundary)
}

// TestReimportLedgerStateNamesAMissingState requires an error that says no
// artifact holds a usable state, with nothing written and the marks cleared.
func TestReimportLedgerStateNamesAMissingState(t *testing.T) {
	t.Parallel()
	f := newReimportFixture(t)
	f.addArtifact("fork", 2200)

	_, err := ReimportLedgerState(t.Context(), f.config(t, 2500))
	require.ErrorIs(t, err, ErrNoReimportState)
	require.NotContains(t, err.Error(), "resync")
	tip, _, err := f.set.Tip()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), tip.Slot, "the set must be untouched")
	for _, key := range []string{"sync_status", database.LedgerReimportSyncKey} {
		v, err := f.db.GetSyncState(key, nil)
		require.NoError(t, err)
		require.Empty(t, v, key)
	}
}

// TestReimportLedgerStateSkipsAnArtifactAboveTheLocalTipBeforeDownloading
// requires an artifact whose beacon epoch is past the local tip's to be passed
// over without a download, and the next one to be imported.
func TestReimportLedgerStateSkipsAnArtifactAboveTheLocalTipBeforeDownloading(t *testing.T) {
	t.Parallel()
	f := newReimportFixture(t)
	f.addArtifact("above", 4000)
	f.artifact[0].Beacon.Epoch = 4
	f.addArtifact("good", 2000)
	f.artifact[1].Beacon.Epoch = 2
	f.onChain[2000] = true
	cfg := f.config(t, 2500)
	cfg.EpochOf = func(slot uint64) (uint64, error) { return slot / 1000, nil }

	point, err := ReimportLedgerState(t.Context(), cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), point.Slot)
	require.Equal(t, []string{"good"}, f.pinned, "the artifact above the tip must not be downloaded")
}

func (f *reimportFixture) storeBlocks(t *testing.T, slots ...uint64) {
	t.Helper()
	for i, slot := range slots {
		prev := slot - 100
		if i == 0 {
			prev = slot - 1
		}
		require.NoError(t, f.db.BlockCreate(models.Block{
			Slot: slot, Hash: stateHash(slot), PrevHash: stateHash(prev),
			Cbor: []byte{0x80}, Number: slot, Type: 6,
		}, nil))
	}
}

// TestReimportLedgerStateChecksTheBlocksBelowTheTrustBoundary requires a state
// below a later Mithril trust boundary to be passed over when a block between
// it and the boundary was pruned, and the next state to be loaded when the
// blocks up to the boundary are stored.
func TestReimportLedgerStateChecksTheBlocksBelowTheTrustBoundary(t *testing.T) {
	t.Parallel()
	f := newReimportFixture(t)
	require.NoError(t, setMithrilLedgerAnchor(
		f.db, ocommon.Point{Slot: 2200, Hash: stateHash(2200)},
	))
	f.storeBlocks(t, 2100, 2200)
	f.addArtifact("pruned", 2000)
	f.addArtifact("kept", 2100)
	f.onChain[2000], f.onChain[2100] = true, true

	point, err := ReimportLedgerState(t.Context(), f.config(t, 2500))
	require.NoError(t, err)
	require.Equal(t, uint64(2100), point.Slot)
	require.Equal(t, []string{"pruned", "kept"}, f.pinned)
	boundary, err := f.db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "2200", boundary, "the later boundary stays")
}

// TestReimportLedgerStateNamesPrunedBlocksBelowTheTrustBoundary requires the
// error to say which blocks are gone, leave the set untouched, and not suggest
// a resync.
func TestReimportLedgerStateNamesPrunedBlocksBelowTheTrustBoundary(t *testing.T) {
	t.Parallel()
	f := newReimportFixture(t)
	require.NoError(t, setMithrilLedgerAnchor(
		f.db, ocommon.Point{Slot: 2200, Hash: stateHash(2200)},
	))
	f.storeBlocks(t, 2100, 2200)
	f.addArtifact("pruned", 2000)
	f.onChain[2000] = true

	_, err := ReimportLedgerState(t.Context(), f.config(t, 2500))
	require.ErrorIs(t, err, ErrNoReimportState)
	require.ErrorIs(t, err, errBlocksNotStored)
	require.NotContains(t, err.Error(), "resync")
	tip, _, err := f.set.Tip()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), tip.Slot, "the set must be untouched")
}

// TestReimportLedgerStateRecordsTheSwapBeforeItFinishes requires a re-import
// to clear the swap mark of an earlier run before it writes, and to record the
// imported point once its set is current, so a run that stops before finishing
// is resumed from the mark rather than from the set's tip alone.
func TestReimportLedgerStateRecordsTheSwapBeforeItFinishes(t *testing.T) {
	t.Parallel()
	f := newReimportFixture(t)
	f.addArtifact("good", 2000)
	f.onChain[2000] = true
	require.NoError(t, f.db.SetSyncState(
		database.LedgerReimportSwappedSyncKey, "stale", nil,
	))
	cfg := f.config(t, 2500)
	var seenInCommit string
	cfg.Commit = func(ctx context.Context, p ocommon.Point) error {
		var err error
		seenInCommit, err = f.db.GetSyncState(database.LedgerReimportSwappedSyncKey, nil)
		if err != nil {
			return err
		}
		// A tip that is not the imported point makes the run stop at
		// finishing, with its marks left set.
		return f.db.SetTip(ochainsync.Tip{Point: ocommon.Point{Slot: 1, Hash: stateHash(1)}}, nil)
	}

	_, err := ReimportLedgerState(t.Context(), cfg)

	require.ErrorContains(t, err, "does not match the ledger state")
	require.Empty(t, seenInCommit, "a mark of an earlier run is cleared before the swap")
	got, err := f.db.GetSyncState(database.LedgerReimportSwappedSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, database.LedgerReimportPointMark(ocommon.Point{
		Slot: 2000, Hash: stateHash(2000),
	}), got)
}
