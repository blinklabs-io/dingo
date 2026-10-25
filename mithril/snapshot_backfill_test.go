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
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/gouroboros/cbor"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshotChunkSlots lays the fixture's files out 100 slots apart, two blocks
// at the end of each: file n holds slots 100n+98 and 100n+99.
const snapshotChunkSlots = 100

func TestSnapshotProbesCloseKeepsSubstitutedDirectory(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	path := filepath.Join(parent, "probe")
	require.NoError(t, os.MkdirAll(filepath.Join(path, "extract"), 0o700))
	root, err := openVerifiedDir(path)
	require.NoError(t, err)
	parentRoot, err := os.OpenRoot(parent)
	require.NoError(t, err)
	info, err := root.Stat(".")
	require.NoError(t, err)
	extractRoot, err := os.OpenRoot(filepath.Join(path, "extract"))
	require.NoError(t, err)
	probes := &snapshotProbes{
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		storeDir: path,
		store: &bootstrapDir{
			parent: parentRoot, root: root, info: info,
			name: "probe", path: path,
		},
		storeRoot: extractRoot,
	}
	moved := path + ".moved"
	require.NoError(t, os.Rename(path, moved))
	require.NoError(t, os.Mkdir(path, 0o700))
	canary := filepath.Join(path, "keep")
	require.NoError(t, os.WriteFile(canary, []byte("keep"), 0o600))

	probes.close()
	data, err := os.ReadFile(canary)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), data)
}

// snapshotFixture is a ten-file certified chain whose ancillary ledger state
// sits on the last block of the last file.
type snapshotFixture struct {
	*v2Fixture
	chain  streamChain
	ledger ocommon.Point
}

func newSnapshotFixture(
	t *testing.T,
	mutate func(*v2FixtureOptions),
) snapshotFixture {
	t.Helper()
	opts := v2FixtureOptions{
		immutableFileNumber: 9,
		validAllImmutables:  true,
		chunkSlots:          snapshotChunkSlots,
	}
	if mutate != nil {
		mutate(&opts)
	}
	chain := readStreamChain(t, opts)
	ledger := chain.last[opts.immutableFileNumber]
	opts.ancillaryLedgerState = minimalLedgerStateWithUTxOMap(
		t, ledger.Slot, ledger.Hash, singleUTxOMap(t),
	)
	opts.ancillaryLedgerSlot = ledger.Slot
	return snapshotFixture{
		v2Fixture: newV2Fixture(t, opts),
		chain:     chain,
		ledger:    ledger,
	}
}

func (f snapshotFixture) config(
	t *testing.T,
	db *database.Database,
	stabilityWindow uint64,
) SnapshotConfig {
	t.Helper()
	bcfg := f.bootstrapConfig("")
	bcfg.Logger = slog.New(slog.DiscardHandler)
	return SnapshotConfig{
		Feed: ChunkFeedConfig{
			Bootstrap:    bcfg,
			WindowChunks: 2,
		},
		DB:              db,
		StabilityWindow: stabilityWindow,
		CardanoNodeConfig: testNodeConfigWithMithrilKeys(
			t, f.genesisVKey, f.ancillaryVKey,
		),
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.DiscardHandler),
		fetchGap: func(
			context.Context, *slog.Logger, string, ocommon.Point, ocommon.Point,
		) ([]models.Block, error) {
			return nil, errors.New("the fixture's ledger state needs no gap")
		},
	}
}

// storedSlots returns the slot of every block in db, ascending.
func storedSlots(t *testing.T, db *database.Database) []uint64 {
	t.Helper()
	iter := db.BlocksInRange(0, ^uint64(0))
	defer iter.Close()
	var slots []uint64
	for {
		next, err := iter.NextRaw()
		require.NoError(t, err)
		if next == nil {
			return slots
		}
		slots = append(slots, next.Slot)
	}
}

func requireEmptyDB(t *testing.T, db *database.Database) {
	t.Helper()
	assert.Empty(t, storedSlots(t, db), "blocks were stored")
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	assert.Zero(t, tip.Point.Slot, "a ledger tip was recorded")
	assert.Empty(t, tip.Point.Hash, "a ledger tip was recorded")
	keys, err := db.ListSyncStateKeysByPrefix("", nil)
	require.NoError(t, err)
	assert.Empty(t, keys, "sync state was written")
	state, err := SnapshotStartupState(t.Context(), db)
	require.NoError(t, err)
	assert.Equal(t, SnapshotStartupFresh, state)
}

// History expiry keeps the blocks at or above ledger slot minus the stability
// window. With the ledger state at slot 999 and a window of 150 the cutoff is
// slot 849, inside file 7 (first block 798, file 8 starts at 898), so the
// restore probes 9, 8 and 7 and never asks for anything below 7.
func TestSnapshotBackfillSkipsExpiredHistory(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)

	tip, err := RunSnapshotBackfill(t.Context(), f.config(t, db, 150))
	require.NoError(t, err)
	assert.True(t, samePoint(f.ledger, tip), "returned the ledger point")

	requested := f.requestedImmutables()
	require.NotEmpty(t, requested)
	assert.GreaterOrEqual(t, slices.Min(requested), uint64(7),
		"requested a file wholly below the retention cutoff: %v", requested)
	for _, want := range []uint64{7, 8, 9} {
		assert.Contains(t, requested, want)
	}

	var want []uint64
	for _, p := range f.chain.points {
		if f.chain.chunkOf(p) >= 7 {
			want = append(want, p.Slot)
		}
	}
	assert.Equal(t, want, storedSlots(t, db))
}

func TestSnapshotBackfillSetsTrustBoundary(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)

	_, err := RunSnapshotBackfill(t.Context(), f.config(t, db, 150))
	require.NoError(t, err)

	slot, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	assert.Equal(t, strconv.FormatUint(f.ledger.Slot, 10), slot)
	bootstrapped, err := WasBootstrapped(db)
	require.NoError(t, err)
	assert.True(t, bootstrapped)
	requireMarker(t, db, 9)

	status, err := db.GetSyncState("sync_status", nil)
	require.NoError(t, err)
	assert.Empty(t, status, "the restore did not complete")
	for _, key := range []string{
		syncKeySnapshotRestoreActive, syncKeyPinnedArtifact,
	} {
		val, err := db.GetSyncState(key, nil)
		require.NoError(t, err)
		assert.Empty(t, val, "%s outlived the restore", key)
	}
	ledgerTip, err := db.GetTip(nil)
	require.NoError(t, err)
	assert.True(t, samePoint(f.ledger, ledgerTip.Point), "ledger tip")

	state, err := SnapshotStartupState(t.Context(), db)
	require.NoError(t, err)
	assert.Equal(t, SnapshotStartupPopulated, state)
}

// A Core restore stores no produced-UTxO offsets, which Core resolution never
// reads, so the completion marker must stay unset while the restore runs: an
// API backfill that found it set would skip building them.
func TestSnapshotBackfillLeavesCoreUtxoOffsetMarkerUnset(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)
	require.Equal(t, types.StorageModeCore, db.StorageMode())

	cfg := f.config(t, db, 150)
	checked := false
	cfg.OnProgress = func(p SyncProgress) {
		if checked || p.Phase != PhaseIndexRebuild || !p.Active {
			return
		}
		checked = true
		_, ok, err := node.ImmutableUtxoOffsetsTipSlot(db)
		assert.NoError(t, err)
		assert.False(t, ok, "a Core restore recorded immutable UTxO offsets")
	}
	_, err := RunSnapshotBackfill(t.Context(), cfg)
	require.NoError(t, err)
	require.True(t, checked, "the restore never rebuilt indexes")
}

// A manifest signed with the wrong key fails the restore before anything is
// written, so the node can sync the still-empty database from genesis.
func TestSnapshotAncillaryBadSignature(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, func(o *v2FixtureOptions) {
		o.badAncillarySig = true
	})
	db := newSyncModeTestDB(t)

	_, err := RunSnapshotBackfill(t.Context(), f.config(t, db, 150))
	require.Error(t, err)
	assert.ErrorContains(t, err, "signature")
	requireEmptyDB(t, db)
}

// A cutoff below the first certified block keeps the whole certified range.
func TestSnapshotBackfillWindowCoversAllHistory(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)

	_, err := RunSnapshotBackfill(t.Context(), f.config(t, db, 5000))
	require.NoError(t, err)
	want := make([]uint64, 0, len(f.chain.points))
	for _, p := range f.chain.points {
		want = append(want, p.Slot)
	}
	assert.Equal(t, want, storedSlots(t, db))
}

// When the cutoff lies further down than the probe cap reaches, the files
// below the lowest probe come through a windowed feed from file 0.
func TestSnapshotBackfillProbeCapLoadsFromFirstFile(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, func(o *v2FixtureOptions) {
		o.immutableFileNumber = maxCutoffProbes + 3
	})
	db := newSyncModeTestDB(t)

	_, err := RunSnapshotBackfill(
		t.Context(), f.config(t, db, f.ledger.Slot),
	)
	require.NoError(t, err)
	want := make([]uint64, 0, len(f.chain.points))
	for _, p := range f.chain.points {
		want = append(want, p.Slot)
	}
	assert.Equal(t, want, storedSlots(t, db))
}

// An empty file is probed past, not taken for the cutoff: file 8 holds no
// blocks, so the cutoff at slot 849 is still found in file 7.
func TestSnapshotBackfillSkipsEmptyFiles(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, func(o *v2FixtureOptions) {
		o.blocksPerChunk = func(num uint64) int {
			if num == 8 {
				return 0
			}
			return 2
		}
	})
	db := newSyncModeTestDB(t)

	_, err := RunSnapshotBackfill(t.Context(), f.config(t, db, 150))
	require.NoError(t, err)
	assert.Equal(t, uint64(7), slices.Min(f.requestedImmutables()))
	var want []uint64
	for _, p := range f.chain.points {
		if f.chain.chunkOf(p) >= 7 {
			want = append(want, p.Slot)
		}
	}
	assert.Equal(t, want, storedSlots(t, db))
}

// An interrupted restore leaves the database marked as the node's own and
// resumes against the artifact it pinned, even after the aggregator moves on.
func TestSnapshotBackfillResumesInterruptedImport(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := f.config(t, db, 150)
	interrupted := false
	cfg.OnProgress = func(p SyncProgress) {
		if !interrupted && p.Phase == PhaseLedgerImport && p.Active &&
			p.Description == "utxo" && p.Count > 0 {
			interrupted = true
			cancel()
		}
	}
	_, err := RunSnapshotBackfill(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, interrupted, "the restore never reached the ledger import")

	state, err := SnapshotStartupState(t.Context(), db)
	require.NoError(t, err)
	require.Equal(t, SnapshotStartupResume, state)
	interruptedOwn, err := SnapshotRestoreInterrupted(db)
	require.NoError(t, err)
	require.True(t, interruptedOwn)

	f.publishNewerArtifact(t)
	cfg.OnProgress = nil
	tip, err := RunSnapshotBackfill(t.Context(), cfg)
	require.NoError(t, err)
	assert.True(t, samePoint(f.ledger, tip))
	status, err := db.GetSyncState("sync_status", nil)
	require.NoError(t, err)
	assert.Empty(t, status)
	var want []uint64
	for _, p := range f.chain.points {
		if f.chain.chunkOf(p) >= 7 {
			want = append(want, p.Slot)
		}
	}
	assert.Equal(t, want, storedSlots(t, db), "blocks stored twice or lost")
}

// A database with anything in it is never restored over.
func TestSnapshotBackfillRefusesPopulatedDatabase(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: f.ledger}, nil))

	state, err := SnapshotStartupState(t.Context(), db)
	require.NoError(t, err)
	require.Equal(t, SnapshotStartupPopulated, state)
	_, err = RunSnapshotBackfill(t.Context(), f.config(t, db, 150))
	require.Error(t, err)
	assert.Empty(t, f.requestedImmutables())
}

func TestSnapshotStartupState(t *testing.T) {
	t.Parallel()
	db := newSyncModeTestDB(t)
	state, err := SnapshotStartupState(t.Context(), db)
	require.NoError(t, err)
	assert.Equal(t, SnapshotStartupFresh, state)

	// A manual sync's interruption is not the node's to resume.
	require.NoError(t, db.SetSyncState("sync_status", syncStatusInProgress, nil))
	state, err = SnapshotStartupState(t.Context(), db)
	require.NoError(t, err)
	assert.Equal(t, SnapshotStartupPopulated, state)
	own, err := SnapshotRestoreInterrupted(db)
	require.NoError(t, err)
	assert.False(t, own)

	require.NoError(t, db.SetSyncState(syncKeySnapshotRestoreActive, "1", nil))
	state, err = SnapshotStartupState(t.Context(), db)
	require.NoError(t, err)
	assert.Equal(t, SnapshotStartupResume, state)
}

// utxoMapWith is singleUTxOMap with the transaction id filled with seed.
func utxoMapWith(t *testing.T, seed byte) cbor.RawMessage {
	t.Helper()
	txIn := append(bytes.Repeat([]byte{seed}, 32), 0, 0)
	key, err := cbor.Encode(txIn)
	require.NoError(t, err)
	address := append([]byte{0x60}, bytes.Repeat([]byte{0x11}, 28)...)
	value, err := cbor.Encode([]any{address, uint64(1_000_000)})
	require.NoError(t, err)
	return append(append(cbor.RawMessage{0xa1}, key...), value...)
}

// newLaterSnapshotFixture is a second aggregator certifying the same chain
// up to file last, with its ledger state on the new last block and a
// UTxO set holding only the output with transaction id seed. The first
// fixture's artifact is not served there, as after an aggregator prunes it.
func newLaterSnapshotFixture(
	t *testing.T,
	last uint64,
	seed byte,
) snapshotFixture {
	t.Helper()
	return newChainSnapshotFixture(t, last, snapshotChunkSlots, seed)
}

// newChainSnapshotFixture is newLaterSnapshotFixture with files chunkSlots
// slots apart, for chains the minimal ledger state's 2000-slot first epoch
// cannot hold at the usual spacing.
func newChainSnapshotFixture(
	t *testing.T,
	last uint64,
	chunkSlots uint64,
	seed byte,
) snapshotFixture {
	t.Helper()
	opts := v2FixtureOptions{
		immutableFileNumber: last,
		validAllImmutables:  true,
		chunkSlots:          chunkSlots,
	}
	chain := readStreamChain(t, opts)
	ledger := chain.last[last]
	opts.ancillaryLedgerState = minimalLedgerStateWithUTxOMap(
		t, ledger.Slot, ledger.Hash, utxoMapWith(t, seed),
	)
	opts.ancillaryLedgerSlot = ledger.Slot
	return snapshotFixture{
		v2Fixture: newV2Fixture(t, opts),
		chain:     chain,
		ledger:    ledger,
	}
}

func requireRestoreComplete(
	t *testing.T,
	db *database.Database,
	f snapshotFixture,
	tip ocommon.Point,
	firstFile uint64,
) {
	t.Helper()
	assert.True(t, samePoint(f.ledger, tip), "restored ledger point")
	status, err := db.GetSyncState("sync_status", nil)
	require.NoError(t, err)
	assert.Empty(t, status)
	var want []uint64
	for _, p := range f.chain.points {
		if f.chain.chunkOf(p) >= firstFile {
			want = append(want, p.Slot)
		}
	}
	assert.Equal(t, want, storedSlots(t, db), "blocks stored twice or lost")
}

// interruptDuringBlockLoad runs a restore of f into db and cancels it once
// it has stored blocks, leaving an interrupted restore in the blocks phase.
func interruptDuringBlockLoad(
	t *testing.T,
	f snapshotFixture,
	db *database.Database,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := f.config(t, db, 150)
	interrupted := false
	cfg.OnProgress = func(p SyncProgress) {
		if !interrupted && p.Phase == PhaseImmutableCopy && p.Active &&
			p.Count > 0 {
			interrupted = true
			cancel()
		}
	}
	_, err := RunSnapshotBackfill(ctx, cfg)
	require.Error(t, err)
	require.True(t, interrupted, "the restore never stored a block")
	phase, err := db.GetSyncState(syncKeySnapshotRestoreActive, nil)
	require.NoError(t, err)
	require.Equal(t, restorePhaseBlocks, phase)
	require.NotEmpty(t, storedSlots(t, db))
}

// The artifact an interrupted restore pinned can be pruned before it
// resumes. With only blocks stored, the restore discards them and starts
// afresh on the latest artifact, from that artifact's own cutoff.
func TestSnapshotBackfillRepinsBeforeLedgerImport(t *testing.T) {
	t.Parallel()
	a := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)
	interruptDuringBlockLoad(t, a, db)

	b := newLaterSnapshotFixture(t, 11, 0x40)
	cfg := b.config(t, db, 150)
	tip, err := RunSnapshotBackfill(t.Context(), cfg)
	require.NoError(t, err)
	// Ledger at slot 1199, cutoff 1049: file 9 is the first kept.
	requireRestoreComplete(t, db, b, tip, 9)
	assert.GreaterOrEqual(t, slices.Min(b.requestedImmutables()), uint64(9))
}

// A re-pin whose stored blocks lie far below the latest artifact's cutoff,
// beyond the probe bound, still downloads nothing below that cutoff.
func TestSnapshotBackfillRepinFarBelowCutoff(t *testing.T) {
	t.Parallel()
	a := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)
	interruptDuringBlockLoad(t, a, db)

	// The stored tip is at slot 798; the new target, file 25, is 18 files
	// above the file of that slot. Files are 70 slots apart: the ledger
	// state is at slot 1819 and the cutoff at 1669, so file 22 (first
	// block 1608) is the first kept.
	c := newChainSnapshotFixture(t, 25, 70, 0x40)
	tip, err := RunSnapshotBackfill(t.Context(), c.config(t, db, 150))
	require.NoError(t, err)
	requireRestoreComplete(t, db, c, tip, 22)
	assert.GreaterOrEqual(
		t, slices.Min(c.requestedImmutables()), uint64(22),
		"downloaded below the new cutoff: %v", c.requestedImmutables(),
	)
}

// With the ledger import begun, a pruned pin is terminal: the rows already
// imported belong to that artifact, so nothing is imported over them.
func TestSnapshotBackfillPrunedPinAfterLedgerImportIsUnrecoverable(
	t *testing.T,
) {
	t.Parallel()
	a := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := a.config(t, db, 150)
	interrupted := false
	cfg.OnProgress = func(p SyncProgress) {
		if !interrupted && p.Phase == PhaseLedgerImport && p.Active &&
			p.Description == "utxo" && p.Count > 0 {
			interrupted = true
			cancel()
		}
	}
	_, err := RunSnapshotBackfill(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, interrupted)
	oldTx := bytes.Repeat([]byte{0x40}, 32)
	before, err := db.Metadata().GetUtxo(oldTx, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, before, "the interrupted import wrote no output")

	b := newLaterSnapshotFixture(t, 11, 0x41)
	cfg = b.config(t, db, 150)
	imported := false
	cfg.OnProgress = func(p SyncProgress) {
		if p.Phase == PhaseLedgerImport {
			imported = true
		}
	}
	_, err = RunSnapshotBackfill(t.Context(), cfg)
	require.ErrorIs(t, err, ErrSnapshotRestoreUnrecoverable)
	assert.False(t, imported, "a ledger import was attempted")
	assert.Empty(t, b.requestedImmutables(), "the restore went on")
	phase, err := db.GetSyncState(syncKeySnapshotRestoreActive, nil)
	require.NoError(t, err)
	assert.Equal(t, restorePhaseLedger, phase)
	after, err := db.Metadata().GetUtxo(oldTx, 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, after, "the partial import was touched")
}

// A resume against its own artifact keeps stored blocks only once they are
// found on its certified chain, whether a certified block holds the stored
// tip's slot or the tip lies above every certified block, where nothing is
// loaded onto it.
func TestSnapshotBackfillResumeRefusesForeignBlocks(t *testing.T) {
	t.Parallel()
	for _, slot := range []uint64{799, 1500} {
		t.Run(strconv.FormatUint(slot, 10), func(t *testing.T) {
			t.Parallel()
			b := newLaterSnapshotFixture(t, 11, 0x40)
			db := newSyncModeTestDB(t)
			require.NoError(t, db.BlockCreate(models.Block{
				Slot: slot, Hash: bytes.Repeat([]byte{0xee}, 32),
				PrevHash: bytes.Repeat([]byte{0}, 32), Cbor: []byte{0x80},
				Number: 1, Type: 2,
			}, nil))
			require.NoError(t, setPinnedArtifact(db, pinnedArtifact{
				Backend:             BackendV2,
				Network:             b.artifact.Network,
				Digest:              b.artifact.Hash,
				Epoch:               b.artifact.Beacon.Epoch,
				ImmutableFileNumber: b.artifact.Beacon.ImmutableFileNumber,
				CertificateHash:     b.artifact.CertificateHash,
			}))
			require.NoError(t, db.SetSyncState(
				syncKeySnapshotRestoreActive, restorePhaseBlocks, nil,
			))
			require.NoError(t, db.SetSyncState(
				"sync_status", syncStatusInProgress, nil,
			))

			_, err := RunSnapshotBackfill(
				t.Context(), b.config(t, db, 150),
			)
			require.ErrorIs(t, err, errStoredChainOffCertified)
		})
	}
}

// The progress token moves with the phase, the stored tip and the ledger
// import checkpoint, and with nothing else.
func TestSnapshotRestoreProgress(t *testing.T) {
	t.Parallel()
	db := newSyncModeTestDB(t)
	progress := func() string {
		token, err := SnapshotRestoreProgress(t.Context(), db)
		require.NoError(t, err)
		return token
	}
	prev := progress()
	assert.Equal(t, prev, progress(), "token moved without progress")
	step := func(name string, change func()) {
		change()
		next := progress()
		assert.NotEqual(t, prev, next, name)
		prev = next
	}
	step("phase", func() {
		require.NoError(t, db.SetSyncState(
			syncKeySnapshotRestoreActive, restorePhaseBlocks, nil,
		))
	})
	step("stored block", func() {
		require.NoError(t, db.BlockCreate(models.Block{
			Slot: 10, Hash: bytes.Repeat([]byte{1}, 32),
			PrevHash: bytes.Repeat([]byte{0}, 32), Cbor: []byte{0x80},
			Number: 1, Type: 2,
		}, nil))
	})
	step("import checkpoint", func() {
		require.NoError(t, db.SetSyncState(
			syncKeySnapshotImportKey, "abcd:10", nil,
		))
		require.NoError(t, db.Metadata().SetImportCheckpoint(
			&models.ImportCheckpoint{
				ImportKey: "abcd:10", Phase: models.ImportPhaseUTxO,
			}, nil,
		))
	})
	assert.Equal(t, prev, progress())
}

// The restore's first three keys are written in one transaction: a failure
// inside it leaves none, and the database fresh.
func TestSnapshotBackfillFirstWriteIsAtomic(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)
	cfg := f.config(t, db, 150)
	crash := errors.New("crash between the first writes")
	cfg.firstWrite = func(*database.Txn) error { return crash }
	_, err := RunSnapshotBackfill(t.Context(), cfg)
	require.ErrorIs(t, err, crash)
	requireEmptyDB(t, db)
}

// A fresh database carrying restore keys alone, as an interruption between
// separate writes left them, has them cleared, so a later interrupted manual
// sync is not taken for the node's own restore.
func TestSnapshotBackfillClearsStrayRestoreKeys(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, func(o *v2FixtureOptions) {
		o.badAncillarySig = true
	})
	db := newSyncModeTestDB(t)
	require.NoError(t, db.SetSyncState(
		syncKeyPinnedArtifact,
		`{"backend":"v2","network":"preprod","digest":"`+
			strings.Repeat("ab", 32)+`"}`, nil,
	))
	require.NoError(t, db.SetSyncState(
		syncKeySnapshotRestoreActive, restorePhaseBlocks, nil,
	))
	state, err := SnapshotStartupState(t.Context(), db)
	require.NoError(t, err)
	require.Equal(t, SnapshotStartupFresh, state)

	_, err = RunSnapshotBackfill(t.Context(), f.config(t, db, 150))
	require.Error(t, err)
	requireEmptyDB(t, db)

	require.NoError(t, db.SetSyncState("sync_status", syncStatusInProgress, nil))
	own, err := SnapshotRestoreInterrupted(db)
	require.NoError(t, err)
	assert.False(t, own, "a manual sync's interruption read as the node's")
}

// chunkFilesUnder counts the immutable chunk files in the feed and probe
// directories beneath dir. The ancillary archive's in-progress trio is not
// one of them.
func chunkFilesUnder(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".chunk" &&
			strings.Contains(path, chunkFeedDirPrefix) {
			n++
		}
		return nil
	})
	require.NoError(t, err)
	return n
}

// Probed files wait for the load on disk, never more than the window at a
// time, rather than in memory.
func TestSnapshotBackfillKeepsProbesOnDisk(t *testing.T) {
	t.Parallel()
	f := newSnapshotFixture(t, nil)
	db := newSyncModeTestDB(t)
	cfg := f.config(t, db, 150)
	downloadDir := filepath.Join(cfg.DataDir, snapshotDownloadSubdir)
	var mu sync.Mutex
	peak := 0
	f.immutableServe = func(uint64) {
		n := chunkFilesUnder(t, downloadDir)
		mu.Lock()
		peak = max(peak, n)
		mu.Unlock()
	}
	afterProbes := -1
	cfg.afterProbes = func() { afterProbes = chunkFilesUnder(t, downloadDir) }

	_, err := RunSnapshotBackfill(t.Context(), cfg)
	require.NoError(t, err)
	// Files 9, 8 and 7 were probed; the window keeps the lowest two.
	assert.Equal(t, 2, afterProbes, "probed files kept on disk")
	mu.Lock()
	assert.LessOrEqual(t, peak, 2, "trios on disk exceeded the window")
	mu.Unlock()
	assert.Zero(t, chunkFilesUnder(t, downloadDir), "trios left behind")
	var want []uint64
	for _, p := range f.chain.points {
		if f.chain.chunkOf(p) >= 7 {
			want = append(want, p.Slot)
		}
	}
	assert.Equal(t, want, storedSlots(t, db))
}

// interruptDuringLedgerImport runs a restore of f into db and cancels it once
// the ledger import has written outputs, leaving an interrupted restore in
// the ledger phase.
func interruptDuringLedgerImport(
	t *testing.T,
	f snapshotFixture,
	db *database.Database,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cfg := f.config(t, db, 150)
	interrupted := false
	cfg.OnProgress = func(p SyncProgress) {
		if !interrupted && p.Phase == PhaseLedgerImport && p.Active &&
			p.Description == "utxo" && p.Count > 0 {
			interrupted = true
			cancel()
		}
	}
	_, err := RunSnapshotBackfill(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, interrupted)
	phase, err := db.GetSyncState(syncKeySnapshotRestoreActive, nil)
	require.NoError(t, err)
	require.Equal(t, restorePhaseLedger, phase)
}

// Only the aggregator answering 404 for the pinned artifact itself, twice,
// makes a ledger-phase resume unrecoverable. A 404 for a certificate, a 404
// that does not repeat, and an aggregator failing with server errors are
// the source being unavailable: the resume fails retryably, and a later one
// finishes the restore.
func TestSnapshotBackfillLedgerResumeOutageIsNotPruning(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		fail func(w http.ResponseWriter, r *http.Request, artifact string) bool
	}{
		{"certificate 404", func(
			w http.ResponseWriter, r *http.Request, _ string,
		) bool {
			if strings.HasPrefix(r.URL.Path, "/certificate/") {
				http.NotFound(w, r)
				return true
			}
			return false
		}},
		{"artifact 404 once", func() func(
			http.ResponseWriter, *http.Request, string,
		) bool {
			var served atomic.Bool
			return func(
				w http.ResponseWriter, r *http.Request, artifact string,
			) bool {
				if r.URL.Path == "/artifact/cardano-database/"+artifact &&
					served.CompareAndSwap(false, true) {
					http.NotFound(w, r)
					return true
				}
				return false
			}
		}()},
		{"aggregator 503", func(
			w http.ResponseWriter, _ *http.Request, _ string,
		) bool {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var failing atomic.Bool
			a := newSnapshotFixture(t, nil)
			a.serveHook = func(w http.ResponseWriter, r *http.Request) bool {
				return failing.Load() && tc.fail(w, r, a.artifact.Hash)
			}
			db := newSyncModeTestDB(t)
			interruptDuringLedgerImport(t, a, db)

			failing.Store(true)
			_, err := RunSnapshotBackfill(t.Context(), a.config(t, db, 150))
			require.Error(t, err)
			assert.NotErrorIs(t, err, ErrSnapshotRestoreUnrecoverable)
			assert.ErrorIs(t, err, ErrSnapshotRestoreUnavailable)
			phase, err := db.GetSyncState(syncKeySnapshotRestoreActive, nil)
			require.NoError(t, err)
			assert.Equal(t, restorePhaseLedger, phase)

			failing.Store(false)
			tip, err := RunSnapshotBackfill(t.Context(), a.config(t, db, 150))
			require.NoError(t, err)
			// Ledger at slot 999, cutoff 849: file 7 is the first kept.
			requireRestoreComplete(t, db, a, tip, 7)
		})
	}
}

// A digest mismatch is what the source answered, not its being unavailable,
// so it is not reported as ErrSnapshotRestoreUnavailable.
func TestSnapshotBackfillVerificationFailureIsNotUnavailable(t *testing.T) {
	t.Parallel()
	good := newSnapshotFixture(t, nil)
	opts := v2FixtureOptions{
		immutableFileNumber: 9,
		validAllImmutables:  true,
		chunkSlots:          snapshotChunkSlots,
		tamperDigestList:    true,
	}
	a := snapshotFixture{
		v2Fixture: newV2Fixture(t, opts),
		chain:     good.chain,
		ledger:    good.ledger,
	}
	_, err := RunSnapshotBackfill(
		t.Context(), a.config(t, newSyncModeTestDB(t), 150),
	)
	require.ErrorContains(t, err, "merkle")
	assert.NotErrorIs(t, err, ErrSnapshotRestoreUnavailable)
}
