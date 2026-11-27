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
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/internal/node"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// verifiedAncillaryStateAt returns a bootstrap result whose verified
// ancillary tree holds stateBytes as the ledger state in directory slot.
func verifiedAncillaryStateAt(
	t *testing.T,
	slot string,
	stateBytes []byte,
) *BootstrapResult {
	t.Helper()
	return verifiedAncillaryTree(t, map[string][]byte{
		"ledger/" + slot + "/state": stateBytes,
	})
}

// verifiedAncillaryTree returns a bootstrap result whose ancillary tree holds
// files, keyed by slash-separated path, each covered by a manifest digest.
func verifiedAncillaryTree(
	t *testing.T,
	files map[string][]byte,
) *BootstrapResult {
	t.Helper()
	anc := t.TempDir()
	digests := make(map[string]string, len(files))
	for name, content := range files {
		path := filepath.Join(anc, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, content, 0o640))
		sum := sha256.Sum256(content)
		digests[name] = hex.EncodeToString(sum[:])
	}
	ancRoot, err := openVerifiedDir(anc)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ancRoot.Close() })
	return &BootstrapResult{
		AncillaryDir:      anc,
		AncillaryRoot:     ancRoot,
		AncillaryVerified: true,
		AncillaryDigests:  digests,
	}
}

// ancillaryChunkAfter builds the immutable trio numbered num whose count
// blocks end at lastSlot and continue the chain from prev. It returns the trio
// keyed by extension, as the fixture takes it, and the block hashes in order.
func ancillaryChunkAfter(
	t *testing.T,
	lastSlot uint64,
	count int,
	prev []byte,
) (map[string][]byte, [][]byte) {
	t.Helper()
	var prevHash common.Blake2b256
	copy(prevHash[:], prev)
	files, hashes := validImmutableChunk(t, lastSlot, count, prevHash)
	trio := make(map[string][]byte, len(files))
	for name, content := range files {
		trio[filepath.Ext(name)[1:]] = content
	}
	return trio, hashes
}

// signedAncillary returns a verified ancillary tree holding trio as immutable
// file 1 and one ledger state per slot in states.
func signedAncillary(
	t *testing.T,
	trio map[string][]byte,
	states map[string][]byte,
) *BootstrapResult {
	t.Helper()
	files := make(map[string][]byte, len(trio)+len(states))
	for ext, content := range trio {
		files["immutable/00001."+ext] = content
	}
	for slot, state := range states {
		files["ledger/"+slot+"/state"] = state
	}
	return verifiedAncillaryTree(t, files)
}

// TestSyncLoadsLedgerStateAtSignedAncillaryChunkTip requires a bootstrap to
// store the blocks of the signed ancillary chunk after the certified ones and
// then load the ledger state taken at the last of them.
func TestSyncLoadsLedgerStateAtSignedAncillaryChunkTip(t *testing.T) {
	t.Parallel()

	_, certifiedHash := validImmutableFiles(t, 1000)
	trio, hashes := ancillaryChunkAfter(t, 1100, 3, certifiedHash)
	fixture := newV2Fixture(t, v2FixtureOptions{
		validImmutable:       true,
		ancillaryChunk:       trio,
		ancillaryLedgerState: minimalLedgerState(t, 1100, hashes[2]),
		ancillaryLedgerSlot:  1100,
	})
	set := newSyncTestSet(t)
	cfg := SyncConfig{
		UtxoSet: set,
		Network: "preprod",
		DataDir: t.TempDir(),
		CardanoNodeConfig: testNodeConfigWithMithrilKeys(
			t, fixture.genesisVKey, fixture.ancillaryVKey,
		),
		Backend:           BackendV2,
		AggregatorURL:     fixture.server.URL,
		AllowInsecureHTTP: true,
		VerifyCertChain:   true,
		Logger:            slog.New(slog.DiscardHandler),
	}
	result, err := Sync(context.Background(), cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(1100), result.LedgerSlot)

	setTip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1100), setTip.Slot)
	require.Equal(t, hashes[2], setTip.Hash)

	db, _ := syncedBlocks(t, cfg)
	require.Equal(
		t, []uint64{999, 1000, 1098, 1099, 1100}, storedSlots(t, db),
	)
	for i, slot := range []uint64{1098, 1099, 1100} {
		_, err := database.BlockByPoint(
			context.Background(), db, ocommon.NewPoint(slot, hashes[i]),
		)
		require.NoError(t, err, "slot %d", slot)
	}
}

// TestSyncRefusesLedgerStateAboveImmutableDBTip requires a bootstrap to load
// nothing when the only ledger state the ancillary archive carries is later
// than the last block the archive puts in the ImmutableDB.
func TestSyncRefusesLedgerStateAboveImmutableDBTip(t *testing.T) {
	t.Parallel()

	_, certifiedHash := validImmutableFiles(t, 1000)
	trio, _ := ancillaryChunkAfter(t, 1099, 2, certifiedHash)
	for name, chunk := range map[string]map[string][]byte{
		"state after the signed chunk":              trio,
		"no readable chunk after the certified tip": nil,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newV2Fixture(t, v2FixtureOptions{
				validImmutable: true,
				ancillaryChunk: chunk,
				ancillaryLedgerState: minimalLedgerState(
					t, 1100, bytes.Repeat([]byte{0x77}, 32),
				),
				ancillaryLedgerSlot: 1100,
			})
			set := newSyncTestSet(t)
			cfg := SyncConfig{
				UtxoSet: set,
				Network: "preprod",
				DataDir: t.TempDir(),
				CardanoNodeConfig: testNodeConfigWithMithrilKeys(
					t, fixture.genesisVKey, fixture.ancillaryVKey,
				),
				Backend:           BackendV2,
				AggregatorURL:     fixture.server.URL,
				AllowInsecureHTTP: true,
				VerifyCertChain:   true,
				Logger:            slog.New(slog.DiscardHandler),
			}
			_, err := Sync(context.Background(), cfg)
			require.ErrorIs(t, err, ErrLedgerStateTooRecent)
			_, loaded, err := set.Tip()
			require.NoError(t, err)
			require.False(t, loaded, "no ledger state may be loaded")
			runtime, err := openDatabase(
				context.Background(), cfg, slog.New(slog.DiscardHandler), 1,
			)
			require.NoError(t, err)
			status, err := runtime.Database.GetChainState("sync_status", nil)
			require.NoError(t, err)
			require.Empty(t, status, "a refused state must not leave the run to resume")
			require.NoError(t, runtime.Close(context.Background()))

			// The next run takes the aggregator's latest artifact.
			eligible, eligibleHashes := ancillaryChunkAfter(
				t, 1100, 3, certifiedHash,
			)
			next := newV2Fixture(t, v2FixtureOptions{
				validImmutable: true,
				ancillaryChunk: eligible,
				ancillaryLedgerState: minimalLedgerState(
					t, 1100, eligibleHashes[2],
				),
				ancillaryLedgerSlot: 1100,
			})
			cfg.UtxoSet = newSyncTestSet(t)
			cfg.CardanoNodeConfig = testNodeConfigWithMithrilKeys(
				t, next.genesisVKey, next.ancillaryVKey,
			)
			cfg.AggregatorURL = next.server.URL
			result, err := Sync(context.Background(), cfg)
			require.NoError(t, err)
			require.Equal(t, uint64(1100), result.LedgerSlot)
		})
	}
}

// TestPrepareLedgerStateImportFallsBackBelowImmutableTip requires selection
// to pass over a state later than the ImmutableDB tip for the newest one at or
// before it.
func TestPrepareLedgerStateImportFallsBackBelowImmutableTip(t *testing.T) {
	t.Parallel()

	olderHash := bytes.Repeat([]byte{0x11}, 32)
	result := signedAncillary(t, nil, map[string][]byte{
		"1000": minimalLedgerState(t, 1000, olderHash),
		"1100": minimalLedgerState(
			t, 1100, bytes.Repeat([]byte{0x22}, 32),
		),
	})
	prepared, err := prepareLedgerStateImport(
		slog.New(slog.DiscardHandler), result, 1050,
	)
	require.NoError(t, err)
	defer prepared.Close()
	require.Equal(t, ocommon.NewPoint(1000, olderHash), prepared.point())

	_, err = prepareLedgerStateImport(
		slog.New(slog.DiscardHandler), result, 999,
	)
	require.ErrorIs(t, err, ErrLedgerStateTooRecent)
	require.ErrorIs(t, err, ledgerstate.ErrNoUsableLedgerState)
}

// immutableBlocksIn returns the blocks of the immutable files under dir.
func immutableBlocksIn(t *testing.T, dir string) []immutable.Block {
	t.Helper()
	imm, err := immutable.New(dir)
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()
	var blocks []immutable.Block
	for {
		block, err := iter.Next()
		require.NoError(t, err)
		if block == nil {
			return blocks
		}
		blocks = append(blocks, *block)
	}
}

// TestRollbackAfterLoadAtAncillaryChunkTip covers a database that keeps its
// ImmutableDB chunk files. The signed ancillary chunk's blocks must be
// appended to those files, so the recorded ImmutableDB tip is the chunk's
// last block and the ledger state taken there loads. From then on a rollback
// to any block from that tip upwards is accepted, by the chain and by the
// live UTxO set, and a rollback below it is refused by both.
func TestRollbackAfterLoadAtAncillaryChunkTip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:   t.TempDir(),
		Logger:    logger,
		Immutable: database.ImmutableConfig{Enabled: true, ChunkSlots: 1000},
	})
	require.NoError(t, err)

	// The certified chunk, already in the ImmutableDB.
	certifiedFiles, certifiedHashes := validImmutableChunk(
		t, 999, 2, common.Blake2b256{},
	)
	certifiedDir := t.TempDir()
	for name, content := range certifiedFiles {
		require.NoError(t, os.WriteFile(
			filepath.Join(certifiedDir, filepath.Base(name)), content, 0o640,
		))
	}
	cm, err := chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	_, err = node.CopyImmutableBlocks(
		ctx, cm.PrimaryChain(), immutableBlocksIn(t, certifiedDir),
	)
	require.NoError(t, err)
	certifiedTip := ocommon.NewPoint(999, certifiedHashes[1])
	certifiedBlock, err := database.BlockByPoint(ctx, db, certifiedTip)
	require.NoError(t, err)
	_, err = db.PromoteThrough(ctx, certifiedBlock.ID)
	require.NoError(t, err)

	trio, hashes := ancillaryChunkAfter(t, 1100, 3, certifiedHashes[1])
	result := signedAncillary(t, trio, map[string][]byte{
		"1100": minimalLedgerState(t, 1100, hashes[2]),
	})
	blocks, err := readAncillaryChunk(logger, result, 0)
	require.NoError(t, err)
	require.Len(t, blocks, 3)
	immutableTip := ancillaryTip(certifiedTip, blocks)
	require.Equal(t, ocommon.NewPoint(1100, hashes[2]), immutableTip)

	// Until the chunk's blocks are in the ImmutableDB the state is refused.
	require.ErrorIs(
		t,
		requireStateInImmutableDB(ctx, db, immutableTip, immutableTip),
		ErrLedgerStateTooRecent,
	)

	unlock, err := db.LockImmutable(ctx)
	require.NoError(t, err)
	stored, err := landAncillaryChunk(ctx, db, blocks)
	unlock()
	require.NoError(t, err)
	require.Equal(t, 3, stored)

	st, ok := db.ImmutableState()
	require.True(t, ok)
	require.True(t, st.HasTip)
	require.Equal(t, immutableTip.Slot, st.TipSlot)
	require.Equal(t, immutableTip.Hash, st.TipHash)
	written := immutableBlocksIn(t, db.ImmutableDir())
	require.Len(t, written, 5)
	for i, hash := range hashes {
		require.Equal(t, hash, written[2+i].Hash)
		require.Equal(t, blocks[i].Cbor, written[2+i].Cbor)
	}

	set := newSyncTestSet(t)
	slot, hash, err := importLedgerState(
		ctx, db, logger, testNodeConfigWithMithrilKeys(t, "", ""), result,
		false, immutableTip.Slot, set, nil,
		func(prepared *preparedLedgerStateImport) error {
			return requireStateInImmutableDB(
				ctx, db, immutableTip, prepared.point(),
			)
		},
	)
	require.NoError(t, err)
	require.Equal(t, immutableTip, ocommon.NewPoint(slot, hash))
	setTip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, immutableTip, setTip)

	// Two volatile blocks above the ImmutableDB tip.
	cm, err = chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	volatile := []chain.RawBlock{{
		Slot: 1101, Hash: bytes.Repeat([]byte{0xa1}, 32), BlockNumber: 4,
		Type: uint(shelley.BlockTypeShelley), PrevHash: immutableTip.Hash,
		Cbor: []byte{0x80},
	}, {
		Slot: 1102, Hash: bytes.Repeat([]byte{0xa2}, 32), BlockNumber: 5,
		Type:     uint(shelley.BlockTypeShelley),
		PrevHash: bytes.Repeat([]byte{0xa1}, 32),
		Cbor:     []byte{0x80},
	}}
	require.NoError(t, c.AddRawBlocks(ctx, volatile))
	first := ocommon.NewPoint(volatile[0].Slot, volatile[0].Hash)
	second := ocommon.NewPoint(volatile[1].Slot, volatile[1].Hash)
	require.NoError(t, set.ApplyUtxoChanges(&first, nil))
	require.NoError(t, set.ApplyUtxoChanges(&second, nil))

	for _, target := range []ocommon.Point{first, immutableTip} {
		require.NoError(t, c.RollbackUnbounded(ctx, target))
		_, err := set.Rollback(target)
		require.NoError(t, err)
		setTip, _, err := set.Tip()
		require.NoError(t, err)
		require.Equal(t, target, setTip)
	}

	below := ocommon.NewPoint(1099, hashes[1])
	require.ErrorIs(
		t, c.RollbackUnbounded(ctx, below),
		database.ErrRollbackBelowImmutableTip,
	)
	_, err = set.Rollback(below)
	require.ErrorIs(t, err, badgerutxo.ErrRollbackBeyondJournal)
}

// TestLandAncillaryChunkPromotesCopiedBodies covers an import that copies
// block bodies into a database whose recorded ImmutableDB tip lags the
// certified chain. A state above the recorded tip is refused, and storing the
// signed chunk moves the recorded tip to the chunk's last block, so the state
// taken there loads.
func TestLandAncillaryChunkPromotesCopiedBodies(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:   t.TempDir(),
		Logger:    logger,
		Immutable: database.ImmutableConfig{Enabled: true, ChunkSlots: 1000},
	})
	require.NoError(t, err)

	certifiedFiles, certifiedHashes := validImmutableChunk(
		t, 999, 2, common.Blake2b256{},
	)
	certifiedDir := t.TempDir()
	for name, content := range certifiedFiles {
		require.NoError(t, os.WriteFile(
			filepath.Join(certifiedDir, filepath.Base(name)), content, 0o640,
		))
	}
	cm, err := chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	_, err = node.CopyImmutableBlocks(
		ctx, cm.PrimaryChain(), immutableBlocksIn(t, certifiedDir),
	)
	require.NoError(t, err)
	first, err := database.BlockByPoint(
		ctx, db, ocommon.NewPoint(998, certifiedHashes[0]),
	)
	require.NoError(t, err)
	_, err = db.PromoteThrough(ctx, first.ID)
	require.NoError(t, err)

	trio, hashes := ancillaryChunkAfter(t, 1100, 3, certifiedHashes[1])
	result := signedAncillary(t, trio, nil)
	blocks, err := readAncillaryChunk(logger, result, 0)
	require.NoError(t, err)
	immutableTip := ancillaryTip(
		ocommon.NewPoint(999, certifiedHashes[1]), blocks,
	)
	require.Equal(t, ocommon.NewPoint(1100, hashes[2]), immutableTip)

	// Stored as bodies only, the chunk's blocks are above the recorded tip.
	cm, err = chain.NewManager(ctx, db, nil)
	require.NoError(t, err)
	_, err = node.CopyImmutableBlocks(ctx, cm.PrimaryChain(), blocks)
	require.NoError(t, err)
	require.ErrorIs(
		t,
		requireStateInImmutableDB(ctx, db, immutableTip, immutableTip),
		ErrLedgerStateTooRecent,
	)

	unlock, err := db.LockImmutable(ctx)
	require.NoError(t, err)
	_, err = landAncillaryChunk(ctx, db, blocks)
	unlock()
	require.NoError(t, err)

	own, ok := db.ImmutablePoint()
	require.True(t, ok)
	require.Equal(t, immutableTip, own)
	require.NoError(
		t, requireStateInImmutableDB(ctx, db, immutableTip, immutableTip),
	)
}

// TestSnapshotBackfillLoadsStateAtSignedAncillaryChunkTip requires a restore
// for a node that expires history to store the signed ancillary chunk's
// blocks after the certified ones and load the ledger state taken at the last
// of them.
func TestSnapshotBackfillLoadsStateAtSignedAncillaryChunkTip(t *testing.T) {
	t.Parallel()

	opts := v2FixtureOptions{
		immutableFileNumber: 9,
		validAllImmutables:  true,
		chunkSlots:          snapshotChunkSlots,
	}
	certified := readStreamChain(t, opts)
	certifiedTip := certified.last[opts.immutableFileNumber]
	stateSlot := certifiedTip.Slot + 3
	trio, hashes := ancillaryChunkAfter(t, stateSlot, 3, certifiedTip.Hash)
	opts.ancillaryChunk = trio
	opts.ancillaryLedgerSlot = stateSlot
	opts.ancillaryLedgerState = minimalLedgerStateWithUTxOMap(
		t, stateSlot, hashes[2], singleUTxOMap(t),
	)
	f := snapshotFixture{v2Fixture: newV2Fixture(t, opts), chain: certified}
	db := newSyncModeTestDB(t)

	tip, err := RunSnapshotBackfill(t.Context(), f.config(t, db, 150))
	require.NoError(t, err)
	require.Equal(t, ocommon.NewPoint(stateSlot, hashes[2]), tip)

	stored := storedSlots(t, db)
	require.GreaterOrEqual(t, len(stored), 4)
	require.Equal(
		t,
		[]uint64{certifiedTip.Slot, stateSlot - 2, stateSlot - 1, stateSlot},
		stored[len(stored)-4:],
	)
	ledgerTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, tip.Slot, ledgerTip.Point.Slot)
	require.Equal(t, tip.Hash, ledgerTip.Point.Hash)
}

// TestSyncKeepChunksAppendsSignedAncillaryChunk requires a bootstrap that
// moves the certified chunk files into the database's ImmutableDB to append
// the signed ancillary chunk's blocks to those files, so the recorded
// ImmutableDB tip is the block the ledger state was taken at. The chunks are
// real Preview files: files 0 and 1 are certified and file 2 is signed.
func TestSyncKeepChunksAppendsSignedAncillaryChunk(t *testing.T) {
	t.Parallel()

	trio := map[string][]byte{}
	for _, ext := range immutableFileExtensions {
		content, err := os.ReadFile(
			filepath.Join(immutableTestdataDir, "00002."+ext),
		)
		require.NoError(t, err)
		trio[ext] = content
	}
	signed, err := readAncillaryChunk(
		slog.New(slog.DiscardHandler), signedAncillary(t, trio, nil), 0,
	)
	require.NoError(t, err)
	require.NotEmpty(t, signed)
	last := signed[len(signed)-1]
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber:  1,
		realImmutableDir:     immutableTestdataDir,
		ancillaryChunk:       trio,
		ancillaryLedgerState: minimalLedgerState(t, last.Slot, last.Hash),
		ancillaryLedgerSlot:  last.Slot,
	})
	// Preview's configuration, for the chunk geometry of the files, trusting
	// the fixture's keys.
	nodeCfg, err := SyncConfig{Network: "preview"}.resolveNodeConfig()
	require.NoError(t, err)
	nodeCfg.MithrilGenesisVerificationKey = fixture.genesisVKey
	nodeCfg.MithrilGenesisAncillaryVerificationKey = fixture.ancillaryVKey
	cfg := SyncConfig{
		UtxoSet:           newSyncTestSet(t),
		Network:           fixture.artifact.Network,
		DataDir:           t.TempDir(),
		CardanoNodeConfig: nodeCfg,
		Backend:           BackendV2,
		AggregatorURL:     fixture.server.URL,
		AllowInsecureHTTP: true,
		VerifyCertChain:   true,
		KeepChunks:        true,
		Logger:            slog.New(slog.DiscardHandler),
	}
	result, err := Sync(context.Background(), cfg)
	require.NoError(t, err)
	require.Equal(t, last.Slot, result.LedgerSlot)

	db, _ := syncedBlocks(t, cfg)
	st, ok := db.ImmutableState()
	require.True(t, ok)
	require.True(t, st.HasTip)
	require.Equal(t, last.Slot, st.TipSlot)
	require.Equal(t, last.Hash, st.TipHash)
	written, err := immutable.New(db.ImmutableDir())
	require.NoError(t, err)
	iter, err := written.BlocksInChunk(2)
	require.NoError(t, err)
	defer iter.Close()
	for i := range signed {
		block, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, block, "block %d of file 2", i)
		require.Equal(t, signed[i].Hash, block.Hash)
		require.Equal(t, signed[i].Cbor, block.Cbor)
	}
}
