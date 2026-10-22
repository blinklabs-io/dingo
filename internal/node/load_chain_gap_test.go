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
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// copyImmutableChunkPrefix copies chunks [0, count) of the preview testdata
// ImmutableDB into a fresh directory, so a test can load a short chain.
func copyImmutableChunkPrefix(t *testing.T, count int) string {
	t.Helper()
	src := filepath.Join("..", "..", "database", "immutable", "testdata")
	dst := t.TempDir()
	for i := range count {
		for _, ext := range []string{"chunk", "primary", "secondary"} {
			name := fmt.Sprintf("%05d.%s", i, ext)
			data, err := os.ReadFile(filepath.Join(src, name))
			require.NoError(t, err)
			require.NoError(
				t,
				os.WriteFile(filepath.Join(dst, name), data, 0o600),
			)
		}
	}
	return dst
}

// Not t.Parallel: swaps the package-level processTrustedBlockBatchesForLoad.
//
// TestLoadWithDBReplaysChainBlocksAheadOfLedgerTip covers a database whose
// chain holds blocks the ledger has not applied, which is what `dingo mithril
// sync` leaves behind: the immutable files past the ledger-state snapshot are
// copied as blobs only. Load must apply those blocks before the ones it
// copies, or the first copied block does not extend the ledger tip.
func TestLoadWithDBReplaysChainBlocksAheadOfLedgerTip(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	immutableDir := copyImmutableChunkPrefix(t, 4)
	imm, err := immutable.New(immutableDir)
	require.NoError(t, err)
	immutableTip, err := imm.GetTip()
	require.NoError(t, err)
	require.NotNil(t, immutableTip)
	blobBound, ok, err := imm.LastSlotInChunk(1)
	require.NoError(t, err)
	require.True(t, ok)

	db := newTestDB(t)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	copied, _, err := CopyImmutableBlobsBounded(
		t.Context(), logger, imm, cm.PrimaryChain(), blobBound, nil,
	)
	require.NoError(t, err)
	require.Positive(t, copied)
	ledgerTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Less(
		t,
		ledgerTip.Point.Slot,
		cm.PrimaryChain().Tip().Point.Slot,
		"setup must leave the chain ahead of the ledger",
	)

	var wantSlots []uint64
	iter, err := imm.BlocksFromPoint(ocommon.NewPointOrigin())
	require.NoError(t, err)
	for {
		blk, err := iter.Next()
		require.NoError(t, err)
		if blk == nil {
			break
		}
		wantSlots = append(wantSlots, blk.Slot)
	}
	iter.Close()

	var replayedSlots []uint64
	oldProcess := processTrustedBlockBatchesForLoad
	processTrustedBlockBatchesForLoad = func(
		ls *ledger.LedgerState,
		ctx context.Context,
		batches <-chan []gledger.Block,
	) error {
		observed := make(chan []gledger.Block)
		go func() {
			defer close(observed)
			for batch := range batches {
				for _, blk := range batch {
					replayedSlots = append(replayedSlots, blk.SlotNumber())
				}
				select {
				case observed <- batch:
				case <-ctx.Done():
					return
				}
			}
		}()
		return oldProcess(ls, ctx, observed)
	}
	t.Cleanup(func() {
		processTrustedBlockBatchesForLoad = oldProcess
	})

	require.NoError(t, LoadWithDB(
		t.Context(),
		&config.Config{Network: "preview"},
		logger,
		immutableDir,
		db,
	))

	require.Equal(
		t,
		wantSlots,
		replayedSlots,
		"load must apply every block from the ledger tip, including the "+
			"ones already on the chain",
	)
	appliedTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, immutableTip.Slot, appliedTip.Point.Slot)
	require.Equal(t, immutableTip.Hash, appliedTip.Point.Hash)
}
