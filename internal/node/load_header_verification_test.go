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
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/internal/config"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// headerVerificationFixtureChunks is the number of leading preview chunks the
// header verification tests replay. Preview begins after Byron, so every block
// carries a Shelley-family header.
const headerVerificationFixtureChunks = 6

// copyImmutableFixturePrefix copies the leading chunks of the preview
// ImmutableDB fixture into a fresh directory.
func copyImmutableFixturePrefix(t *testing.T) string {
	t.Helper()
	src := filepath.Join("..", "..", "database", "immutable", "testdata")
	dir := t.TempDir()
	for i := range headerVerificationFixtureChunks {
		for _, ext := range []string{"chunk", "primary", "secondary"} {
			name := fmt.Sprintf("%05d.%s", i, ext)
			data, err := os.ReadFile(filepath.Join(src, name))
			require.NoError(t, err)
			require.NoError(
				t,
				os.WriteFile(filepath.Join(dir, name), data, 0o600),
			)
		}
	}
	return dir
}

// corruptImmutableTipSignature flips one byte inside the KES signature of the
// tip block's header. The tip has no successor, so the corrupted chain stays
// structurally valid: only the Praos header proof is wrong.
func corruptImmutableTipSignature(
	t *testing.T,
	dir string,
) (tipSlot uint64) {
	t.Helper()
	imm, err := immutable.New(dir)
	require.NoError(t, err)
	tip, err := imm.GetTip()
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()
	var last *immutable.Block
	for {
		next, err := iter.Next()
		require.NoError(t, err)
		if next == nil {
			break
		}
		last = next
	}
	require.NotNil(t, last)
	require.Equal(t, tip.Slot, last.Slot)
	headerCbor, err := extractHeaderCbor(last.Cbor)
	require.NoError(t, err)
	// The KES signature is the last field of the header, so a byte near the
	// end of the header lies inside it.
	target := len(headerCbor) - 20
	corrupt := bytes.Clone(last.Cbor)
	offset := bytes.Index(corrupt, headerCbor) + target
	corrupt[offset] ^= 0xFF
	require.NotEqual(t, last.Cbor, corrupt)
	chunks, err := filepath.Glob(filepath.Join(dir, "*.chunk"))
	require.NoError(t, err)
	patched := false
	for i := len(chunks) - 1; i >= 0 && !patched; i-- {
		data, err := os.ReadFile(chunks[i])
		require.NoError(t, err)
		idx := bytes.LastIndex(data, last.Cbor)
		if idx < 0 {
			continue
		}
		copy(data[idx:], corrupt)
		require.NoError(t, os.WriteFile(chunks[i], data, 0o600))
		patched = true
	}
	require.True(t, patched, "tip block not found in any chunk file")
	return last.Slot
}

func loadFixtureForHeaderVerification(
	t *testing.T,
	immutableDir string,
) (*chain.Chain, ochainsync.Tip, error) {
	t.Helper()
	db := newTestDB(t)
	cfg := &config.Config{
		Network:       "preview",
		CardanoConfig: cardano.EmbeddedConfigPath("preview"),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := LoadWithDB(context.Background(), cfg, logger, immutableDir, db)
	cm, cmErr := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, cmErr)
	dbTip, tipErr := db.GetTip(nil)
	require.NoError(t, tipErr)
	return cm.PrimaryChain(), dbTip, err
}

func TestLoadWithDBRejectsInvalidPraosHeader(t *testing.T) {
	t.Parallel()
	dir := copyImmutableFixturePrefix(t)
	tipSlot := corruptImmutableTipSignature(t, dir)
	c, dbTip, err := loadFixtureForHeaderVerification(t, dir)
	require.Error(
		t, err,
		"load must reject a block whose Praos header signature is invalid",
	)
	require.ErrorContains(t, err, "header verification failed")
	require.Less(
		t, c.Tip().Point.Slot, tipSlot,
		"the block with the invalid header must not remain on the chain",
	)
	require.Less(
		t, dbTip.Point.Slot, tipSlot,
		"the block with the invalid header must not reach ledger state",
	)
	// Blocks copied ahead of the rejected batch would otherwise sit above the
	// ledger tip, where a later serve-mode replay applies them without the
	// load-only header check.
	require.Equal(
		t, dbTip.Point, c.Tip().Point,
		"the chain must be rewound exactly to the ledger tip",
	)
}

func TestLoadWithDBAcceptsValidPraosHeaders(t *testing.T) {
	t.Parallel()
	dir := copyImmutableFixturePrefix(t)
	imm, err := immutable.New(dir)
	require.NoError(t, err)
	tip, err := imm.GetTip()
	require.NoError(t, err)
	c, dbTip, err := loadFixtureForHeaderVerification(t, dir)
	require.NoError(t, err)
	require.Equal(t, tip.Slot, c.Tip().Point.Slot)
	require.Equal(t, tip.Slot, dbTip.Point.Slot)
}
