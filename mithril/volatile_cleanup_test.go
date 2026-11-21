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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testHash32(seed string) []byte {
	hash := make([]byte, 32)
	copy(hash, []byte(seed))
	return hash
}

func TestDeleteBlobBlocksAboveSlot(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: tmpDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// Three synthetic blocks at well-separated slots so the deletion
	// boundary is unambiguous. CBOR/type don't need to parse — the
	// blob store treats them as opaque bytes.
	blocks := []models.Block{
		{
			Slot: 100,
			Hash: testHash32("blk-100"),
			Cbor: []byte{0x82, 0x01},
			Type: 1,
		},
		{
			Slot: 200,
			Hash: testHash32("blk-200"),
			Cbor: []byte{0x82, 0x02},
			Type: 1,
		},
		{
			Slot: 300,
			Hash: testHash32("blk-300"),
			Cbor: []byte{0x82, 0x03},
			Type: 1,
		},
	}
	for _, b := range blocks {
		require.NoError(t, db.BlockCreate(b, nil))
	}

	require.NoError(t, deleteBlobBlocksAboveSlotExcept(context.Background(), db, 150, nil))

	require.Equal(t, []uint64{100}, storedSlots(t, db))

	// Idempotent re-run is a no-op.
	require.NoError(t, deleteBlobBlocksAboveSlotExcept(context.Background(), db, 150, nil))
	require.Equal(t, []uint64{100}, storedSlots(t, db))
}

func TestDeleteBlobBlocksAboveSlotKeepsBoundaryTip(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: tmpDir,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	blocks := []models.Block{
		{
			Slot: 100,
			Hash: testHash32("blk-100"),
			Cbor: []byte{0x82, 0x01},
			Type: 1,
		},
		{
			Slot: 200,
			Hash: testHash32("blk-200"),
			Cbor: []byte{0x82, 0x02},
			Type: 1,
		},
		{
			Slot: 300,
			Hash: testHash32("blk-300"),
			Cbor: []byte{0x82, 0x03},
			Type: 1,
		},
	}
	for _, b := range blocks {
		require.NoError(t, db.BlockCreate(b, nil))
	}

	require.NoError(t, deleteBlobBlocksAboveSlotExcept(context.Background(), db, 200, nil))

	require.Equal(t, []uint64{100, 200}, storedSlots(t, db))

	recent, err := database.BlocksRecent(context.Background(), db, 1)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	assert.Equal(t, uint64(200), recent[0].Slot)
}
