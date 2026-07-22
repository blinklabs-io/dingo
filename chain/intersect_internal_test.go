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

package chain

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func TestIntersectPointsKeepsCurrentTipFallback(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })

	persistedBlock := models.Block{
		ID:     initialBlockIndex,
		Slot:   1,
		Hash:   bytes.Repeat([]byte{0x01}, 32),
		Number: 1,
		Cbor:   []byte{0x80},
	}
	require.NoError(t, db.BlockCreate(persistedBlock, nil))

	cm, err := NewManager(db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()

	pendingTip := ocommon.NewPoint(2, bytes.Repeat([]byte{0xde}, 32))
	c.currentTip = ochainsync.Tip{
		Point:       pendingTip,
		BlockNumber: 2,
	}
	c.tipBlockIndex = initialBlockIndex + 1

	points := c.IntersectPoints(4)
	require.Len(t, points, 2)
	require.Equal(t, pendingTip.Slot, points[0].Slot)
	require.Equal(t, pendingTip.Hash, points[0].Hash)
	require.Equal(t, persistedBlock.Slot, points[1].Slot)
	require.Equal(t, persistedBlock.Hash, points[1].Hash)
}

func TestIntersectPointsSkipsMissingDenseBlockIndex(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })

	var prevHash []byte
	for slot := uint64(1); slot <= 40; slot++ {
		hash := bytes.Repeat([]byte{byte(slot)}, 32)
		require.NoError(t, db.BlockCreate(models.Block{
			ID:       slot,
			Slot:     slot,
			Hash:     hash,
			Number:   slot,
			Type:     1,
			PrevHash: prevHash,
			Cbor:     []byte{0x80},
		}, nil))
		prevHash = hash
	}

	cm, err := NewManager(db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return db.Blob().Delete(
			txn.Blob(),
			dbtypes.BlockBlobIndexKey(39),
		)
	}))

	points := c.IntersectPoints(40)
	require.GreaterOrEqual(t, len(points), intersectDensePointCount)

	pointSlots := make(map[uint64]struct{}, len(points))
	for _, point := range points {
		pointSlots[point.Slot] = struct{}{}
	}
	_, hasMissingIndexSlot := pointSlots[39]
	require.False(t, hasMissingIndexSlot)
	_, hasPreviousDenseSlot := pointSlots[38]
	require.True(t, hasPreviousDenseSlot)
}
