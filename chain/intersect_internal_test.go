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
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func TestIntersectPointsKeepsCurrentTipFallback(t *testing.T) {
	db, err := database.New(&database.Config{
		DataDir:        t.TempDir(),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

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
