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

package database

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// An output a snapshot imported carries the anchor slot, and every rollback
// entry point floors its deletion slot at the anchor, so a rollback target
// below the anchor leaves the imported output in place.
func TestTruncateAfterSlotKeepsImportedUtxoAtMithrilFloor(t *testing.T) {
	t.Parallel()

	const anchor, target = uint64(100), uint64(50)
	truncate := func(t *testing.T, floor uint64) bool {
		t.Helper()
		db := newOnDiskTestDB(t)
		block := testIndexedBlock(target, 1, 0x15)
		require.NoError(t, db.BlockCreate(block, nil))
		imported := models.Utxo{
			TxId:      bytes.Repeat([]byte{0xa1}, 32),
			OutputIdx: 0,
			AddedSlot: anchor,
			Amount:    1_000_000,
		}
		seed := db.MetadataTxn(context.Background(), true)
		require.NoError(t, seed.Do(func(txn *Txn) error {
			return db.Metadata().ImportUtxos(
				[]models.Utxo{imported}, txn.Metadata(),
			)
		}))
		seed.Release()

		txn := db.Transaction(context.Background(), true)
		require.NoError(t, txn.Do(func(txn *Txn) error {
			_, _, err := db.TruncateAfterSlot(
				context.Background(),
				ocommon.Point{Slot: target, Hash: block.Hash},
				floor,
				txn,
			)
			return err
		}))
		exists, err := db.UtxoExists(
			context.Background(), imported.TxId, imported.OutputIdx, nil,
		)
		require.NoError(t, err)
		return exists
	}

	require.True(
		t,
		truncate(t, anchor),
		"a rollback below the Mithril anchor must keep the imported output",
	)
	require.False(
		t,
		truncate(t, 0),
		"without the floor the same rollback removes it",
	)
}
