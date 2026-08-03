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

package ledger

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPersistTipAfterForgedBlockUpdatesPersistedTip verifies that
// persistTipAfterForgedBlock actually advances database.GetTip to match
// the forged block -- forgeBlock's own ls.chain.AddBlock call only
// updates ls.chain's in-memory tip, not the persisted one, unlike the
// normal chainsync/forged-block batch pipeline (which calls db.SetTip
// itself). Without this call, a dev-mode-forged block is written to the
// blob/metadata block tables but invisible to anything relying on the
// persisted tip -- dingoctl's `database info`, a live Truncate's
// deletion boundary, and BlockForger's leader-election check all read
// stale data, and a later Truncate can never reach (and clean up) such a
// block, eventually surfacing as a "persistent chain index gap" error.
func TestPersistTipAfterForgedBlockUpdatesPersistedTip(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	block := newRecordForgedBlockTestBlock(42, 7)
	require.NoError(t, ls.persistTipAfterForgedBlock(block))

	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(42), tip.Point.Slot)
	require.Equal(t, block.Hash().Bytes(), tip.Point.Hash)
	require.Equal(t, uint64(7), tip.BlockNumber)
}
