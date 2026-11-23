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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// A commit of several blocks leaves every one of them a point the set rolls
// back to, including a block with no transactions, which changes no output
// and so stages no change of its own.
func TestCommitOfSeveralBlocksRollsBackToEachBlock(t *testing.T) {
	t.Parallel()
	set := openMigrationSet(t, t.TempDir())
	defer set.Close()
	point := func(i int) ocommon.Point {
		return ocommon.NewPoint(
			uint64(i)*10, bytes.Repeat([]byte{byte(i)}, 32), //nolint:gosec // a small index
		)
	}
	output := func(i int) ledger.UtxoOutput {
		return ledger.UtxoOutput{
			Ref: models.UtxoId{
				Hash: bytes.Repeat([]byte{0xa0 + byte(i)}, 32),
			},
			Cbor: []byte{0x80, byte(i)},
		}
	}
	base := point(1)
	require.NoError(t, set.ApplyLedgerChanges(&base, ledger.LedgerChanges{
		Utxo: []ledger.UtxoEvent{{
			Point: base, Added: []ledger.UtxoOutput{output(1)},
		}},
		Nonces: []models.BlockNonce{{Slot: base.Slot, Hash: base.Hash}},
	}))
	// Blocks 2 to 6 commit together; only block 4 has a transaction.
	tip := point(6)
	var nonces []models.BlockNonce
	for i := 2; i <= 6; i++ {
		p := point(i)
		nonces = append(nonces, models.BlockNonce{Slot: p.Slot, Hash: p.Hash})
	}
	require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
		Utxo: []ledger.UtxoEvent{{
			Point: point(4), Added: []ledger.UtxoOutput{output(4)},
		}},
		Nonces: nonces,
	}))

	for _, i := range []int{5, 3, 2} {
		_, err := set.Rollback(point(i))
		require.NoError(t, err, "block %d", i)
		got, _, err := set.Tip()
		require.NoError(t, err)
		require.Equal(t, point(i), got)
		_, live, err := set.Get(output(4).Ref.Hash, 0)
		require.NoError(t, err)
		require.Equal(t, i >= 4, live, "block 4's output at block %d", i)
	}
}
