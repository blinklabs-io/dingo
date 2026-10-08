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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestProcessGovernanceJudgesPreviousEraBlockByItsEraParameters applies a
// ParameterChange of an era-1 block after the ledger has advanced to the next
// era. A zero coinsPerUTxOByte is accepted at protocol version 9 and refused
// at the successor's version, so the proposal persists only when the block is
// judged under the parameters transaction validation used for it.
func TestProcessGovernanceJudgesPreviousEraBlockByItsEraParameters(
	t *testing.T,
) {
	t.Parallel()

	fx := newParameterChangeFixture(t, map[uint]any{
		testutil.PParamUpdateKeyAdaPerUtxoByte: 0,
	})
	prev := *fx.pparams
	prev.ProtocolVersion.Major = 9
	next := eras.EraDesc{Id: eras.DijkstraEraDesc.Id + 1}
	fx.ls.activeEras = []eras.EraDesc{eras.DijkstraEraDesc, next}
	fx.ls.currentEra = next
	fx.ls.prevEraPParams = &prev
	fx.ls.publishSnapshotsLocked()

	delta := NewLedgerDelta(
		ocommon.Point{
			Slot: fx.block.SlotNumber(),
			Hash: fx.block.Hash().Bytes(),
		},
		eras.DijkstraEraDesc.Id,
		fx.block.BlockNumber(),
	)
	t.Cleanup(delta.Release)
	require.NoError(t, fx.db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
		return delta.processGovernance(t.Context(), fx.ls, fx.tx, 0, txn, nil)
	}))
	require.True(t, fx.proposalStored(t))
}
