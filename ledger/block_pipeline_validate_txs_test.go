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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/pipeline"
	"github.com/stretchr/testify/require"
)

// The validate stage carries no protocol parameters, so a body rule it runs
// cannot succeed. A header-valid block with a transaction must pass it, as a
// block with an empty body does.
func TestDecodeReadChainBatchValidatesBlockWithTransactions(t *testing.T) {
	t.Parallel()

	var seed [32]byte
	seed[0] = 9
	vb := testutil.BuildValidatedConwayBlockBytesWithTransaction(
		t, seed, 7, 100, 1,
	)
	block := models.Block{
		Slot:   vb.Slot,
		Hash:   vb.Hash,
		Number: vb.BlockNumber,
		Type:   gledger.BlockTypeConway,
		Cbor:   vb.Cbor,
	}

	ls := newValidatedPipelineTestLedger(t, vb)
	require.NoError(t, ls.blockPipeline.Stop())
	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(1),
		pipeline.WithValidateWorkers(1),
		pipeline.WithEta0(vb.EpochNonceHex),
		pipeline.WithSlotsPerKesPeriod(vb.SlotsPerKesPeriod),
		pipeline.WithVerifyConfig(blockPipelineVerifyConfig()),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))

	decoded, ok := ls.decodeReadChainBatch(
		t.Context(),
		[]models.Block{block},
	)
	require.True(t, ok, "a header-valid block with transactions must pass")
	require.Len(t, decoded, 1)
	require.Len(t, decoded[0].Transactions(), 1)
}
