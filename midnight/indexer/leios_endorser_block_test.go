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

package indexer

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// A certifying ranking block has an empty body; its transactions live in the
// endorser block. The indexer must take its transactions from BlockTransactions
// rather than from the decoded ranking block, on the live path and on rollback,
// including a later spend of an output the endorser block created.
func TestLiveAndRollbackUseBlockTransactionsHook(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	create := validRegistrationHistoryBlock(t)
	spend := create
	spend.ID, spend.Number, spend.Slot = 2, create.Number+1, create.Slot+1
	cfg := registrationRebuildConfig(store)
	cfg.FatalErrorFunc = func(err error) { require.NoError(t, err) }

	createHash := pad32("fa000001")
	createTx := buildTx(t, createHash,
		[]lcommon.TransactionInput{buildInput(t, pad32("fa000002"), 0)},
		[]lcommon.TransactionOutput{
			buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, simpleDatumCbor(t)),
		},
	)
	spendTx := buildTx(t, pad32("fa000003"),
		[]lcommon.TransactionInput{buildInput(t, createHash, 0)},
		[]lcommon.TransactionOutput{
			buildCNightOutput(t, "aa", "bb", 1),
		},
	)
	cfg.BlockTransactions = func(
		_ context.Context,
		block models.Block,
	) ([]lcommon.Transaction, error) {
		if block.Number == create.Number {
			return []lcommon.Transaction{createTx}, nil
		}
		return []lcommon.Transaction{spendTx}, nil
	}
	idx, err := New(cfg)
	require.NoError(t, err)

	key := utxoKey{TxHash: createHash, Index: 0}
	tracked := func() bool {
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		_, ok := idx.regUTxOs[key]
		return ok
	}
	send := func(action ledger.BlockAction, block models.Block) {
		idx.handleBlockEvent(event.Event{Data: ledger.BlockEvent{
			Action: action, Block: block,
		}})
	}

	send(ledger.BlockActionApply, create)
	require.True(t, tracked(), "endorser-block registration must be indexed")
	send(ledger.BlockActionApply, spend)
	require.False(t, tracked(), "endorser-block spend must be recorded")
	send(ledger.BlockActionUndo, spend)
	require.True(t, tracked(), "rolling back the spend must restore the output")
	send(ledger.BlockActionUndo, create)
	require.False(t, tracked(), "rolling back the create must remove the output")
}
