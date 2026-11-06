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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestValidatedBlockBatchesAPIIndexesAndKeepsStateImmediate(t *testing.T) {
	t.Parallel()
	for _, failFlush := range []bool{false, true} {
		t.Run(fmt.Sprint(failFlush), func(t *testing.T) {
			t.Parallel()
			ls, db, raw, _ := newLeiosAPIApplyTestLedger(t)
			address := rowBatchingBaseAddress(t, 0x21, 0x22)
			first := newRowBatchingTx(
				t,
				map[uint]any{
					0: cbor.Tag{Number: 258, Content: []any{}},
					1: []any{[]any{address, uint64(1_000_000)}},
					2: uint64(200_000),
				},
			)
			second := newRowBatchingTx(
				t,
				map[uint]any{
					0: cbor.Tag{
						Number:  258,
						Content: []any{[]any{first.hash, uint64(0)}},
					},
					1: []any{[]any{address, uint64(800_000)}},
					2: uint64(200_000),
				},
			)
			block := rowBatchingBlock(
				t,
				10,
				1,
				lcommon.Blake2b256{},
				first,
				second,
			)
			bodySize, err := serializedBlockBodySize(block)
			require.NoError(t, err)
			conwayBlock := block.(*conway.ConwayBlock)
			conwayBlock.BlockHeader.Body.BlockBodySize = bodySize
			conwayBlock.BlockHeader.SetCbor(nil)
			conwayBlock.BlockHeader.Body.SetCbor(nil)
			encodedBlock, err := cbor.EncodeGeneric(conwayBlock)
			require.NoError(t, err)
			conwayBlock.SetCbor(encodedBlock)
			point := ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
			offsets, err := database.NewBlockIndexer(point.Slot, point.Hash).
				ComputeOffsets(block.Cbor(), block)
			require.NoError(t, err)
			_, err = raw.Exec(
				`CREATE TRIGGER require_complete_validated_block BEFORE INSERT ON address_transaction WHEN (SELECT COUNT(*) FROM "transaction") < 2 BEGIN SELECT RAISE(ABORT, 'API rows flushed before validated block completed'); END`,
			)
			require.NoError(t, err)
			if failFlush {
				_, err = raw.Exec(
					`CREATE TRIGGER fail_validated_block_api BEFORE INSERT ON address_transaction WHEN (SELECT COUNT(*) FROM "transaction") >= 2 BEGIN SELECT RAISE(ABORT, 'injected validated-block flush failure'); END`,
				)
				require.NoError(t, err)
			}
			validations := 0
			validate := func(tx lcommon.Transaction, _ uint64, state lcommon.LedgerState, _ lcommon.ProtocolParameters) error {
				validations++
				lv := state.(*LedgerView)
				for _, input := range tx.Inputs() {
					stored, err := db.Metadata().
						GetUtxo(input.Id().Bytes(), input.Index(), lv.txn.Metadata())
					require.NoError(t, err)
					require.NotNil(
						t,
						stored,
						"previous transaction's UTxO must already be in SQL",
					)
					require.EqualValues(t, 1_000_000, stored.Amount)
					_, err = state.UtxoById(input)
					require.NoError(t, err)
				}
				return nil
			}
			era := eras.EraDesc{
				Id:             conway.EraIdConway,
				ValidateTxFunc: validate,
			}
			pp := &conway.ConwayProtocolParameters{
				MaxBlockBodySize:   2_000_000,
				MaxBlockHeaderSize: 100_000,
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 10,
				},
			}
			nodeConfig := newTestShelleyGenesisCfg(t)
			nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
			ls.config.CardanoNodeConfig = nodeConfig
			ls.currentEra = era
			ls.activeEras = []eras.EraDesc{era}
			ls.currentPParams = pp
			ls.currentEpoch.EraId = era.Id
			ls.publishSnapshotsLocked()
			err = db.Transaction(t.Context(), true).
				Do(func(txn *database.Txn) error {
					delta, err := ls.ledgerProcessBlock(
						t.Context(),
						txn,
						point,
						block,
						true,
						false,
						false,
						nil,
						envelopeParent{origin: true},
						offsets,
						era,
						pp,
						nil,
						0,
						0,
						false,
					)
					if delta != nil {
						delta.Release()
					}
					return err
				})
			require.Equal(t, 2, validations, "block apply error: %v", err)
			if failFlush {
				require.ErrorContains(
					t,
					err,
					"injected validated-block flush failure",
				)
				requireLeiosApplyTestTxCount(t, raw, 0)
				var count int
				require.NoError(
					t,
					raw.QueryRow(`SELECT COUNT(*) FROM utxo`).Scan(&count),
				)
				require.Zero(t, count)
				return
			}
			require.NoError(t, err)
			requireLeiosApplyTestTxCount(t, raw, 2)
			var history int
			require.NoError(
				t,
				raw.QueryRow(`SELECT COUNT(*) FROM address_transaction`).
					Scan(&history),
			)
			require.Equal(t, 2, history)
			var utxoStake, totalStake string
			require.NoError(t, raw.QueryRow(
				`SELECT utxo_stake, total_stake FROM reward_live_stake WHERE credential_tag = 0 AND staking_key = ?`,
				rowBatchingKey(0x22),
			).Scan(&utxoStake, &totalStake))
			require.Equal(t, "800000", utxoStake,
				"validated-block stake must apply once across the API flush")
			require.Equal(t, "800000", totalStake)
		})
	}
}
