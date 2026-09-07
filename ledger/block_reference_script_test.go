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
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestBlockReferenceScriptLimitAdmission(t *testing.T) {
	for _, over := range []bool{false, true} {
		name := "at limit"
		if over {
			name = "over limit"
		}
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			address, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeKeyNone,
				0,
				bytes.Repeat([]byte{1}, 28),
				nil,
			)
			require.NoError(t, err)
			block := &conway.ConwayBlock{
				BlockHeader: &conway.ConwayBlockHeader{},
			}
			block.BlockHeader.Body.BlockNumber = 1
			block.BlockHeader.Body.Slot = 1
			block.BlockHeader.Body.ProtoVersion.Major = 11
			for i := range 6 {
				size := int(conway.MaxRefScriptSizePerBlock / 6)
				if i == 5 {
					size += int(conway.MaxRefScriptSizePerBlock % 6)
					if over {
						size++
					}
				}
				require.Less(t, uint64(size), conway.MaxRefScriptSizePerTx)
				txID := bytes.Repeat([]byte{byte(i + 1)}, 32)
				input := shelley.ShelleyTransactionInput{
					TxId: lcommon.NewBlake2b256(txID),
				}
				output := &babbage.BabbageTransactionOutput{
					OutputAddress: address,
					TxOutScriptRef: &lcommon.ScriptRef{
						Type:   lcommon.ScriptRefTypePlutusV3,
						Script: make(lcommon.PlutusV3Script, size),
					},
				}
				encoded, err := cbor.Encode(output)
				require.NoError(t, err)
				require.NoError(
					t,
					db.Transaction(true).Do(func(txn *database.Txn) error {
						if err := db.CreateUtxo(txn, &models.Utxo{TxId: txID, OutputIdx: 0, AddedSlot: 0}); err != nil {
							return err
						}
						return db.Blob().SetUtxo(txn.Blob(), txID, 0, encoded)
					}),
				)
				block.TransactionBodies = append(
					block.TransactionBodies,
					conway.ConwayTransactionBody{
						TxReferenceInputs: cbor.NewSetType(
							[]shelley.ShelleyTransactionInput{input},
							false,
						),
					},
				)
				block.TransactionWitnessSets = append(
					block.TransactionWitnessSets,
					conway.ConwayTransactionWitnessSet{},
				)
			}
			encodedBlock, err := cbor.EncodeGeneric(block)
			require.NoError(t, err)
			block.SetCbor(encodedBlock)
			bodySize, err := serializedBlockBodySize(block)
			require.NoError(t, err)
			block.BlockHeader.Body.BlockBodySize = bodySize
			encodedBlock, err = cbor.EncodeGeneric(block)
			require.NoError(t, err)
			block.SetCbor(encodedBlock)
			pp := &conway.ConwayProtocolParameters{
				MaxBlockBodySize:   100000,
				MaxBlockHeaderSize: 100000,
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 11,
				},
			}
			sentinel := errors.New("transaction validator reached")
			era := eras.ConwayEraDesc
			era.ValidateTxFunc = func(lcommon.Transaction, uint64, lcommon.LedgerState, lcommon.ProtocolParameters) error {
				return sentinel
			}
			nodeConfig := newTestShelleyGenesisCfg(t)
			nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
			ls := &LedgerState{
				db:             db,
				activeEras:     []eras.EraDesc{era, eras.DijkstraEraDesc},
				currentEra:     era,
				currentPParams: pp,
				config: LedgerStateConfig{
					Logger:            testLogger(),
					CardanoNodeConfig: nodeConfig,
				},
			}
			ls.metrics.init(prometheus.NewRegistry())
			ls.publishSnapshotsLocked()
			for path, run := range map[string]func() error{
				"imported_previous_era": func() error {
					currentParams := &dijkstra.DijkstraProtocolParameters{
						ConwayProtocolParameters: *pp,
						MaxRefScriptSizePerBlock: 1,
					}
					return db.Transaction(true).Do(func(txn *database.Txn) error {
						_, err := ls.ledgerProcessBlock(txn, ocommon.NewPoint(1, block.Hash().Bytes()), block, true, false, false, nil, envelopeParent{origin: true}, nil, eras.DijkstraEraDesc, currentParams, pp, 0)
						return err
					})
				},
				"imported": func() error {
					return db.Transaction(true).Do(func(txn *database.Txn) error {
						_, err := ls.ledgerProcessBlock(txn, ocommon.NewPoint(1, block.Hash().Bytes()), block, true, false, false, nil, envelopeParent{origin: true}, nil, era, pp, nil, 0)
						return err
					})
				},
				"forged": func() error { return ls.validateForgedTxs(block) },
			} {
				t.Run(path, func(t *testing.T) {
					err := run()
					if over {
						var limit lcommon.RefScriptSizePerBlockTooLargeError
						require.ErrorAs(
							t,
							err,
							&limit,
							"aggregate reference-script limit must reject before transaction validation",
						)
						require.Equal(
							t,
							conway.MaxRefScriptSizePerBlock+1,
							limit.BlockSize,
						)
					} else {
						require.ErrorIs(t, err, sentinel, "exact block limit must reach later transaction validation")
					}
				})
			}
		})
	}
}
