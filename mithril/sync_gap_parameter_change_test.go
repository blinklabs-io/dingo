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

package mithril

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestProcessGapBlockTransactionsRejectsMalformedSubtransactionParameterChange
// applies a Dijkstra sub-transaction ParameterChange through the gap-block
// path, which stores transactions already trusted by a snapshot without
// running transaction validation. A zero-valued deposit must still be refused
// before a governance_proposal row exists, and the same proposal with a
// nonzero value must be stored.
func TestProcessGapBlockTransactionsRejectsMalformedSubtransactionParameterChange(
	t *testing.T,
) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		deposit uint64
		wantErr bool
	}{
		{name: "zero govActionDeposit", deposit: 0, wantErr: true},
		{name: "nonzero govActionDeposit", deposit: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(t, &database.Config{
				DataDir: t.TempDir(),
				Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			require.NoError(t, err)
			defer dbtest.CloseDatabase(db)

			rewardAddress, err := lcommon.NewAddressFromBytes(
				append([]byte{0xe0}, testGapHash28("dijkstra-paramchange")...),
			)
			require.NoError(t, err)
			deposit := tc.deposit
			proposal := dijkstra.DijkstraProposalProcedure{
				PPDeposit:       42,
				PPRewardAccount: rewardAddress,
				PPGovAction: dijkstra.DijkstraGovAction{
					Type: uint(lcommon.GovActionTypeParameterChange),
					Action: &dijkstra.DijkstraParameterChangeGovAction{
						Type: uint(lcommon.GovActionTypeParameterChange),
						ParamUpdate: dijkstra.DijkstraProtocolParameterUpdate{
							GovActionDeposit: &deposit,
						},
					},
				},
				PPAnchor: lcommon.GovAnchor{
					Url:      "https://example.com/dijkstra-gap-paramchange",
					DataHash: [32]byte(testGapHash32("dijkstra-paramchange-anchor")),
				},
			}
			outputCbor, err := cbor.Encode(map[uint]any{
				0: append([]byte{0x60}, testGapHash28("dijkstra-paramchange-output")...),
				1: uint64(1_000_000),
			})
			require.NoError(t, err)
			var output dijkstra.DijkstraTransactionOutput
			_, err = cbor.Decode(outputCbor, &output)
			require.NoError(t, err)
			tx := &dijkstra.DijkstraTransaction{
				Body: dijkstra.DijkstraTransactionBody{
					TxSubTransactions: cbor.NewSetType(
						[]dijkstra.DijkstraSubTransaction{{
							Body: dijkstra.DijkstraSubTransactionBody{
								TxOutputs: []dijkstra.DijkstraTransactionOutput{output},
								TxProposalProcedures: []dijkstra.DijkstraProposalProcedure{
									proposal,
								},
							},
						}},
						true,
					),
				},
				TxIsValid: true,
			}
			txCbor, err := tx.MarshalCBOR()
			require.NoError(t, err)
			decodedTx, err := gledger.NewTransactionFromCbor(
				gledger.TxTypeDijkstra,
				txCbor,
			)
			require.NoError(t, err)
			tx = decodedTx.(*dijkstra.DijkstraTransaction)
			childHash := tx.Body.TxSubTransactions.Items()[0].Body.Id()
			rootHash := tx.Hash()

			point := ocommon.Point{
				Slot: 1000,
				Hash: testGapHash32("dijkstra-paramchange-block"),
			}
			var blockHash [32]byte
			copy(blockHash[:], point.Hash)
			var childHashArray, rootHashArray [32]byte
			copy(childHashArray[:], childHash.Bytes())
			copy(rootHashArray[:], rootHash.Bytes())
			offsets := &database.BlockIngestionResult{
				TxOffsets: map[[32]byte]database.CborOffset{
					childHashArray: {
						BlockSlot: point.Slot, BlockHash: blockHash,
						ByteOffset: 0, ByteLength: 1,
					},
					rootHashArray: {
						BlockSlot: point.Slot, BlockHash: blockHash,
						ByteOffset: 1, ByteLength: 1,
					},
				},
				UtxoOffsets: map[database.UtxoRef]database.CborOffset{
					{TxId: childHashArray, OutputIdx: 0}: {
						BlockSlot: point.Slot, BlockHash: blockHash,
						ByteOffset: 2, ByteLength: 1,
					},
				},
			}
			pparams := &dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: dijkstra.MinProtocolVersionDijkstra,
					},
					GovActionValidityPeriod: 20,
					DRepInactivityPeriod:    20,
				},
			}
			err = processGapBlockTransactions(
				context.Background(),
				db,
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				point,
				[]lcommon.Transaction{tx},
				offsets,
				100,
				dijkstra.EraIdDijkstra,
				pparams,
				&pparams.ConwayProtocolParameters,
			)
			got, getErr := db.GetGovernanceProposal(
				context.Background(), childHash.Bytes(), 0, nil,
			)
			if tc.wantErr {
				require.ErrorContains(t, err, "govActionDeposit")
				require.ErrorIs(t, getErr, models.ErrGovernanceProposalNotFound)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.NoError(t, getErr)
			require.Equal(t, childHash.Bytes(), got.TxHash)
		})
	}
}
