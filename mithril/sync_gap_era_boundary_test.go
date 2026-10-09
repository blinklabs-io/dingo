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
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// TestProcessGapBlockJudgesPreviousEraBlockByItsEraParameters stores a Conway
// ParameterChange from a Conway gap block that falls in the epoch recorded as
// Dijkstra's first. A zero coinsPerUTxOByte is accepted at protocol version 9
// and refused at the successor's version, so the proposal persists only when
// the block is judged under the last Conway epoch's stored parameters, as
// transaction validation judged it. A Dijkstra block in the same epoch keeps
// that epoch's parameters.
func TestProcessGapBlockJudgesPreviousEraBlockByItsEraParameters(
	t *testing.T,
) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		blockEra uint
		wantErr  string
	}{
		{name: "previous-era block", blockEra: conway.EraIdConway},
		{
			name:     "boundary-era block",
			blockEra: dijkstra.EraIdDijkstra,
			wantErr:  "coinsPerUTxOByte",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(t, &database.Config{
				DataDir: t.TempDir(),
				Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			require.NoError(t, err)
			defer dbtest.CloseDatabase(db)

			prev := mockledger.NewMockConwayProtocolParams()
			prev.ProtocolVersion.Major = 9
			prev.GovActionValidityPeriod = 20
			prev.DRepInactivityPeriod = 20
			next := dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: prev,
			}
			next.ProtocolVersion.Major = dijkstra.MinProtocolVersionDijkstra
			epochs := []models.Epoch{
				{EpochId: 10, EraId: conway.EraIdConway, StartSlot: 0},
				{EpochId: 11, EraId: dijkstra.EraIdDijkstra, StartSlot: 500},
			}
			for _, stored := range []struct {
				epoch  models.Epoch
				params any
			}{
				{epoch: epochs[0], params: &prev},
				{epoch: epochs[1], params: &next},
			} {
				encoded, err := cbor.Encode(stored.params)
				require.NoError(t, err)
				require.NoError(t, db.SetPParams(
					encoded,
					stored.epoch.StartSlot,
					stored.epoch.EpochId,
					stored.epoch.EraId,
					nil,
				))
			}

			paramsEpoch, pparams, err := gapBlockParams(
				db,
				epochs,
				epochs[1],
				test.blockEra,
				make(map[uint64]lcommon.ProtocolParameters),
			)
			require.NoError(t, err)
			require.Equal(t, test.blockEra, paramsEpoch.EraId)
			var conwayPParams *conway.ConwayProtocolParameters
			switch p := pparams.(type) {
			case *conway.ConwayProtocolParameters:
				conwayPParams = p
			case *dijkstra.DijkstraProtocolParameters:
				conwayPParams = &p.ConwayProtocolParameters
			default:
				require.Failf(t, "unexpected protocol parameters", "%T", pparams)
			}

			tx, err := conway.NewConwayTransactionFromCbor(
				testutil.ParameterChangeTxCbor(
					t,
					map[uint]any{testutil.PParamUpdateKeyAdaPerUtxoByte: 0},
					"https://example.com/"+test.name,
				),
			)
			require.NoError(t, err)
			// The gap path consumes the snapshot's live row for each input.
			inputs := tx.Inputs()
			require.Len(t, inputs, 1)
			require.NoError(t, db.Transaction(context.Background(), true).Do(
				func(txn *database.Txn) error {
					return db.CreateUtxo(context.Background(), txn, &models.Utxo{
						TxId:      inputs[0].Id().Bytes(),
						OutputIdx: inputs[0].Index(),
						AddedSlot: 1,
					})
				},
			))
			point := ocommon.Point{
				Slot: 600,
				Hash: testGapHash32("era-boundary-gap-block"),
			}
			var blockHash, txHash [32]byte
			copy(blockHash[:], point.Hash)
			copy(txHash[:], tx.Hash().Bytes())
			offsets := &database.BlockIngestionResult{
				TxOffsets: map[[32]byte]database.CborOffset{
					txHash: {
						BlockSlot: point.Slot, BlockHash: blockHash,
						ByteOffset: 0, ByteLength: 1,
					},
				},
				UtxoOffsets: map[database.UtxoRef]database.CborOffset{
					{TxId: txHash, OutputIdx: 0}: {
						BlockSlot: point.Slot, BlockHash: blockHash,
						ByteOffset: 1, ByteLength: 1,
					},
				},
			}
			err = processGapBlockTransactions(
				context.Background(),
				db,
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				point,
				[]lcommon.Transaction{tx},
				offsets,
				epochs[1].EpochId,
				paramsEpoch.EraId,
				pparams,
				conwayPParams,
			)
			got, getErr := db.GetGovernanceProposal(
				context.Background(), txHash[:], 0, nil,
			)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.ErrorIs(t, getErr, models.ErrGovernanceProposalNotFound)
				return
			}
			require.NoError(t, err)
			require.NoError(t, getErr)
			require.Equal(t, txHash[:], got.TxHash)
		})
	}
}
