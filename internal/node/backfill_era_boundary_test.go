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

package node

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// TestBackfillJudgesPreviousEraBlockByItsEraParameters persists a Conway
// ParameterChange from a Conway block that falls in the epoch recorded as
// Dijkstra's first. A zero coinsPerUTxOByte is accepted at protocol version 9
// and refused at the successor's version, so the proposal persists only when
// the block is judged under the last Conway epoch's parameters, as transaction
// validation judged it. A Dijkstra block in the same epoch keeps that epoch's
// parameters.
func TestBackfillJudgesPreviousEraBlockByItsEraParameters(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		blockEra uint
		wantEra  uint
		wantErr  string
	}{
		{
			name:     "previous-era block",
			blockEra: conway.EraIdConway,
			wantEra:  conway.EraIdConway,
		},
		{
			name:     "boundary-era block",
			blockEra: dijkstra.EraIdDijkstra,
			wantEra:  dijkstra.EraIdDijkstra,
			wantErr:  "coinsPerUTxOByte",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			backfill := NewBackfill(db, nil, slog.Default())

			prev := mockledger.NewMockConwayProtocolParams()
			prev.GovActionValidityPeriod = 20
			prev.ProtocolVersion.Major = 9
			next := &dijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: prev,
			}
			next.ProtocolVersion.Major = dijkstra.MinProtocolVersionDijkstra
			backfill.epochs = []models.Epoch{
				{EpochId: 10, EraId: conway.EraIdConway},
				{EpochId: 11, EraId: dijkstra.EraIdDijkstra},
			}
			backfill.pparamsCache = map[uint64]lcommon.ProtocolParameters{
				10: &prev,
				11: next,
			}

			eraID, pparams := backfill.blockEraParams(
				11,
				dijkstra.EraIdDijkstra,
				test.blockEra,
			)
			require.Equal(t, test.wantEra, eraID)

			tx, err := conway.NewConwayTransactionFromCbor(
				testutil.ParameterChangeTxCbor(
					t,
					map[uint]any{testutil.PParamUpdateKeyAdaPerUtxoByte: 0},
					test.name,
				),
			)
			require.NoError(t, err)
			err = db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
				return backfill.processBlockGovernanceLevel(
					context.Background(),
					tx,
					ocommon.NewPoint(1000, bytes.Repeat([]byte{0xCE}, 32)),
					0,
					11,
					pparams,
					txn,
					nil,
				)
			})
			stored, getErr := db.GetGovernanceProposal(
				context.Background(), tx.Id().Bytes(), 0, nil,
			)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.ErrorIs(t, getErr, models.ErrGovernanceProposalNotFound)
				return
			}
			require.NoError(t, err)
			require.NoError(t, getErr)
			require.NotNil(t, stored)
		})
	}
}
