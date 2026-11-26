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

// TestBackfillUsesPreviousEraParametersForItsBlocks indexes a Conway
// ParameterChange from a block in the epoch recorded as Dijkstra's first. A
// Conway block there takes the last Conway epoch's parameters, as transaction
// validation did, and a Dijkstra block keeps that epoch's. Either way the
// proposal is stored as the chain holds it.
func TestBackfillUsesPreviousEraParametersForItsBlocks(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		blockEra uint
		wantEra  uint
		wantNext bool
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
			wantNext: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db := newIndexerTestDB(t)
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
			if test.wantNext {
				require.Same(t, next, pparams)
			} else {
				require.Same(t, &prev, pparams)
			}

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
				)
			})
			stored, getErr := db.GetGovernanceProposal(
				context.Background(), tx.Id().Bytes(), 0, nil,
			)
			require.NoError(t, err)
			require.NoError(t, getErr)
			require.NotNil(t, stored)
		})
	}
}
