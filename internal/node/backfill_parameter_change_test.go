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

// TestBackfillRejectsMalformedParameterChange applies ParameterChange
// proposals through the backfill governance path, which stores transactions
// the chain already accepted without validating them. A proposal the
// transaction rules refuse must be refused here too, and leave no
// governance_proposal row; the nearest valid value must be stored.
func TestBackfillRejectsMalformedParameterChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dijkstra bool
		major    uint
		ppu      map[uint]any
		wantErr  string
	}{
		{
			name:    "conway zero govActionDeposit",
			major:   11,
			ppu:     map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 0},
			wantErr: "govActionDeposit",
		},
		{
			name:  "conway nonzero govActionDeposit",
			major: 11,
			ppu:   map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 1},
		},
		{
			name:  "conway zero coinsPerUTxOByte at PV9 is allowed",
			major: 9,
			ppu:   map[uint]any{testutil.PParamUpdateKeyAdaPerUtxoByte: 0},
		},
		{
			name:    "conway zero coinsPerUTxOByte at PV10",
			major:   10,
			ppu:     map[uint]any{testutil.PParamUpdateKeyAdaPerUtxoByte: 0},
			wantErr: "coinsPerUTxOByte",
		},
		{
			name:     "dijkstra zero govActionDeposit",
			dijkstra: true,
			major:    dijkstra.MinProtocolVersionDijkstra,
			ppu:      map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 0},
			wantErr:  "govActionDeposit",
		},
		{
			name:     "dijkstra nonzero govActionDeposit",
			dijkstra: true,
			major:    dijkstra.MinProtocolVersionDijkstra,
			ppu:      map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			backfill := NewBackfill(db, nil, slog.Default())

			txCbor := testutil.ParameterChangeTxCbor(t, test.ppu, test.name)
			var tx lcommon.Transaction
			var err error
			if test.dijkstra {
				tx, err = dijkstra.NewDijkstraTransactionFromCbor(txCbor)
			} else {
				tx, err = conway.NewConwayTransactionFromCbor(txCbor)
			}
			require.NoError(t, err)

			conwayPParams := mockledger.NewMockConwayProtocolParams()
			conwayPParams.GovActionValidityPeriod = 20
			conwayPParams.ProtocolVersion.Major = test.major
			var pparams lcommon.ProtocolParameters = &conwayPParams
			if test.dijkstra {
				pparams = &dijkstra.DijkstraProtocolParameters{
					ConwayProtocolParameters: conwayPParams,
				}
			}

			txn := db.Transaction(true)
			defer txn.Release()
			err = txn.Do(func(txn *database.Txn) error {
				return backfill.processBlockGovernanceLevel(
					tx,
					ocommon.NewPoint(1000, bytes.Repeat([]byte{0xCD}, 32)),
					0,
					100,
					pparams,
					txn,
					nil,
				)
			})
			stored, getErr := db.GetGovernanceProposal(tx.Id().Bytes(), 0, nil)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				require.ErrorIs(t, getErr, models.ErrGovernanceProposalNotFound)
				require.Nil(t, stored)
				return
			}
			require.NoError(t, err)
			require.NoError(t, getErr)
			require.NotNil(t, stored)
		})
	}
}
