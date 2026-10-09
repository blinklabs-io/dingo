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
	"github.com/blinklabs-io/gouroboros/cbor"
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

			txn := db.Transaction(context.Background(), true)
			defer txn.Release()
			err = txn.Do(func(txn *database.Txn) error {
				return backfill.processBlockGovernanceLevel(
					context.Background(),
					tx,
					ocommon.NewPoint(1000, bytes.Repeat([]byte{0xCD}, 32)),
					0,
					100,
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
				require.Nil(t, stored)
				return
			}
			require.NoError(t, err)
			require.NoError(t, getErr)
			require.NotNil(t, stored)
		})
	}
}

// TestBackfillRejectsOutOfDomainParameterChange replays ParameterChange
// proposals whose updates fall outside the Dijkstra field domains or carry a
// cost-model language ID above Word8 through the backfill path. Each must be
// refused, by the transaction decoder or by the proposal rule, before a
// governance_proposal row exists; the in-domain controls must be stored.
func TestBackfillRejectsOutOfDomainParameterChange(t *testing.T) {
	t.Parallel()

	rat := func(num, den int64) cbor.Tag {
		return cbor.Tag{Number: 30, Content: []any{num, den}}
	}
	tests := []struct {
		name     string
		dijkstra bool
		ppu      map[uint]any
		wantErr  string
	}{
		{
			name:     "dijkstra tag 37 zero",
			dijkstra: true,
			ppu:      map[uint]any{37: rat(0, 1)},
			wantErr:  "refScriptCostMultiplier",
		},
		{
			name:     "dijkstra tag 39 above one",
			dijkstra: true,
			ppu:      map[uint]any{39: rat(2, 1)},
			wantErr:  "minPoolMargin",
		},
		{
			name:     "dijkstra tag 44 above one",
			dijkstra: true,
			ppu:      map[uint]any{44: rat(2, 1)},
			wantErr:  "leiosQuorumStakeThreshold",
		},
		{
			name:     "dijkstra tag 47 negative",
			dijkstra: true,
			ppu:      map[uint]any{47: []any{int64(-1), int64(0)}},
			wantErr:  "negative integer",
		},
		{
			name:     "dijkstra tag 39 one",
			dijkstra: true,
			ppu:      map[uint]any{39: rat(1, 1)},
		},
		{
			name: "conway cost-model language 256",
			ppu: map[uint]any{testutil.PParamUpdateKeyCostModels: map[uint]any{
				256: []int64{1, 2, 3},
			}},
			wantErr: "Word8",
		},
		{
			name:     "dijkstra cost-model language 256",
			dijkstra: true,
			ppu: map[uint]any{testutil.PParamUpdateKeyCostModels: map[uint]any{
				256: []int64{1, 2, 3},
			}},
			wantErr: "Word8",
		},
		{
			name: "conway cost-model language 255",
			ppu: map[uint]any{testutil.PParamUpdateKeyCostModels: map[uint]any{
				255: []int64{1, 2, 3},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			backfill := NewBackfill(db, nil, slog.Default())

			conwayPParams := mockledger.NewMockConwayProtocolParams()
			conwayPParams.GovActionValidityPeriod = 20
			conwayPParams.ProtocolVersion.Major = 11
			var pparams lcommon.ProtocolParameters = &conwayPParams
			if test.dijkstra {
				pparams = &dijkstra.DijkstraProtocolParameters{
					ConwayProtocolParameters: conwayPParams,
				}
				conwayPParams.ProtocolVersion.Major = dijkstra.MinProtocolVersionDijkstra
			}

			txCbor := testutil.ParameterChangeTxCbor(t, test.ppu, test.name)
			var tx lcommon.Transaction
			var err error
			if test.dijkstra {
				var decoded *dijkstra.DijkstraTransaction
				if decoded, err = dijkstra.NewDijkstraTransactionFromCbor(txCbor); err == nil {
					tx = decoded
				}
			} else {
				var decoded *conway.ConwayTransaction
				if decoded, err = conway.NewConwayTransactionFromCbor(txCbor); err == nil {
					tx = decoded
				}
			}
			if err == nil {
				err = db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
					return backfill.processBlockGovernanceLevel(
						context.Background(),
						tx,
						ocommon.NewPoint(1000, bytes.Repeat([]byte{0xCF}, 32)),
						0,
						100,
						pparams,
						txn,
						nil,
					)
				})
			}
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				// A transaction the decoder refuses reaches no
				// persistence path at all.
				if tx != nil {
					_, getErr := db.GetGovernanceProposal(
						context.Background(),
						tx.Id().Bytes(),
						0,
						nil,
					)
					require.ErrorIs(
						t,
						getErr,
						models.ErrGovernanceProposalNotFound,
					)
				}
				return
			}
			require.NoError(t, err)
			stored, getErr := db.GetGovernanceProposal(
				context.Background(), tx.Id().Bytes(), 0, nil,
			)
			require.NoError(t, getErr)
			require.NotNil(t, stored)
		})
	}
}
