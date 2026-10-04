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

package governance

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func parameterChangeTestPParams(
	dijkstra bool,
	major uint,
) lcommon.ProtocolParameters {
	pp := testConwayProtocolParameters()
	pp.ProtocolVersion.Major = major
	if dijkstra {
		return &gdijkstra.DijkstraProtocolParameters{
			ConwayProtocolParameters: *pp,
		}
	}
	return pp
}

func decodeParameterChangeTx(
	t *testing.T,
	dijkstra bool,
	txCbor []byte,
) lcommon.Transaction {
	t.Helper()
	if dijkstra {
		tx, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
		require.NoError(t, err)
		return tx
	}
	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return tx
}

// TestProcessProposalsEnforcesParameterChangeWellFormedness drives the
// persistence entry point shared by live block application, replay, and
// backfill. A ParameterChange that the transaction rules refuse must leave
// no governance_proposal row, and the protocol-version gated zero values must
// follow the version in force.
func TestProcessProposalsEnforcesParameterChangeWellFormedness(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name     string
		dijkstra bool
		major    uint
		ppu      map[uint]any
		wantErr  string
	}{
		{
			name:    "conway zero govActionDeposit at PV11",
			major:   11,
			ppu:     map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 0},
			wantErr: "govActionDeposit",
		},
		{
			name:    "conway zero poolDeposit at PV9",
			major:   9,
			ppu:     map[uint]any{testutil.PParamUpdateKeyPoolDeposit: 0},
			wantErr: "poolDeposit",
		},
		{
			name:    "conway zero collateralPercentage at PV10",
			major:   10,
			ppu:     map[uint]any{testutil.PParamUpdateKeyCollateralPercent: 0},
			wantErr: "collateralPercentage",
		},
		{
			name:    "conway zero coinsPerUTxOByte at PV10",
			major:   10,
			ppu:     map[uint]any{testutil.PParamUpdateKeyAdaPerUtxoByte: 0},
			wantErr: "coinsPerUTxOByte",
		},
		{
			name:    "conway zero nOptimalPoolCount at PV11",
			major:   11,
			ppu:     map[uint]any{testutil.PParamUpdateKeyNOpt: 0},
			wantErr: "nOptimalPoolCount",
		},
		{
			name:     "dijkstra zero govActionDeposit",
			dijkstra: true,
			major:    gdijkstra.MinProtocolVersionDijkstra,
			ppu:      map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 0},
			wantErr:  "govActionDeposit",
		},
		{
			name:     "dijkstra zero coinsPerUTxOByte",
			dijkstra: true,
			major:    gdijkstra.MinProtocolVersionDijkstra,
			ppu:      map[uint]any{testutil.PParamUpdateKeyAdaPerUtxoByte: 0},
			wantErr:  "coinsPerUTxOByte",
		},
		{
			name:  "conway zero coinsPerUTxOByte at PV9 is allowed",
			major: 9,
			ppu:   map[uint]any{testutil.PParamUpdateKeyAdaPerUtxoByte: 0},
		},
		{
			name:  "conway zero nOptimalPoolCount at PV10 is allowed",
			major: 10,
			ppu:   map[uint]any{testutil.PParamUpdateKeyNOpt: 0},
		},
		{
			name:  "conway nonzero govActionDeposit",
			major: 11,
			ppu:   map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 1},
		},
		{
			name:     "dijkstra nonzero govActionDeposit",
			dijkstra: true,
			major:    gdijkstra.MinProtocolVersionDijkstra,
			ppu:      map[uint]any{testutil.PParamUpdateKeyGovActionDeposit: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(t, &database.Config{
				DataDir: t.TempDir(),
				Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
			})
			require.NoError(t, err)
			defer dbtest.CloseDatabase(db)

			tx := decodeParameterChangeTx(
				t,
				test.dijkstra,
				testutil.ParameterChangeTxCbor(t, test.ppu, test.name),
			)
			err = ProcessProposals(
				tx,
				ocommon.Point{Slot: 100},
				0,
				100,
				20,
				parameterChangeTestPParams(test.dijkstra, test.major),
				db,
				nil,
				nil,
			)
			stored, getErr := db.GetGovernanceProposal(
				tx.Id().Bytes(), 0, nil,
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

// TestProcessProposalsRejectsProgrammaticParameterUpdateWidths covers an
// update that never went through CBOR decoding: its integer widths must be
// held to the same bounds the decoder enforces.
func TestProcessProposalsRejectsProgrammaticParameterUpdateWidths(
	t *testing.T,
) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		set     func(*conway.ConwayProtocolParameterUpdate, uint)
		max     uint
		wantErr string
	}{
		{
			name: "Word32 maxTxSize",
			set: func(u *conway.ConwayProtocolParameterUpdate, v uint) {
				u.MaxTxSize = &v
			},
			max:     1<<32 - 1,
			wantErr: "maxTxSize",
		},
		{
			name: "Word16 collateralPercentage",
			set: func(u *conway.ConwayProtocolParameterUpdate, v uint) {
				u.CollateralPercentage = &v
			},
			max:     1<<16 - 1,
			wantErr: "collateralPercentage",
		},
	} {
		for _, over := range []bool{false, true} {
			name := tc.name + " at maximum"
			if over {
				name = tc.name + " above maximum"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				db, err := dbtest.NewDatabase(t, &database.Config{
					DataDir: t.TempDir(),
					Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
				})
				require.NoError(t, err)
				defer dbtest.CloseDatabase(db)

				tx, ok := decodeParameterChangeTx(
					t,
					false,
					testutil.ParameterChangeTxCbor(
						t,
						map[uint]any{testutil.PParamUpdateKeyMaxTxSize: 1},
						name,
					),
				).(*conway.ConwayTransaction)
				require.True(t, ok)
				action, ok := tx.ProposalProcedures()[0].GovAction().(*conway.ConwayParameterChangeGovAction)
				require.True(t, ok)
				value := tc.max
				if over {
					value++
				}
				action.ParamUpdate = conway.ConwayProtocolParameterUpdate{}
				tc.set(&action.ParamUpdate, value)

				err = ProcessProposals(
					tx,
					ocommon.Point{Slot: 100},
					0,
					100,
					20,
					parameterChangeTestPParams(false, 10),
					db,
					nil,
					nil,
				)
				stored, getErr := db.GetGovernanceProposal(
					tx.Id().Bytes(), 0, nil,
				)
				if over {
					require.ErrorContains(t, err, tc.wantErr)
					require.ErrorIs(
						t, getErr, models.ErrGovernanceProposalNotFound,
					)
					require.Nil(t, stored)
					return
				}
				require.NoError(t, err)
				require.NoError(t, getErr)
				require.NotNil(t, stored)
			})
		}
	}
}
