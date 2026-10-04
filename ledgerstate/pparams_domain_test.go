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

package ledgerstate

import (
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lconway "github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// TestValidatePParamsDataEnforcesCddlDomains checks that protocol parameters
// read from an imported snapshot are held to the same CDDL integer widths and
// language-ID bounds as a proposed update, rather than being accepted because
// the Go field is wider than the wire type.
func TestValidatePParamsDataEnforcesCddlDomains(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*lconway.ConwayProtocolParameters)
		wantErr string
	}{
		{name: "baseline", mutate: func(*lconway.ConwayProtocolParameters) {}},
		{
			name: "maxTxSize at Word32 maximum",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.MaxTxSize = math.MaxUint32
			},
		},
		{
			name: "maxTxSize above Word32",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.MaxTxSize = math.MaxUint32 + 1
			},
			wantErr: "maxTxSize",
		},
		{
			name: "maxBlockBodySize above Word32",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.MaxBlockBodySize = math.MaxUint32 + 1
			},
			wantErr: "maxBlockBodySize",
		},
		{
			name: "collateralPercentage at Word16 maximum",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.CollateralPercentage = math.MaxUint16
			},
		},
		{
			name: "collateralPercentage above Word16",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.CollateralPercentage = math.MaxUint16 + 1
			},
			wantErr: "collateralPercentage",
		},
		{
			name: "nOpt above Word16",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.NOpt = math.MaxUint16 + 1
			},
			wantErr: "nOpt",
		},
		{
			name: "committeeTermLimit above Word32",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.CommitteeTermLimit = math.MaxUint32 + 1
			},
			wantErr: "committeeTermLimit",
		},
		{
			name: "cost model language ID at Word8 maximum",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.CostModels[255] = []int64{0}
			},
		},
		{
			name: "cost model language ID above Word8",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.CostModels[256] = []int64{0}
			},
			wantErr: "256",
		},
		{
			name: "execution price below zero",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.ExecutionCosts.MemPrice = &cbor.Rat{Rat: big.NewRat(-1, 2)}
			},
			wantErr: "executionCosts",
		},
		{
			name: "voting threshold above one",
			mutate: func(p *lconway.ConwayProtocolParameters) {
				p.PoolVotingThresholds.CommitteeNormal = cbor.Rat{
					Rat: big.NewRat(3, 2),
				}
			},
			wantErr: "poolVotingThresholds",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			pparams := testConwayPParams()
			test.mutate(pparams)
			data, err := cbor.Encode(pparams)
			require.NoError(t, err)

			err = validatePParamsData(EraConway, data)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}
