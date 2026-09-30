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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
)

// TestRewardParametersDecentralizationIsZeroWhenCalculatedInBabbage pins the
// d that startStep reads across the Alonzo to Babbage boundary. The reward
// update runs in the epoch after the performance epoch, in that epoch's era.
// Babbage's PParams has no d field (ppDG = to (const minBound)), and the
// translated prevPParams read back as 0, so the round for the last Alonzo
// epoch uses d = 0 even though the Alonzo parameters held d = 7/10. Reading
// d from the performance epoch's Alonzo parameters overstates eta's
// expectedBlocks denominator reduction and inflates every reward of the
// round (Prime Mainnet performance epoch 39).
//
// Block counts are the exception: BBODY accumulated the performance epoch's
// BlocksMade under that epoch's curPParams, so incrBlocks skipped overlay
// slots with the Alonzo d. The d returned for block counting must stay the
// performance epoch's.
func TestRewardParametersDecentralizationIsZeroWhenCalculatedInBabbage(
	t *testing.T,
) {
	t.Parallel()

	const (
		performanceEpoch = uint64(2)
		potsEpoch        = uint64(3)
	)
	tests := []struct {
		name         string
		calcEra      uint
		expectedDRat *big.Rat
	}{
		{
			name:         "alonzo calculation keeps the performance epoch d",
			calcEra:      eras.AlonzoEraDesc.Id,
			expectedDRat: big.NewRat(7, 10),
		},
		{
			name:         "babbage calculation reads d as zero",
			calcEra:      eras.BabbageEraDesc.Id,
			expectedDRat: big.NewRat(0, 1),
		},
		{
			name:         "conway calculation reads d as zero",
			calcEra:      eras.ConwayEraDesc.Id,
			expectedDRat: big.NewRat(0, 1),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls, db := newRewardCalculationTestLedger(t)
			meta := db.Metadata()
			pparams := &alonzo.AlonzoProtocolParameters{
				NOpt:             10,
				A0:               rewardCalcRat(0, 1),
				Rho:              rewardCalcRat(1, 100),
				Tau:              rewardCalcRat(1, 5),
				Decentralization: rewardCalcRat(7, 10),
				ProtocolMajor:    6,
			}
			pparamsCbor, err := cbor.Encode(pparams)
			require.NoError(t, err)
			require.NoError(t, meta.SetEpoch(
				100, performanceEpoch, nil, nil, nil, nil,
				eras.AlonzoEraDesc.Id, 1, 100, nil,
			))
			require.NoError(t, meta.SetEpoch(
				200, potsEpoch, nil, nil, nil, nil,
				tc.calcEra, 1, 1_000, nil,
			))
			require.NoError(t, db.SetPParams(
				pparamsCbor, 100, performanceEpoch,
				eras.AlonzoEraDesc.Id, nil,
			))

			txn := db.Transaction(false)
			defer func() { _ = txn.Rollback() }()
			_, params, performanceD, err := ls.rewardParameters(
				txn,
				performanceEpoch,
				potsEpoch,
				&models.RewardAdaPots{Reserves: 100_000_000},
			)
			require.NoError(t, err)
			require.Zero(t, tc.expectedDRat.Cmp(params.Decentralization),
				"d = %s, want %s", params.Decentralization, tc.expectedDRat)
			require.Zero(t, big.NewRat(7, 10).Cmp(performanceD),
				"block-count d = %s, want 7/10", performanceD)
			require.Equal(t, big.NewRat(1, 5), params.TreasuryExpansion,
				"tau still comes from the performance epoch")
		})
	}
}
