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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lbabbage "github.com/blinklabs-io/gouroboros/ledger/babbage"
	lconway "github.com/blinklabs-io/gouroboros/ledger/conway"
	ldijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// translatedBabbagePParams returns a Babbage parameter set and the Conway
// value translateGovState makes of it at the hard fork: upgradeConwayPParams
// copies every Babbage field, merges the Conway genesis PlutusV3 cost model
// and adds the governance fields.
func translatedBabbagePParams() (
	*lbabbage.BabbageProtocolParameters,
	*lconway.ConwayProtocolParameters,
) {
	babbageParams := testBabbagePParams()
	babbageParams.NOpt = 400
	babbageParams.A0 = &cbor.Rat{Rat: big.NewRat(1, 5)}
	babbageParams.Rho = &cbor.Rat{Rat: big.NewRat(1, 250)}
	babbageParams.Tau = &cbor.Rat{Rat: big.NewRat(3, 20)}
	babbageParams.ProtocolMinor = 1
	babbageParams.CostModels = map[uint][]int64{0: {7}, 1: {8}}

	translated := testConwayPParams()
	upgraded := lconway.UpgradePParams(*babbageParams)
	upgraded.CostModels = map[uint][]int64{0: {7}, 1: {8}, 2: {9}}
	upgraded.PoolVotingThresholds = translated.PoolVotingThresholds
	upgraded.DRepVotingThresholds = translated.DRepVotingThresholds
	upgraded.MinCommitteeSize = translated.MinCommitteeSize
	upgraded.CommitteeTermLimit = translated.CommitteeTermLimit
	upgraded.GovActionValidityPeriod = translated.GovActionValidityPeriod
	upgraded.GovActionDeposit = translated.GovActionDeposit
	upgraded.DRepDeposit = translated.DRepDeposit
	upgraded.DRepInactivityPeriod = translated.DRepInactivityPeriod
	upgraded.MinFeeRefScriptCostPerByte = translated.MinFeeRefScriptCostPerByte
	return babbageParams, &upgraded
}

func TestPreviousPParamsForEraAppliesLedgerDowngrades(t *testing.T) {
	t.Parallel()

	babbageParams, conwayParams := translatedBabbagePParams()
	// downgradeConwayPParams keeps the merged cost-model map as it is, so the
	// recovered Babbage value carries the upgrade's PlutusV3 entry.
	wantBabbage := *babbageParams
	wantBabbage.CostModels = conwayParams.CostModels
	conwayData, err := cbor.Encode(conwayParams)
	require.NoError(t, err)

	dijkstraParams := ldijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: *conwayParams,
		MaxRefScriptSizePerBlock: 1_048_576,
		MaxRefScriptSizePerTx:    204_800,
		RefScriptCostStride:      25_600,
		RefScriptCostMultiplier:  &cbor.Rat{Rat: big.NewRat(6, 5)},
	}
	dijkstraData, err := cbor.Encode(&dijkstraParams)
	require.NoError(t, err)
	dijkstraEra := int(eras.DijkstraEraDesc.Id)

	t.Run("same era keeps the payload", func(t *testing.T) {
		t.Parallel()
		got, err := previousPParamsForEra(EraConway, conwayData, EraConway)
		require.NoError(t, err)
		require.Equal(t, conwayData, got)
	})
	t.Run("Conway to Babbage", func(t *testing.T) {
		t.Parallel()
		got, err := previousPParamsForEra(EraConway, conwayData, EraBabbage)
		require.NoError(t, err)
		decoded, err := eras.DecodePParamsBabbage(got)
		require.NoError(t, err)
		require.Equal(t, &wantBabbage, decoded)
	})
	t.Run("Dijkstra to Conway", func(t *testing.T) {
		t.Parallel()
		got, err := previousPParamsForEra(dijkstraEra, dijkstraData, EraConway)
		require.NoError(t, err)
		decoded, err := eras.DecodePParamsConway(got)
		require.NoError(t, err)
		require.Equal(t, conwayParams, decoded)
	})
	t.Run("Dijkstra to Babbage", func(t *testing.T) {
		t.Parallel()
		got, err := previousPParamsForEra(dijkstraEra, dijkstraData, EraBabbage)
		require.NoError(t, err)
		decoded, err := eras.DecodePParamsBabbage(got)
		require.NoError(t, err)
		require.Equal(t, &wantBabbage, decoded)
	})
	t.Run("Babbage to Alonzo needs d and extraEntropy", func(t *testing.T) {
		t.Parallel()
		babbageData, err := cbor.Encode(babbageParams)
		require.NoError(t, err)
		_, err = previousPParamsForEra(EraBabbage, babbageData, EraAlonzo)
		require.ErrorContains(t, err,
			"Babbage-to-Alonzo protocol parameter downgrade needs values the snapshot does not carry")
	})
	t.Run("Conway to Alonzo stops at the Babbage step", func(t *testing.T) {
		t.Parallel()
		_, err := previousPParamsForEra(EraConway, conwayData, EraAlonzo)
		require.ErrorContains(t, err, "Babbage-to-Alonzo")
	})
	t.Run("missing payload", func(t *testing.T) {
		t.Parallel()
		_, err := previousPParamsForEra(EraConway, nil, EraBabbage)
		require.ErrorContains(t, err, "carries no previous protocol parameters")
	})
	t.Run("payload not in the snapshot era", func(t *testing.T) {
		t.Parallel()
		babbageData, err := cbor.Encode(babbageParams)
		require.NoError(t, err)
		_, err = previousPParamsForEra(EraConway, babbageData, EraBabbage)
		require.ErrorContains(t, err, "decoding Conway protocol parameters")
	})
	t.Run("later era", func(t *testing.T) {
		t.Parallel()
		_, err := previousPParamsForEra(EraConway, conwayData, dijkstraEra)
		require.ErrorContains(t, err, "later than snapshot era Conway")
	})
}
