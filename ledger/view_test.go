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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractCostModelsFromPParams_Nil(t *testing.T) {
	result := extractCostModelsFromPParams(nil)
	require.Empty(t, result)
}

func TestExtractCostModelsFromPParams_Alonzo(t *testing.T) {
	pp := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100, 200, 300},
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 1)
	_, ok := result[lcommon.PlutusLanguage(1)]
	assert.True(t, ok, "expected PlutusV1 cost model")
}

func TestExtractCostModelsFromPParams_Babbage(t *testing.T) {
	pp := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100, 200, 300},
			1: {400, 500, 600},
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 2)
	_, hasV1 := result[lcommon.PlutusLanguage(1)]
	_, hasV2 := result[lcommon.PlutusLanguage(2)]
	assert.True(t, hasV1, "expected PlutusV1 cost model")
	assert.True(t, hasV2, "expected PlutusV2 cost model")
}

func TestExtractCostModelsFromPParams_Conway(t *testing.T) {
	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100, 200, 300},
			1: {400, 500, 600},
			2: {700, 800, 900},
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 3)
	_, hasV1 := result[lcommon.PlutusLanguage(1)]
	_, hasV2 := result[lcommon.PlutusLanguage(2)]
	_, hasV3 := result[lcommon.PlutusLanguage(3)]
	assert.True(t, hasV1, "expected PlutusV1 cost model")
	assert.True(t, hasV2, "expected PlutusV2 cost model")
	assert.True(t, hasV3, "expected PlutusV3 cost model")
}

func TestExtractCostModelsFromPParams_NilCostModels(t *testing.T) {
	pp := &babbage.BabbageProtocolParameters{
		CostModels: nil,
	}
	result := extractCostModelsFromPParams(pp)
	require.Empty(t, result)
}

func TestExtractCostModelsFromPParams_SkipsUnknownVersions(
	t *testing.T,
) {
	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {100},
			1: {200},
			2: {300},
			3: {400}, // unknown version, should be skipped
			9: {500}, // unknown version, should be skipped
		},
	}
	result := extractCostModelsFromPParams(pp)
	require.Len(t, result, 3,
		"should only include versions 0-2")
}

func TestCostModels_WithCurrentPParams(t *testing.T) {
	ls := &LedgerState{
		currentPParams: &conway.ConwayProtocolParameters{
			CostModels: map[uint][]int64{
				0: {1, 2, 3},
				1: {4, 5, 6},
				2: {7, 8, 9},
			},
		},
	}
	lv := &LedgerView{ls: ls}
	result := lv.CostModels()
	require.Len(t, result, 3)
}

func TestCostModels_NilPParams(t *testing.T) {
	ls := &LedgerState{
		currentPParams: nil,
	}
	lv := &LedgerView{ls: ls}
	result := lv.CostModels()
	require.NotNil(t, result,
		"should return empty map, not nil")
	require.Empty(t, result)
}
