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

package koiosparity

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestKoiosCoverageMatrixIsComplete makes a Koios response-shape change fail
// until the new field is assigned an explicit coverage class. This prevents a
// report from continuing to say PASS while silently omitting a new field.
func TestKoiosCoverageMatrixIsComplete(t *testing.T) {
	matrix := KoiosCoverageMatrix()
	byKey := make(map[string]KoiosFieldCoverage, len(matrix))
	validClasses := map[CoverageClass]bool{
		CoverageExactMatch:                true,
		CoverageDerivedMatch:              true,
		CoverageIntentionallyIncomparable: true,
		CoverageUnsupported:               true,
	}
	for _, field := range matrix {
		key := field.Endpoint + "." + field.Field
		require.NotEmpty(t, field.Endpoint)
		require.NotEmpty(t, field.Field)
		require.True(
			t,
			validClasses[field.Class],
			"invalid coverage class for %s",
			key,
		)
		require.NotEmpty(
			t,
			field.Reason,
			"coverage reason is required for %s",
			key,
		)
		_, duplicate := byKey[key]
		require.False(t, duplicate, "duplicate coverage entry for %s", key)
		byKey[key] = field
	}

	requireResponseFieldsCovered(t, byKey, "/tip", KoiosTipResp{})
	requireResponseFieldsCovered(t, byKey, "/epoch_info", KoiosEpochInfoResp{})
	requireResponseFieldsCovered(t, byKey, "/totals", KoiosTotalsResp{})
	requireResponseFieldsCovered(
		t,
		byKey,
		"/pool_history",
		KoiosPoolHistoryItem{},
	)
	// dingo #3099: KoiosAccountRewardHistoryItem was not previously enforced
	// here, so a future field added to it would go unclassified without any
	// test failing — close that gap the same way every other response
	// struct is already guarded.
	requireResponseFieldsCovered(
		t,
		byKey,
		"/account_reward_history",
		KoiosAccountRewardHistoryItem{},
	)
	// dingo #3931: /epoch_params. The response struct models the scalar
	// parameters this checker fetches; the documented fields it deliberately
	// does not model (cost_models, nonce/block_hash/extra_entropy, and the
	// Conway governance block) are pinned explicitly below so they cannot be
	// dropped from the contract without a test failing.
	requireResponseFieldsCovered(
		t,
		byKey,
		"/epoch_params",
		KoiosEpochParamsResp{},
	)

	// Fields selected by the pool-discovery helpers use function-local response
	// structs, and documented fields omitted from preview/preprod responses are
	// pinned explicitly here.
	for _, key := range []string{
		"/pool_list.pool_id_bech32",
		"/pool_updates.pool_id_bech32",
		"/pool_updates.active_epoch_no",
		"/pool_history.pool_id_bech32",
		"/epoch_info.pool_cnt",
		"/epoch_info.delegator_cnt",
		"/epoch_params.cost_models",
		"/epoch_params.nonce",
		"/epoch_params.block_hash",
		"/epoch_params.extra_entropy",
		"/epoch_params.pvt_motion_no_confidence",
		"/epoch_params.pvt_committee_normal",
		"/epoch_params.pvt_committee_no_confidence",
		"/epoch_params.pvt_hard_fork_initiation",
		"/epoch_params.pvtpp_security_group",
		"/epoch_params.dvt_motion_no_confidence",
		"/epoch_params.dvt_committee_normal",
		"/epoch_params.dvt_committee_no_confidence",
		"/epoch_params.dvt_update_to_constitution",
		"/epoch_params.dvt_hard_fork_initiation",
		"/epoch_params.dvt_p_p_network_group",
		"/epoch_params.dvt_p_p_economic_group",
		"/epoch_params.dvt_p_p_technical_group",
		"/epoch_params.dvt_p_p_gov_group",
		"/epoch_params.dvt_treasury_withdrawal",
		"/epoch_params.committee_min_size",
		"/epoch_params.committee_max_term_length",
		"/epoch_params.gov_action_lifetime",
		"/epoch_params.gov_action_deposit",
		"/epoch_params.drep_deposit",
		"/epoch_params.drep_activity",
		"/epoch_params.min_fee_ref_script_cost_per_byte",
	} {
		require.Contains(t, byKey, key)
	}
}

func requireResponseFieldsCovered(
	t *testing.T,
	coverage map[string]KoiosFieldCoverage,
	endpoint string,
	response any,
) {
	t.Helper()
	typ := reflect.TypeOf(response)
	for field := range typ.Fields() {
		jsonName, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		require.Contains(t, coverage, endpoint+"."+jsonName)
	}
}

func TestKoiosCoverageMatrixReturnsCopy(t *testing.T) {
	first := KoiosCoverageMatrix()
	require.NotEmpty(t, first)
	first[0].Field = "mutated"
	require.NotEqual(t, "mutated", KoiosCoverageMatrix()[0].Field)
}
