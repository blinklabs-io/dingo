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

//go:build erastest

package erastest

import (
	"reflect"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPV11Readiness asserts the static plumbing required for dingo to
// cross the PV10→PV11 (vanRossem) intra-Conway hard fork. Unlike the
// rest of this package, it does not need the eras DevNet to be running:
// every assertion is a compile-time symbol lookup or a constant
// comparison. A run can be performed in isolation with
//
//	go test -tags erastest -run TestPV11Readiness ./internal/erastest/
//
// without docker-compose. The boundary-time no-op behavior of
// applyIntraEraHardForkRule(11) is covered by
// TestApplyIntraEraHardForkRule_UnknownMajor_NoOp in
// ledger/hardfork_rule_test.go and is not duplicated here.
func TestPV11Readiness(t *testing.T) {
	t.Run("ProtocolVersionConstants", func(t *testing.T) {
		assert.Equal(
			t, uint(11), lcommon.ProtocolVersionVanRossem,
			"common.ProtocolVersionVanRossem must equal 11",
		)
		assert.Equal(
			t, uint(11), uint(conway.MaxProtocolVersionConway),
			"conway.MaxProtocolVersionConway must cover PV11",
		)
	})

	t.Run("ConwayEraCoversPV11", func(t *testing.T) {
		assert.Equal(
			t, uint(11), eras.ConwayEraDesc.MaxMajorVersion,
			"ConwayEraDesc.MaxMajorVersion must cover PV11",
		)
		era, ok := eras.EraForVersion(11)
		require.True(
			t, ok,
			"eras.EraForVersion(11) must resolve a known era",
		)
		require.NotNil(t, era, "EraForVersion returned nil descriptor")
		assert.Equal(
			t, eras.ConwayEraDesc.Id, era.Id,
			"PV11 must resolve to Conway, not a successor era",
		)
	})

	t.Run("VanRossemGatedRulesPresent", func(t *testing.T) {
		// Both rules are PV11-gated transaction-validation rules
		// registered directly in conway.UtxoValidationRules. dingo's
		// Conway era iterates this slice in ledger/eras/conway.go, so
		// presence here is what makes the rules fire on dingo once
		// pparams.Major reaches 11.
		assertRuleInSlice(
			t,
			conway.UtxoValidationRules,
			conway.UtxoValidateDisjointRefInputs,
			"UtxoValidateDisjointRefInputs",
		)
		assertRuleInSlice(
			t,
			conway.UtxoValidationRules,
			conway.UtxoValidateCCVotingRestrictions,
			"UtxoValidateCCVotingRestrictions",
		)
	})

	t.Run("PoolValidateVrfKeyUniquenessExported", func(t *testing.T) {
		// PoolValidateVrfKeyUniqueness is the third PV11-gated rule
		// but has a per-certificate signature, so it is invoked
		// transitively from validateCertificates inside Conway's rule
		// path rather than registered in UtxoValidationRules.
		// Asserting the function exists as an exported, callable
		// symbol guards against accidental rename or unexport.
		require.NotNil(
			t, conway.PoolValidateVrfKeyUniqueness,
			"conway.PoolValidateVrfKeyUniqueness must remain "+
				"exported and callable",
		)
	})
}

// assertRuleInSlice fails the test if want is not present in slice.
// Compares by function pointer because Go function values are not
// directly comparable with ==.
func assertRuleInSlice(
	t *testing.T,
	slice []lcommon.UtxoValidationRuleFunc,
	want lcommon.UtxoValidationRuleFunc,
	name string,
) {
	t.Helper()
	wantPtr := reflect.ValueOf(want).Pointer()
	for _, fn := range slice {
		if reflect.ValueOf(fn).Pointer() == wantPtr {
			return
		}
	}
	t.Errorf(
		"rule %s not present in conway.UtxoValidationRules",
		name,
	)
}
