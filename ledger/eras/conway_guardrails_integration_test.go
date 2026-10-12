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

package eras

import (
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestValidateTxConwayRejectsProposalWithoutConstitutionGuardrailsHash(
	t *testing.T,
) {
	guardrailsHash := lcommon.Blake2b224Hash([]byte("constitution guardrails"))
	state := mockledger.NewLedgerStateBuilder().
		WithConstitutionValue(&lcommon.Constitution{
			ScriptHash: guardrailsHash.Bytes(),
		}).
		Build()
	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxProposalProcedures: []conway.ConwayProposalProcedure{{
				PPGovAction: conway.ConwayGovAction{
					Action: &conway.ConwayParameterChangeGovAction{},
				},
			}},
		},
		TxIsValid: true,
	}

	err := validateConwayWithRule(
		t,
		lcommon.UtxoValidationRuleGovActionWellFormedness,
		tx,
		state,
		&conway.ConwayProtocolParameters{},
	)
	var mismatch conway.InvalidGuardrailsScriptHashError
	require.ErrorAs(t, err, &mismatch)
	require.Empty(t, mismatch.Actual)
	require.Equal(t, guardrailsHash.Bytes(), mismatch.Expected)
}
