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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A certified endorser block's transactions are validated by the era rules and
// written to the metadata store inside the ranking block's transaction body,
// so they are validate and apply work like a ranking block's own transactions.
func TestEndorserBlockTransactionIsObservedAsValidateAndApply(t *testing.T) {
	t.Parallel()

	fx := newDijkstraCollateralReturnFixture(t, 0)
	require.NotNil(t, fx.ls.currentEra.ValidateTxFunc)
	applyFixtureEndorserBlock(
		t, fx, endorserBlockTxWithValidity(t, fx, true),
	)
	requireFixtureUtxo(t, fx, fx.tx.Hash().Bytes(), 0, true,
		"the endorser transaction must have been applied")

	count, sum := blockStageSampleCount(t, &fx.ls.metrics, blockStageValidate)
	assert.Equal(t, uint64(1), count,
		"the endorser transaction's validation must be observed as validate")
	assert.Positive(t, sum)
	count, sum = blockStageSampleCount(t, &fx.ls.metrics, blockStageApply)
	assert.Equal(t, uint64(1), count,
		"the endorser transaction's state write must be observed as apply")
	assert.Positive(t, sum)
}

// Without rule checks there is no validation to time, so only the write is
// observed.
func TestEndorserBlockTransactionWithoutRulesIsObservedAsApplyOnly(
	t *testing.T,
) {
	t.Parallel()

	fx := newDijkstraCollateralReturnFixture(t, 0)
	withoutTxValidation(fx)
	applyFixtureEndorserBlock(
		t, fx, endorserBlockTxWithValidity(t, fx, true),
	)

	count, _ := blockStageSampleCount(t, &fx.ls.metrics, blockStageValidate)
	assert.Zero(t, count)
	count, _ = blockStageSampleCount(t, &fx.ls.metrics, blockStageApply)
	assert.Equal(t, uint64(1), count)
}
