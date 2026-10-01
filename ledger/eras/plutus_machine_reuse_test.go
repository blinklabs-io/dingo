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
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPlutusEvaluateReusesMachineAcrossRedeemers proves that evaluating many
// redeemers through the cached EvalContext resolved by plutusEvalContext
// reuses cek.Machine instances. The control evaluates the same script with a
// distinct EvalContext per call, so every call constructs a Machine; the two
// runs differ only in whether the Machine pool can hit. Allocation counts are
// compared because the pool's construction counter is not exported.
//
// Not t.Parallel: testing.AllocsPerRun is a process-wide measurement.
func TestPlutusEvaluateReusesMachineAcrossRedeemers(t *testing.T) {
	const runs = 50
	script, _ := buildMintingV1Script(t)
	budget := lcommon.ExUnits{Steps: 10_000_000_000, Memory: 14_000_000}
	arg := data.NewInteger(big.NewInt(0))
	protoVersion := cek.ProtoVersion{Major: 10}
	costModel := defaultMachineCostModel(t, lang.LanguageVersionV1)

	ls := newMockLedgerState()
	ls.plutusEvalContextCache = NewPlutusEvalContextCache()
	evalContext, err := plutusEvalContext(
		ls, lang.LanguageVersionV1, protoVersion, costModel, false,
	)
	require.NoError(t, err)

	evaluate := func(ec *cek.EvalContext) lcommon.ExUnits {
		used, err := script.Evaluate(nil, arg, arg, budget, ec)
		require.NoError(t, err)
		return used
	}

	// Warm the pool entry and the decode path.
	want := evaluate(evalContext)

	// Every iteration resolves its context through the production seam.
	shared := testing.AllocsPerRun(runs, func() {
		ec, err := plutusEvalContext(
			ls, lang.LanguageVersionV1, protoVersion, costModel, false,
		)
		require.NoError(t, err)
		require.Same(t, evalContext, ec)
		require.Equal(t, want, evaluate(ec))
	})

	fresh := make([]*cek.EvalContext, runs+1)
	for i := range fresh {
		fresh[i], err = cek.NewEvalContext(
			lang.LanguageVersionV1, protoVersion, costModel,
		)
		require.NoError(t, err)
	}
	var next int
	control := testing.AllocsPerRun(runs, func() {
		require.Equal(t, want, evaluate(fresh[next]))
		next++
	})

	assert.Less(
		t,
		shared,
		control,
		"evaluating with one shared EvalContext must allocate less than "+
			"building a Machine per call; Machine pooling is not in effect",
	)
}
