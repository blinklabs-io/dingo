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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/plutusv4script"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// storedCborState resolves every output with stored CBOR, as outputs decoded
// from the database are in Dingo. The mock outputs are built in memory and
// carry none, which would leave the phase-2 key unavailable. The stored bytes
// are the output's Go representation, so distinct outputs keep distinct
// bytes.
type storedCborState struct {
	*mockLedgerState
}

type storedCborOutput struct {
	lcommon.TransactionOutput
	raw []byte
}

func (o storedCborOutput) Cbor() []byte { return o.raw }

func (s storedCborState) UtxoById(input lcommon.TransactionInput) (lcommon.Utxo, error) {
	utxo, err := s.mockLedgerState.UtxoById(input)
	if err != nil || utxo.Output == nil {
		return utxo, err
	}
	utxo.Output = storedCborOutput{
		TransactionOutput: utxo.Output,
		raw:               []byte(fmt.Sprintf("%#v", utxo.Output)),
	}
	return utxo, nil
}

// phase2CachingState is a mock ledger state that provides a
// Phase2ResultCache, as ledger.LedgerView does.
type phase2CachingState struct {
	storedCborState
	cache *Phase2ResultCache
}

func (s phase2CachingState) Phase2ResultCache() *Phase2ResultCache {
	return s.cache
}

// countDijkstraPhase2 replaces validateDijkstraPhase2 with a wrapper that
// counts its calls, for the duration of the test. Tests that use it must not
// run in parallel.
func countDijkstraPhase2(t *testing.T) *int {
	t.Helper()
	calls := 0
	original := validateDijkstraPhase2
	validateDijkstraPhase2 = func(
		tx lcommon.Transaction,
		slot uint64,
		ls lcommon.LedgerState,
		pp lcommon.ProtocolParameters,
	) error {
		calls++
		return original(tx, slot, ls, pp)
	}
	t.Cleanup(func() { validateDijkstraPhase2 = original })
	return &calls
}

// v4ChildSpec guards a sub-transaction with a script that succeeds or, when
// fail is set, fails.
func v4ChildSpec(t *testing.T, fail bool) v4ContextSpec {
	t.Helper()
	tag := int64(0)
	if fail {
		tag = 1
	}
	return v4ContextSpec{
		childScripts: []lcommon.PlutusV4Script{
			plutusv4script.MaybeScript(t, plutusv4script.TxInfoSubTxIx, tag),
		},
	}
}

func TestPhase2ResultCacheSkipsRepeatedDijkstraEvaluation(t *testing.T) {
	calls := countDijkstraPhase2(t)
	spec := v4ChildSpec(t, false)
	fixture := newV4ContextFixture(t, spec)
	tx := fixture.wire(t, spec)
	ls := phase2CachingState{
		storedCborState: storedCborState{fixture.state},
		cache:           NewPhase2ResultCache(),
	}
	for range 3 {
		require.NoError(t, ValidateTxDijkstra(tx, 0, ls, fixture.params))
	}
	require.Equal(t, 1, *calls, "scripts must run once")

	*calls = 0
	for range 2 {
		require.NoError(t, ValidateTxDijkstra(tx, 0, fixture.state, fixture.params))
	}
	require.Equal(t, 2, *calls, "without a cache every validation runs the scripts")
}

func TestPhase2ResultCacheDoesNotCacheDijkstraFailure(t *testing.T) {
	calls := countDijkstraPhase2(t)
	spec := v4ChildSpec(t, true)
	fixture := newV4ContextFixture(t, spec)
	tx := fixture.wire(t, spec)
	ls := phase2CachingState{
		storedCborState: storedCborState{fixture.state},
		cache:           NewPhase2ResultCache(),
	}
	for range 2 {
		requireV4ScriptFailure(t, ValidateTxDijkstra(tx, 0, ls, fixture.params))
	}
	require.Equal(t, 2, *calls, "a failure must be re-evaluated")
}

func TestDijkstraPhase2ResultKeyCoversEveryLevel(t *testing.T) {
	t.Parallel()
	spec := v4ChildSpec(t, false)
	fixture := newV4ContextFixture(t, spec)
	tx := fixture.wire(t, spec)
	pp := &fixture.params.ConwayProtocolParameters
	subBodies := lcommon.SubTransactionBodiesFromTransaction(tx)
	require.NotEmpty(t, subBodies)
	childInputs := subBodies[0].Inputs()
	require.NotEmpty(t, childInputs, "the fixture's child must spend an input")

	_, ok := dijkstraPhase2ResultKey(tx, fixture.state, pp)
	require.False(t, ok, "outputs without stored CBOR must not produce a key")

	base, ok := dijkstraPhase2ResultKey(tx, storedCborState{fixture.state}, pp)
	require.True(t, ok)
	again, ok := dijkstraPhase2ResultKey(tx, storedCborState{fixture.state}, pp)
	require.True(t, ok)
	require.Equal(t, base, again, "the key must be stable")

	// restate copies the fixture's UTxOs for every level's inputs, resolving
	// the child's first input through swap.
	restate := func(swap func(lcommon.Utxo) (lcommon.TransactionOutput, bool)) *mockLedgerState {
		state := newMockLedgerState()
		for _, body := range append([]lcommon.TransactionBody{tx}, subBodies...) {
			for _, input := range append(body.Inputs(), body.ReferenceInputs()...) {
				utxo, err := fixture.state.UtxoById(input)
				require.NoError(t, err)
				output := utxo.Output
				if input.String() == childInputs[0].String() {
					var keep bool
					if output, keep = swap(utxo); !keep {
						continue
					}
				}
				state.addUtxo(input, output)
			}
		}
		return state
	}
	topUtxo, err := fixture.state.UtxoById(tx.Inputs()[0])
	require.NoError(t, err)

	// The child's resolved output is part of its script context.
	changed := restate(func(lcommon.Utxo) (lcommon.TransactionOutput, bool) {
		return topUtxo.Output, true
	})
	changedKey, ok := dijkstraPhase2ResultKey(tx, storedCborState{changed}, pp)
	require.True(t, ok)
	require.NotEqual(t, base, changedKey, "a child's resolved output must change the key")

	// A child input that no longer resolves leaves the outcome to evaluation.
	missing := restate(func(lcommon.Utxo) (lcommon.TransactionOutput, bool) {
		return nil, false
	})
	_, ok = dijkstraPhase2ResultKey(tx, storedCborState{missing}, pp)
	require.False(t, ok, "an unresolved child input must not produce a key")
}
