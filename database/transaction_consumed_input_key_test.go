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

package database

import (
	"fmt"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// TestConsumedInputKeyEquality verifies that consumedInputKey compares two
// inputs by hash and index the same way the fmt.Sprintf("%x:%d", ...) string
// key it replaced did: equal for a repeated (hash, index) pair, distinct
// when either component differs. ensureTransactionConsumedUtxos and
// ensureGapConsumedUtxos rely on this equality to dedup a transaction's
// Consumed() set, which can legitimately list the same input more than once.
func TestConsumedInputKeyEquality(t *testing.T) {
	t.Parallel()

	hashA := strings.Repeat("ab", 32)
	hashB := strings.Repeat("cd", 32)

	inputA0 := shelley.NewShelleyTransactionInput(hashA, 0)
	inputA0Again := shelley.NewShelleyTransactionInput(hashA, 0)
	inputA1 := shelley.NewShelleyTransactionInput(hashA, 1)
	inputB0 := shelley.NewShelleyTransactionInput(hashB, 0)

	require.Equal(
		t,
		newConsumedInputKey(inputA0),
		newConsumedInputKey(inputA0Again),
		"same hash and index must produce equal keys",
	)
	require.NotEqual(
		t,
		newConsumedInputKey(inputA0),
		newConsumedInputKey(inputA1),
		"same hash but different index must produce distinct keys",
	)
	require.NotEqual(
		t,
		newConsumedInputKey(inputA0),
		newConsumedInputKey(inputB0),
		"same index but different hash must produce distinct keys",
	)
}

// TestConsumedInputKeyDedupsRepeatedInput exercises the exact dedup idiom
// used by ensureTransactionConsumedUtxos/ensureGapConsumedUtxos: a "seen"
// set keyed by consumedInputKey collapses a Consumed() slice that lists the
// same input twice down to one entry, while distinct inputs are preserved.
func TestConsumedInputKeyDedupsRepeatedInput(t *testing.T) {
	t.Parallel()

	hash := strings.Repeat("ef", 32)
	consumed := []shelley.ShelleyTransactionInput{
		shelley.NewShelleyTransactionInput(hash, 0),
		shelley.NewShelleyTransactionInput(hash, 0), // repeated on-chain input
		shelley.NewShelleyTransactionInput(hash, 1),
	}

	seen := make(map[consumedInputKey]struct{}, len(consumed))
	var processed int
	for _, input := range consumed {
		key := newConsumedInputKey(input)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		processed++
	}

	require.Equal(
		t,
		2,
		processed,
		"the repeated (hash, index 0) input must be processed only once",
	)
}

// TestConsumedInputKeyAllocationFree pins the perf property this type
// exists for. The dedup key it replaced was built with
// fmt.Sprintf("%x:%d", inputTxId, input.Index()), which profiling on a live
// Preview-sync node showed allocating on essentially every consumed input
// across every applied transaction (encoding/hex.EncodeToString and
// fmt.Sprintf together accounted for a double-digit percentage of the
// process's cumulative heap allocations). Building the key from
// input.Id()/input.Index() as plain value types must not allocate at all.
func TestConsumedInputKeyAllocationFree(t *testing.T) {
	// Match the type consumed inputs actually have on the hot path:
	// tx.Consumed() returns []lcommon.TransactionInput, so the loop variable
	// is already interface-typed and boxing has already happened once
	// upstream, not on every newConsumedInputKey call.
	var input lcommon.TransactionInput = shelley.NewShelleyTransactionInput(
		strings.Repeat("11", 32),
		7,
	)

	allocs := testing.AllocsPerRun(1000, func() {
		_ = newConsumedInputKey(input)
	})

	require.Equal(
		t,
		float64(0),
		allocs,
		"constructing the consumed-input dedup key must not allocate",
	)
}

// BenchmarkConsumedInputKeyOldPattern reproduces, verbatim, the dedup-key
// construction ensureTransactionConsumedUtxos and ensureGapConsumedUtxos
// used before this change: a hex-formatted "hash:index" string built with
// fmt.Sprintf, once per consumed input, purely to key a same-transaction
// "seen" dedup set. Kept for direct before/after comparison against
// BenchmarkConsumedInputKeyNewPattern.
func BenchmarkConsumedInputKeyOldPattern(b *testing.B) {
	var input lcommon.TransactionInput = shelley.NewShelleyTransactionInput(
		strings.Repeat("ab", 32),
		3,
	)
	inputTxId := ledgerInputIDBytes(input)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%x:%d", inputTxId, input.Index())
	}
}

// BenchmarkConsumedInputKeyNewPattern measures the replacement: a
// comparable consumedInputKey struct built directly from input.Id() and
// input.Index(), with no formatting and no heap allocation. input is
// interface-typed here to match tx.Consumed()'s []lcommon.TransactionInput,
// the type consumed inputs actually have on the hot path.
func BenchmarkConsumedInputKeyNewPattern(b *testing.B) {
	var input lcommon.TransactionInput = shelley.NewShelleyTransactionInput(
		strings.Repeat("ab", 32),
		3,
	)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = newConsumedInputKey(input)
	}
}
