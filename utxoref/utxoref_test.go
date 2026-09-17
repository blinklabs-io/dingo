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

package utxoref_test

import (
	"fmt"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/utxoref"
)

// TestForInputEquality verifies that Key compares two inputs by hash and
// index the same way the fmt.Sprintf("<hash>:<index>", ...) string keys it
// replaces (across ledger, ledger/forging, and mempool's UTxO overlays) did:
// equal for a repeated (hash, index) pair, distinct when either component
// differs.
func TestForInputEquality(t *testing.T) {
	t.Parallel()

	hashA := strings.Repeat("ab", 32)
	hashB := strings.Repeat("cd", 32)

	inputA0 := shelley.NewShelleyTransactionInput(hashA, 0)
	inputA0Again := shelley.NewShelleyTransactionInput(hashA, 0)
	inputA1 := shelley.NewShelleyTransactionInput(hashA, 1)
	inputB0 := shelley.NewShelleyTransactionInput(hashB, 0)

	require.Equal(
		t,
		utxoref.ForInput(inputA0),
		utxoref.ForInput(inputA0Again),
		"same hash and index must produce equal keys",
	)
	require.NotEqual(
		t,
		utxoref.ForInput(inputA0),
		utxoref.ForInput(inputA1),
		"same hash but different index must produce distinct keys",
	)
	require.NotEqual(
		t,
		utxoref.ForInput(inputA0),
		utxoref.ForInput(inputB0),
		"same index but different hash must produce distinct keys",
	)
}

// TestForUtxoMatchesForInput verifies that a produced UTxO's Key (computed
// via ForUtxo, from utxo.Id) matches the Key that would be computed for an
// input spending that same UTxO (via ForInput). This equivalence is what
// lets a consumer look up an intra-block/mempool-overlay "created" map
// (populated with ForUtxo at production time) using a spending input's Key
// (computed with ForInput).
func TestForUtxoMatchesForInput(t *testing.T) {
	t.Parallel()

	hash := strings.Repeat("11", 32)
	inputRef := shelley.NewShelleyTransactionInput(hash, 2)
	utxo := lcommon.Utxo{Id: inputRef}

	require.Equal(
		t,
		utxoref.ForInput(inputRef),
		utxoref.ForUtxo(utxo),
		"ForUtxo(utxo) must match ForInput(the input that spends utxo)",
	)
}

// TestKeyDedupsRepeatedInput exercises the dedup idiom the overlays use: a
// "seen"/consumed set keyed by utxoref.Key collapses repeated (hash, index)
// entries to one, while distinct entries are preserved.
func TestKeyDedupsRepeatedInput(t *testing.T) {
	t.Parallel()

	hash := strings.Repeat("ef", 32)
	consumed := []lcommon.TransactionInput{
		shelley.NewShelleyTransactionInput(hash, 0),
		shelley.NewShelleyTransactionInput(hash, 0), // repeated on-chain input
		shelley.NewShelleyTransactionInput(hash, 1),
	}

	seen := make(map[utxoref.Key]struct{}, len(consumed))
	var processed int
	for _, input := range consumed {
		key := utxoref.ForInput(input)
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

// TestKeyString pins the human-readable rendering used for logging (e.g. a
// double-spend log line) to the same "<hex-hash>:<index>" shape the string
// keys this type replaces used, so existing log consumers see no format
// change.
func TestKeyString(t *testing.T) {
	t.Parallel()

	hash := strings.Repeat("11", 32)
	input := shelley.NewShelleyTransactionInput(hash, 9)
	key := utxoref.ForInput(input)

	require.Equal(t, hash+":9", key.String())
}

// TestKeyAllocationFree pins the perf property this type exists for. The
// keys it replaces were built with fmt.Sprintf("<hash>:<index>", ...) on
// every consulted or recorded input/output across ledger.LedgerView.UtxoById,
// ledger's block-application and forged-tx-validation overlays, and
// ledger/forging's block-assembly overlay -- all real-execution-path, per-
// transaction, per-input/output call sites. Building the key from
// input.Id()/input.Index() (or utxo.Id.Id()/utxo.Id.Index()) as plain value
// types must not allocate at all.
func TestKeyAllocationFree(t *testing.T) {
	// Match the type consumed inputs actually have on the hot path:
	// tx.Consumed() returns []lcommon.TransactionInput, so the loop variable
	// is already interface-typed and boxing has already happened once
	// upstream, not on every ForInput call.
	var input lcommon.TransactionInput = shelley.NewShelleyTransactionInput(
		strings.Repeat("11", 32),
		7,
	)

	allocsForInput := testing.AllocsPerRun(1000, func() {
		_ = utxoref.ForInput(input)
	})
	require.Equal(
		t,
		float64(0),
		allocsForInput,
		"ForInput must not allocate",
	)

	utxo := lcommon.Utxo{Id: input}
	allocsForUtxo := testing.AllocsPerRun(1000, func() {
		_ = utxoref.ForUtxo(utxo)
	})
	require.Equal(
		t,
		float64(0),
		allocsForUtxo,
		"ForUtxo must not allocate",
	)
}

// BenchmarkKeyOldPatternInput reproduces, verbatim, the input-side dedup-key
// construction that ledger.LedgerView.UtxoById, ledger's block-application
// and forged-tx-validation overlays, and ledger/forging's block-assembly
// overlay used before this change: a formatted "hash:index" string built
// with fmt.Sprintf, once per consulted or recorded input, purely to key a
// map. Kept for direct before/after comparison against
// BenchmarkKeyNewPatternInput.
func BenchmarkKeyOldPatternInput(b *testing.B) {
	var input lcommon.TransactionInput = shelley.NewShelleyTransactionInput(
		strings.Repeat("ab", 32),
		3,
	)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%s:%d", input.Id().String(), input.Index())
	}
}

// BenchmarkKeyNewPatternInput measures the replacement: a comparable Key
// struct built directly from input.Id() and input.Index(), with no
// formatting, no hex encoding, and no heap allocation.
func BenchmarkKeyNewPatternInput(b *testing.B) {
	var input lcommon.TransactionInput = shelley.NewShelleyTransactionInput(
		strings.Repeat("ab", 32),
		3,
	)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = utxoref.ForInput(input)
	}
}

// BenchmarkKeyOldPatternUtxo/BenchmarkKeyNewPatternUtxo repeat the same
// before/after comparison for the produced-output side (utxo.Id.Id(),
// utxo.Id.Index()), as used when recording an intra-block or forging-
// candidate "created" overlay entry.
func BenchmarkKeyOldPatternUtxo(b *testing.B) {
	input := shelley.NewShelleyTransactionInput(strings.Repeat("ab", 32), 3)
	utxo := lcommon.Utxo{Id: input}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = fmt.Sprintf("%s:%d", utxo.Id.Id().String(), utxo.Id.Index())
	}
}

func BenchmarkKeyNewPatternUtxo(b *testing.B) {
	input := shelley.NewShelleyTransactionInput(strings.Repeat("ab", 32), 3)
	utxo := lcommon.Utxo{Id: input}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = utxoref.ForUtxo(utxo)
	}
}
