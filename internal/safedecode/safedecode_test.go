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

package safedecode_test

import (
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
)

func TestGuardPassesThroughNormalReturns(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		value, err := safedecode.Guard(func() (int, error) { return 42, nil })
		require.NoError(t, err)
		require.Equal(t, 42, value)
	})
	t.Run("ordinary error is not reported as a panic", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("malformed")
		value, err := safedecode.Guard(
			func() (int, error) { return 0, sentinel },
		)
		require.ErrorIs(t, err, sentinel)
		require.NotErrorIs(t, err, safedecode.ErrDecodePanic)
		require.Equal(t, 0, value)
	})
}

// TestGuardContainsPanics drives the production recovery with decoders that
// genuinely panic. Remove the recovery from Guard and every subtest here
// takes the test binary down instead of failing.
func TestGuardContainsPanics(t *testing.T) {
	t.Parallel()

	inner := errors.New("index out of range")
	tests := []struct {
		name  string
		panic func()
		// wrapped is the panic value when it is itself an error, so the
		// caller can still classify the underlying fault.
		wrapped error
	}{
		{name: "string value", panic: func() { panic("boom") }},
		{
			name:    "error value",
			panic:   func() { panic(inner) },
			wrapped: inner,
		},
		{name: "nil value", panic: func() { panic(nil) }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			value, err := safedecode.Guard(func() (*int, error) {
				testCase.panic()
				return nil, nil
			})
			require.ErrorIs(t, err, safedecode.ErrDecodePanic)
			require.Nil(t, value)
			if testCase.wrapped != nil {
				require.ErrorIs(t, err, testCase.wrapped)
			}
		})
	}
}

// TestGuardNilPanicIsNotSilentSuccess pins that a nil panic is reported as a
// decode failure rather than inferred to be a normal return from recover's
// value.
func TestGuardNilPanicIsNotSilentSuccess(t *testing.T) {
	t.Parallel()

	value, err := safedecode.Guard(func() (string, error) {
		panic(nil)
	})
	require.Error(t, err)
	require.ErrorIs(t, err, safedecode.ErrDecodePanic)
	require.Empty(t, value)
}

func TestTransaction(t *testing.T) {
	t.Parallel()

	t.Run("malformed body returns an ordinary error", func(t *testing.T) {
		t.Parallel()
		tx, err := safedecode.Transaction(6, []byte{0xff, 0xff, 0xff})
		require.Error(t, err)
		require.NotErrorIs(t, err, safedecode.ErrDecodePanic)
		require.Nil(t, tx)
	})
	t.Run("unknown era returns an ordinary error", func(t *testing.T) {
		t.Parallel()
		tx, err := safedecode.Transaction(9999, []byte{0x80})
		require.Error(t, err)
		require.NotErrorIs(t, err, safedecode.ErrDecodePanic)
		require.Nil(t, tx)
	})
	t.Run("single transaction and trailing data", func(t *testing.T) {
		t.Parallel()
		body := map[uint]any{
			0: []any{[]any{make([]byte, 32), uint64(0)}},
			1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1_000_000)}},
			2: uint64(200_000),
			3: uint64(100),
		}
		valid, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
		require.NoError(t, err)
		tx, err := safedecode.Transaction(uint(alonzo.EraIdAlonzo), valid)
		require.NoError(t, err)
		require.NotNil(t, tx)
		tx, err = safedecode.Transaction(uint(alonzo.EraIdAlonzo), append(valid, 0))
		require.ErrorIs(t, err, safedecode.ErrTrailingData)
		require.Nil(t, tx)
	})
}

// Era constructors differ in whether the decoded value keeps the consumed
// prefix or the caller's whole slice, so trailing data must be rejected in
// every era rather than inferred from the stored CBOR length.
func TestTransactionRejectsTrailingDataInEveryEra(t *testing.T) {
	t.Parallel()

	body := map[uint]any{
		0: []any{[]any{make([]byte, 32), uint64(0)}},
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1_000_000)}},
		2: uint64(200_000),
		3: uint64(100),
	}
	shelleyStyle, err := cbor.Encode([]any{body, map[uint]any{}, nil})
	require.NoError(t, err)
	alonzoStyle, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		txType uint
		valid  []byte
	}{
		{"shelley", ledger.TxTypeShelley, shelleyStyle},
		{"allegra", ledger.TxTypeAllegra, shelleyStyle},
		{"mary", ledger.TxTypeMary, shelleyStyle},
		{"alonzo", ledger.TxTypeAlonzo, alonzoStyle},
		{"babbage", ledger.TxTypeBabbage, alonzoStyle},
		{"conway", ledger.TxTypeConway, alonzoStyle},
		{"dijkstra", ledger.TxTypeDijkstra, alonzoStyle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx, err := safedecode.Transaction(tc.txType, tc.valid)
			require.NoError(t, err)
			require.NotNil(t, tx)
			padded := append(append([]byte{}, tc.valid...), 0)
			tx, err = safedecode.Transaction(tc.txType, padded)
			require.ErrorIs(t, err, safedecode.ErrTrailingData)
			require.Nil(t, tx)
		})
	}
}
