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
	"github.com/stretchr/testify/require"
)

var errUnmarshalPanic = errors.New(
	"runtime error: index out of range [4] with length 2",
)

// The three targets below panic from UnmarshalCBOR, so the panic originates
// inside the real cbor.Decode call Cbor makes rather than in a substitute for
// it. That is where a decode panic actually comes from in production: the
// custom UnmarshalCBOR implementations are what index, convert and allocate
// on peer-controlled lengths. Each panic value is its own type because Cbor's
// target is a type parameter.
type panicStringTarget struct{}

func (*panicStringTarget) UnmarshalCBOR([]byte) error {
	panic("cbor: bad header")
}

type panicErrorTarget struct{}

func (*panicErrorTarget) UnmarshalCBOR([]byte) error {
	panic(errUnmarshalPanic)
}

type panicNilTarget struct{}

func (*panicNilTarget) UnmarshalCBOR([]byte) error {
	panic(nil)
}

var errTargetRejected = errors.New("target rejected the input")

// rejectingTarget returns an ordinary error, pinning that a normal decode
// failure is not reclassified as a panic.
type rejectingTarget struct{}

func (*rejectingTarget) UnmarshalCBOR([]byte) error {
	return errTargetRejected
}

func validCbor(t *testing.T) []byte {
	t.Helper()
	raw, err := cbor.Encode([]any{uint64(1), uint64(2)})
	require.NoError(t, err)
	return raw
}

// TestCborContainsDecoderPanic is the causal proof for Cbor: drop the Guard
// in Cbor and every subtest crashes the test binary instead of failing.
func TestCborContainsDecoderPanic(t *testing.T) {
	t.Parallel()
	valid := validCbor(t)

	t.Run("string value", func(t *testing.T) {
		t.Parallel()
		value, bytesRead, err := safedecode.Cbor[panicStringTarget](valid)
		require.ErrorIs(t, err, safedecode.ErrDecodePanic)
		require.ErrorContains(t, err, "cbor: bad header")
		require.Equal(t, panicStringTarget{}, value)
		require.Zero(t, bytesRead)
	})

	t.Run("error value", func(t *testing.T) {
		t.Parallel()
		_, bytesRead, err := safedecode.Cbor[panicErrorTarget](valid)
		require.ErrorIs(t, err, safedecode.ErrDecodePanic)
		require.ErrorIs(t, err, errUnmarshalPanic)
		require.Zero(t, bytesRead)
	})

	t.Run("nil value", func(t *testing.T) {
		t.Parallel()
		_, bytesRead, err := safedecode.Cbor[panicNilTarget](valid)
		require.ErrorIs(t, err, safedecode.ErrDecodePanic)
		require.Zero(t, bytesRead)
	})
}

// TestCborNonPanickingOutcomesUnchanged keeps the two outcomes that must not
// move: valid input still decodes with its byte count, and an ordinary decode
// failure still reports itself as one.
func TestCborNonPanickingOutcomesUnchanged(t *testing.T) {
	t.Parallel()
	valid := validCbor(t)

	t.Run("valid input decodes", func(t *testing.T) {
		t.Parallel()
		value, bytesRead, err := safedecode.Cbor[[]cbor.RawMessage](valid)
		require.NoError(t, err)
		require.Len(t, value, 2)
		require.Equal(t, len(valid), bytesRead)
	})

	t.Run("malformed input reports an ordinary error", func(t *testing.T) {
		t.Parallel()
		_, _, err := safedecode.Cbor[[]cbor.RawMessage](
			[]byte{0xff, 0xff, 0xff},
		)
		require.Error(t, err)
		require.NotErrorIs(t, err, safedecode.ErrDecodePanic)
	})

	t.Run("target error is passed through", func(t *testing.T) {
		t.Parallel()
		_, _, err := safedecode.Cbor[rejectingTarget](valid)
		require.ErrorIs(t, err, errTargetRejected)
		require.NotErrorIs(t, err, safedecode.ErrDecodePanic)
	})
}
