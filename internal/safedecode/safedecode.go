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

// Package safedecode contains CBOR decoder panics on externally-supplied
// bytes, so a malformed or adversarial payload fails the single decode that
// received it rather than the process that attempted it.
//
// gouroboros is not a backstop: its cbor.Value decoder recovers exactly one
// panic class (an unhashable Go map key) and re-panics every other one, so a
// decode reached from a protocol worker or an API handler goroutine has no
// containment above it unless a caller supplies some.
package safedecode

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/ledger"
)

// ErrDecodePanic wraps every panic recovered by this package. A caller that
// already branches on a decode error needs no new branch: a recovered panic
// arrives on the path it already has, and errors.Is distinguishes it from an
// ordinary malformed-input error when that distinction matters.
var ErrDecodePanic = errors.New("cbor decode panicked")

// Guard runs decode and returns its result, converting a panic into an error
// wrapping ErrDecodePanic. On the panic path the returned value is the zero
// value: decode's return values are assigned only on its normal return, so a
// panicking decode never publishes a partially-built value to the caller.
//
// Containment is sound here only because decoding is a pure function of its
// input bytes -- it builds a fresh value, takes no lock and mutates no state
// the caller shares. Do not reach for Guard around work that mutates shared
// state: recovering there converts a crash into silent corruption, which is
// worse.
//
// completed records whether decode returned normally, including when the
// panic value is nil. See blinklabs-io/gouroboros#2075.
func Guard[T any](decode func() (T, error)) (value T, err error) {
	completed := false
	defer func() {
		if completed {
			return
		}
		r := recover()
		if asErr, ok := r.(error); ok {
			err = fmt.Errorf("%w: %w", ErrDecodePanic, asErr)
			return
		}
		err = fmt.Errorf("%w: %v", ErrDecodePanic, r)
	}()
	value, err = decode()
	completed = true
	return value, err
}

// TransactionType determines the era for one transaction body while
// containing decoder panics from era-specific CBOR constructors.
func TransactionType(txCbor []byte) (uint, error) {
	return Guard(func() (uint, error) {
		return ledger.DetermineTransactionType(txCbor)
	})
}

// Transaction decodes one transaction body from CBOR, containing decoder
// panics as Guard describes.
func Transaction(txType uint, txCbor []byte) (ledger.Transaction, error) {
	return Guard(func() (ledger.Transaction, error) {
		return ledger.NewTransactionFromCbor(txType, txCbor)
	})
}
