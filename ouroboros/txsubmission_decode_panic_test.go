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

package ouroboros

import (
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/stretchr/testify/require"
)

// txsubmissionPanicReply builds a well-formed two-body reply whose sizes and
// hashes all match, so nothing but the decode can reject it.
func txsubmissionPanicReply(
	t *testing.T,
) ([]txsubmission.TxIdAndSize, []txsubmission.TxBody) {
	t.Helper()
	fixtures := txsubmissionTestFixtures(t)[:2]
	requested := make([]txsubmission.TxIdAndSize, 0, len(fixtures))
	returned := make([]txsubmission.TxBody, 0, len(fixtures))
	for _, fixture := range fixtures {
		requested = append(requested, txsubmission.TxIdAndSize{
			TxId: fixture.txId,
			Size: uint32(len(fixture.body)), // #nosec G115 -- real fixture
		})
		returned = append(returned, txsubmission.TxBody{
			EraId:  fixture.txId.EraId,
			TxBody: fixture.body,
		})
	}
	return requested, returned
}

// TestValidateTxsubmissionReplyContainsDecoderPanic covers the transaction-body
// half of blinklabs-io/gouroboros#2075: a peer
// body whose bytes panic the ledger decoder must be rejected as a decode
// failure, not unwound into the per-peer txsubmission goroutine, which has no
// recover above it and would take the node process down. Drop the containment
// and each subtest crashes the test binary rather than failing.
func TestValidateTxsubmissionReplyContainsDecoderPanic(t *testing.T) {
	t.Parallel()

	requested, returned := txsubmissionPanicReply(t)
	inner := errors.New("runtime error: index out of range [4] with length 2")
	tests := []struct {
		name    string
		panic   func()
		wrapped error
	}{
		{name: "string value", panic: func() { panic("cbor: bad header") }},
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
			validated, err := validateTxsubmissionReply(
				requested,
				returned,
				func(uint, []byte) (gledger.Transaction, error) {
					testCase.panic()
					return nil, nil
				},
			)
			require.ErrorIs(t, err, safedecode.ErrDecodePanic)
			require.ErrorContains(
				t,
				err,
				"txsubmission reply transaction 0 decode failed",
			)
			if testCase.wrapped != nil {
				require.ErrorIs(t, err, testCase.wrapped)
			}
			// The reply is rejected outright: no body reaches admission, so
			// a panicking decode can never mark a peer's reply valid.
			require.Nil(t, validated)
		})
	}
}

// TestValidateTxsubmissionReplyPanicDropsWholeReply pins that a panic on one
// body discards the bodies that decoded before it. A partially valid batch
// from a peer that can crash the decoder is not trusted, matching how an
// ordinary decode failure is handled.
func TestValidateTxsubmissionReplyPanicDropsWholeReply(t *testing.T) {
	t.Parallel()

	requested, returned := txsubmissionPanicReply(t)
	var decoded int
	validated, err := validateTxsubmissionReply(
		requested,
		returned,
		func(txType uint, txCbor []byte) (gledger.Transaction, error) {
			decoded++
			if decoded == 2 {
				panic("cbor: bad header")
			}
			return gledger.NewTransactionFromCbor(txType, txCbor)
		},
	)
	require.ErrorIs(t, err, safedecode.ErrDecodePanic)
	require.ErrorContains(
		t,
		err,
		"txsubmission reply transaction 1 decode failed",
	)
	require.Nil(t, validated)
	require.Equal(t, 2, decoded)
}

// TestValidateTxsubmissionReplyNonPanickingFailuresUnchanged keeps the two
// outcomes that must not move: a valid reply still decodes, and a
// malformed-but-non-panicking body still produces the plain decode error it
// produced before, classified as an ordinary failure rather than a panic.
func TestValidateTxsubmissionReplyNonPanickingFailuresUnchanged(t *testing.T) {
	t.Parallel()

	requested, returned := txsubmissionPanicReply(t)

	t.Run("valid reply decodes", func(t *testing.T) {
		t.Parallel()
		validated, err := validateTxsubmissionReply(
			requested,
			returned,
			gledger.NewTransactionFromCbor,
		)
		require.NoError(t, err)
		require.Len(t, validated, len(returned))
	})

	t.Run("malformed body reports an ordinary decode error", func(t *testing.T) {
		t.Parallel()
		corrupt := []txsubmission.TxBody{{
			EraId:  returned[0].EraId,
			TxBody: []byte{0xff, 0xff, 0xff},
		}}
		want := []txsubmission.TxIdAndSize{{
			TxId: requested[0].TxId,
			Size: 3,
		}}
		validated, err := validateTxsubmissionReply(
			want,
			corrupt,
			gledger.NewTransactionFromCbor,
		)
		require.Error(t, err)
		require.ErrorContains(
			t,
			err,
			"txsubmission reply transaction 0 decode failed",
		)
		require.NotErrorIs(t, err, safedecode.ErrDecodePanic)
		require.Nil(t, validated)
	})
}
