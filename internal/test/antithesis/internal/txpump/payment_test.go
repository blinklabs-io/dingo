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

package txpump

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sampleAddr is a 29-byte enterprise address (type 0x60) with an all-zeros
// payment key hash, valid for devnet testing.
var sampleAddr = []byte{
	0x60, // enterprise address discriminant
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
}

var sampleHash = "" +
	"abcdef1234567890abcdef1234567890" +
	"abcdef1234567890abcdef1234567890"

func validParams() PaymentParams {
	return PaymentParams{
		Inputs: []UTxO{
			{TxHash: sampleHash, Index: 0, Amount: 10_000_000},
		},
		ToAddr:     sampleAddr,
		ChangeAddr: sampleAddr,
		SendAmount: 5_000_000,
		Change:     4_800_000,
	}
}

func TestBuildPayment_Success(t *testing.T) {
	txBytes, err := BuildPayment(validParams())
	require.NoError(t, err)
	assert.NotEmpty(t, txBytes, "encoded transaction should not be empty")
}

func TestBuildPayment_NoInputs(t *testing.T) {
	p := validParams()
	p.Inputs = nil
	_, err := BuildPayment(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "input")
}

func TestBuildPayment_EmptyToAddr(t *testing.T) {
	p := validParams()
	p.ToAddr = nil
	_, err := BuildPayment(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "address")
}

func TestBuildPayment_SendAmountBelowMinimum(t *testing.T) {
	p := validParams()
	p.SendAmount = minSendAmount - 1
	_, err := BuildPayment(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "minimum")
}

func TestBuildPayment_InvalidTxHash(t *testing.T) {
	p := validParams()
	p.Inputs[0].TxHash = "not-hex!"
	_, err := BuildPayment(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tx hash")
}

func TestBuildPayment_NoChange(t *testing.T) {
	// Zero change should produce a transaction with only the payment output.
	p := validParams()
	p.SendAmount = 9_800_000
	p.Change = 0
	txBytes, err := BuildPayment(p)
	require.NoError(t, err)
	assert.NotEmpty(t, txBytes)
}

func TestBuildPayment_IsDeterministic(t *testing.T) {
	// Two calls with identical params must produce identical bytes.
	p := validParams()
	a, err := BuildPayment(p)
	require.NoError(t, err)
	b, err := BuildPayment(p)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildPayment must be deterministic")
}

func TestBuildPayment_MultipleInputs(t *testing.T) {
	p := PaymentParams{
		Inputs: []UTxO{
			{TxHash: sampleHash, Index: 0, Amount: 3_000_000},
			{TxHash: sampleHash, Index: 1, Amount: 3_000_000},
		},
		ToAddr:     sampleAddr,
		ChangeAddr: sampleAddr,
		SendAmount: 5_000_000,
		Change:     800_000,
	}
	txBytes, err := BuildPayment(p)
	require.NoError(t, err)
	assert.NotEmpty(t, txBytes)
}

func TestBuildPayment_MismatchedTotals(t *testing.T) {
	p := validParams()
	p.Change = 4_700_000

	_, err := BuildPayment(p)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not equal outputs+fee")
}
