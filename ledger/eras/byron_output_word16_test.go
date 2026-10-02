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
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// byronProducedLimit is the number of outputs a Byron transaction adds to the
// UTxO set: the reference indexes outputs with a Word16.
const byronProducedLimit = math.MaxUint16 + 1

// byronWord16Tx builds a real Byron transaction spending one input worth
// inputAmount into count outputs of one lovelace each, with the output at
// index byronProducedLimit (when present) set to tailAmount and tailAddr.
func byronWord16Tx(
	t *testing.T,
	count int,
	tailAmount uint64,
	tailAddr lcommon.Address,
	inputAmount uint64,
) (*byron.ByronTransaction, *mockLedgerState) {
	t.Helper()
	input, err := byron.NewByronTransactionInput(
		strings.Repeat("ab", lcommon.Blake2b256Size),
		0,
	)
	require.NoError(t, err)
	addr := newByronAddressForNetwork(t, nil, 0x01)
	outputs := make([]byron.ByronTransactionOutput, count)
	for i := range outputs {
		outputs[i] = byron.ByronTransactionOutput{
			OutputAddress: addr,
			OutputAmount:  1,
		}
	}
	if count > byronProducedLimit {
		outputs[byronProducedLimit] = byron.ByronTransactionOutput{
			OutputAddress: tailAddr,
			OutputAmount:  tailAmount,
		}
	}
	tx := &byron.ByronTransaction{
		Body: byron.ByronTransactionBody{
			TxInputs:  []byron.ByronTransactionInput{input},
			TxOutputs: outputs,
		},
	}
	ls := newNetworkTestLedgerState(byron.MainnetProtocolMagic)
	// A 1000-lovelace fee floor (the mock's policy is scaled by 10^9), so the
	// minimum-fee rule binds rather than accepting any balance.
	ls.byronFeeSummand = 1_000 * 1_000_000_000
	ls.addUtxo(input, newTestOutputWithAddress(inputAmount, addr))
	return tx, ls
}

// TestByronBalancesUseWord16OutputView pins that the fee and value checks
// balance only outputs 0 through 65535, the outputs the transaction stores,
// as the reference's balance (txOutputUTxO tx) does.
func TestByronBalancesUseWord16OutputView(t *testing.T) {
	t.Parallel()
	addr := newByronAddressForNetwork(t, nil, 0x02)
	for _, count := range []int{
		byronProducedLimit - 1,
		byronProducedLimit,
		byronProducedLimit + 1,
	} {
		// The tail output alone exceeds the input; it must not count.
		tx, ls := byronWord16Tx(
			t,
			count,
			1_000_000_000_000,
			addr,
			byronProducedLimit+2_000,
		)
		_, out, _, err := byronBalances(tx, ls)
		require.NoError(t, err, "outputs=%d", count)
		require.Equal(
			t,
			big.NewInt(int64(min(count, byronProducedLimit))),
			out,
			"outputs=%d",
			count,
		)
		require.NoError(
			t,
			byronValidateValueConserved(tx, 0, ls, nil),
			"outputs=%d",
			count,
		)
		require.NoError(
			t,
			byronValidateMinFee(tx, 0, ls, nil),
			"outputs=%d",
			count,
		)
		require.Equal(t, count, len(tx.Outputs()))
		require.Equal(t, min(count, byronProducedLimit), len(tx.Produced()))

		// With only 500 lovelace left over for the fee, the rule binds.
		short, shortLs := byronWord16Tx(
			t,
			count,
			1_000_000_000_000,
			addr,
			byronProducedLimit+500,
		)
		var feeErr FeeTooLowByronError
		require.True(
			t,
			errors.As(byronValidateMinFee(short, 0, shortLs, nil), &feeErr),
			"outputs=%d",
			count,
		)
	}
}

// TestByronOutputNetworkStillChecksOutputsPastWord16 pins that an output past
// index 65535, though it never enters the UTxO set, still has its address
// validated, as the reference's validateTxOutNM runs over every txOutput.
func TestByronOutputNetworkStillChecksOutputsPastWord16(t *testing.T) {
	t.Parallel()
	wrongMagic := uint32(42)
	tx, ls := byronWord16Tx(
		t,
		byronProducedLimit+1,
		1,
		newByronAddressForNetwork(t, &wrongMagic, 0x03),
		byronProducedLimit+100,
	)
	err := byronValidateOutputNetwork(tx, 0, ls, nil)
	var mismatch NetworkMagicMismatchByronError
	require.True(t, errors.As(err, &mismatch), "got %v", err)
	require.Equal(t, byronProducedLimit, mismatch.OutputIndex)
}
