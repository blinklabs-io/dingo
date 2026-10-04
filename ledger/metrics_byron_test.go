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

package ledger

import (
	"math"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestCreatedUtxoCountStopsByronAtWord16 pins the metric to the UTxO set a
// Byron transaction actually creates: only outputs 0 through 65535.
func TestCreatedUtxoCountStopsByronAtWord16(t *testing.T) {
	t.Parallel()
	input, err := byron.NewByronTransactionInput(
		strings.Repeat("cd", lcommon.Blake2b256Size),
		0,
	)
	require.NoError(t, err)
	addr, err := lcommon.NewByronAddressFromParts(
		lcommon.ByronAddressTypePubkey,
		make([]byte, lcommon.AddressHashSize),
		lcommon.ByronAddressAttributes{},
	)
	require.NoError(t, err)
	const limit = math.MaxUint16 + 1
	for _, count := range []int{1, limit, limit + 1} {
		outputs := make([]byron.ByronTransactionOutput, count)
		for i := range outputs {
			outputs[i] = byron.ByronTransactionOutput{
				OutputAddress: addr,
				OutputAmount:  1,
			}
		}
		tx := &byron.ByronTransaction{
			Body: byron.ByronTransactionBody{
				TxInputs:  []byron.ByronTransactionInput{input},
				TxOutputs: outputs,
			},
		}
		require.Equal(t, min(count, limit), createdUtxoCount(tx),
			"outputs=%d", count)
	}
}
