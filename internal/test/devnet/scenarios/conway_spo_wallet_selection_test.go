//go:build linux && devnet && !devnet_conformance

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

package scenarios

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	olsq "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func governanceWalletFixture(
	t *testing.T,
	hashes ...byte,
) (
	*governanceSigningKey,
	map[string]*governanceSigningKey,
	map[olsq.UtxoId]babbage.BabbageTransactionOutput,
) {
	t.Helper()
	address, err := common.NewAddressFromBytes(
		append([]byte{0x60}, bytes.Repeat([]byte{0xc1}, 28)...),
	)
	require.NoError(t, err)
	addressBytes, err := address.Bytes()
	require.NoError(t, err)
	key := &governanceSigningKey{address: address}
	results := make(map[olsq.UtxoId]babbage.BabbageTransactionOutput)
	for _, h := range hashes {
		var id olsq.UtxoId
		id.Hash = common.Blake2b256(bytes.Repeat([]byte{h}, 32))
		results[id] = babbage.BabbageTransactionOutput{
			OutputAddress: address,
			OutputAmount: mary.MaryTransactionOutputValue{
				Amount: 700_000_000_000,
			},
		}
	}
	keys := map[string]*governanceSigningKey{string(addressBytes): key}
	return key, keys, results
}

// The ledger does not show a UTxO that a mempool transaction has claimed as
// spent, so the wallet must skip it or the node rejects the transaction with
// "UTxO already consumed".
func TestSelectGovernanceUTxOSkipsInputsPendingInMempool(t *testing.T) {
	t.Parallel()
	_, keys, results := governanceWalletFixture(t, 0x15, 0x40, 0x90)
	lowest := governanceInput{hash: [32]byte(bytes.Repeat([]byte{0x15}, 32))}

	_, utxo, ok := selectGovernanceUTxO(results, keys, 0, nil)
	require.True(t, ok)
	require.Equal(t, lowest.hash[:], utxo.ID.Hash[:])

	_, utxo, ok = selectGovernanceUTxO(
		results, keys, 0, map[governanceInput]struct{}{lowest: {}},
	)
	require.True(t, ok)
	require.Equal(t, bytes.Repeat([]byte{0x40}, 32), utxo.ID.Hash[:])

	pending := make(map[governanceInput]struct{})
	for id := range results {
		pending[governanceInput{hash: id.Hash, idx: id.Idx}] = struct{}{}
	}
	_, _, ok = selectGovernanceUTxO(results, keys, 0, pending)
	require.False(t, ok, "every candidate is pending")
}

func TestGovernanceTransactionInputsReadsMempoolTransactions(t *testing.T) {
	t.Parallel()
	key, _, results := governanceWalletFixture(t, 0x15)
	var spent governanceUTxO
	for id, output := range results {
		spent = governanceUTxO{ID: id, Output: output}
	}
	body, err := governanceTransactionBody(spent, key.address)
	require.NoError(t, err)
	txBytes, err := cbor.Encode(&conway.ConwayTransaction{
		Body:      *body,
		TxIsValid: true,
	})
	require.NoError(t, err)

	inputs, err := governanceTransactionInputs([][]byte{txBytes})
	require.NoError(t, err)
	require.Equal(t, map[governanceInput]struct{}{
		{hash: [32]byte(spent.ID.Hash), idx: spent.ID.Idx}: {},
	}, inputs)
}
