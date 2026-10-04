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
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sampleStakeKeyHash = make([]byte, 28)
var samplePoolKeyHash = make([]byte, 28)

func init() {
	for i := range sampleStakeKeyHash {
		sampleStakeKeyHash[i] = byte(i + 1)
	}
	for i := range samplePoolKeyHash {
		samplePoolKeyHash[i] = byte(i + 0x80)
	}
}

func sampleDelegInputs() []UTxO {
	return []UTxO{
		{TxHash: sampleHash, Index: 0, Amount: 5_000_000},
	}
}

func TestBuildDelegationTx_Success(t *testing.T) {
	txBytes, err := BuildDelegationTx(
		sampleDelegInputs(),
		sampleStakeKeyHash,
		samplePoolKeyHash,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, txBytes)
	requireConwayDecode(t, txBytes)
}

func TestBuildDelegationTx_NoInputs(t *testing.T) {
	_, err := BuildDelegationTx(
		nil,
		sampleStakeKeyHash,
		samplePoolKeyHash,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "input")
}

func TestBuildDelegationTx_EmptyStakeKeyHash(t *testing.T) {
	_, err := BuildDelegationTx(
		sampleDelegInputs(),
		nil,
		samplePoolKeyHash,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stake key hash")
}

func TestBuildDelegationTx_EmptyPoolKeyHash(t *testing.T) {
	_, err := BuildDelegationTx(
		sampleDelegInputs(),
		sampleStakeKeyHash,
		nil,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool key hash")
}

func TestBuildDelegationTx_InvalidTxHash(t *testing.T) {
	inputs := []UTxO{{TxHash: "not-hex!", Index: 0, Amount: 5_000_000}}
	_, err := BuildDelegationTx(
		inputs,
		sampleStakeKeyHash,
		samplePoolKeyHash,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tx hash")
}

func TestBuildDelegationTx_FeeExceedsInputs(t *testing.T) {
	inputs := []UTxO{{TxHash: sampleHash, Index: 0, Amount: MinFee - 1}}
	_, err := BuildDelegationTx(
		inputs,
		sampleStakeKeyHash,
		samplePoolKeyHash,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot cover fee")
}

func TestBuildDelegationTx_MissingChangeAddr(t *testing.T) {
	_, err := BuildDelegationTx(
		sampleDelegInputs(),
		sampleStakeKeyHash,
		samplePoolKeyHash,
		MinFee,
		nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "change address")
}

func TestBuildDelegationTx_IsDeterministic(t *testing.T) {
	a, err := BuildDelegationTx(
		sampleDelegInputs(),
		sampleStakeKeyHash,
		samplePoolKeyHash,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	b, err := BuildDelegationTx(
		sampleDelegInputs(),
		sampleStakeKeyHash,
		samplePoolKeyHash,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildDelegationTx must be deterministic")
}

func testUTxOKey(seedByte byte) *UTxOKey {
	seed := bytes.Repeat([]byte{seedByte}, ed25519.SeedSize)
	return &UTxOKey{
		VKey: ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey),
		SKey: seed,
	}
}

// A delegation certificate is authorised by the stake credential's signature,
// and the inputs by their payment key's, so the node accepts the transaction
// only if the witness set carries both, each signing the body hash.
func TestBuildDelegationTxWitnessesInputAndStakeKeys(t *testing.T) {
	t.Parallel()
	inputKey := testUTxOKey(1)
	stakeKey := testUTxOKey(2)
	stakeHash := common.Blake2b224Hash(stakeKey.VKey)

	txBytes, err := BuildDelegationTx(
		sampleDelegInputs(),
		stakeHash.Bytes(),
		samplePoolKeyHash,
		MinFee,
		sampleAddr,
		inputKey,
		stakeKey,
	)
	require.NoError(t, err)

	var tx conway.ConwayTransaction
	_, err = cbor.Decode(txBytes, &tx)
	require.NoError(t, err)
	txHash := tx.Hash()
	witnesses := tx.Witnesses().Vkey()
	require.Len(t, witnesses, 2, "input and stake keys must each witness")
	signers := map[string]bool{}
	for _, w := range witnesses {
		require.True(
			t,
			ed25519.Verify(w.Vkey, txHash[:], w.Signature),
			"witness signature must cover the transaction body hash",
		)
		signers[hex.EncodeToString(w.Vkey)] = true
	}
	require.True(t, signers[hex.EncodeToString(inputKey.VKey)])
	require.True(t, signers[hex.EncodeToString(stakeKey.VKey)])
}
