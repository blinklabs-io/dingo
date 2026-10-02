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

// testSigningKey returns a deterministic Ed25519 key controlling sampleAddr.
func testSigningKey(seedByte byte) *UTxOKey {
	seed := bytes.Repeat([]byte{seedByte}, ed25519.SeedSize)
	return &UTxOKey{
		VKey:    ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey),
		SKey:    seed,
		Address: sampleAddr,
	}
}

// requireSignedBy decodes txBytes as a Conway transaction and checks that it
// carries a valid body signature from every key in keys.
func requireSignedBy(t *testing.T, txBytes []byte, keys ...*UTxOKey) *conway.ConwayTransaction {
	t.Helper()
	var tx conway.ConwayTransaction
	_, err := cbor.Decode(txBytes, &tx)
	require.NoError(t, err)
	bodyHash := tx.Id()
	witnesses := tx.WitnessSet.VkeyWitnesses.Items()
	for _, key := range keys {
		found := false
		for _, w := range witnesses {
			if bytes.Equal(w.Vkey, key.VKey) {
				require.True(t, ed25519.Verify(key.VKey, bodyHash[:], w.Signature))
				found = true
			}
		}
		require.True(t, found, "missing witness for key %x", key.VKey)
	}
	return &tx
}

func TestCredentialForUsesFundingPaymentKey(t *testing.T) {
	key := testSigningKey(0x11)
	hash, credKey := credentialFor([]UTxO{{TxHash: sampleHash, SigningKey: key}})
	require.Equal(t, common.Blake2b224Hash(key.VKey).Bytes(), hash)
	require.Same(t, key, credKey)
}

func TestBuildDelegationTx_RegistersAndSigns(t *testing.T) {
	inputKey := testSigningKey(0x11)
	stakeKey := testSigningKey(0x22)
	stakeHash := common.Blake2b224Hash(stakeKey.VKey).Bytes()
	inputs := []UTxO{{TxHash: sampleHash, Amount: 5_000_000, SigningKey: inputKey}}

	txBytes, err := BuildDelegationTx(
		inputs, stakeHash, samplePoolKeyHash, stakeKeyDeposit, MinFee, sampleAddr, stakeKey,
	)
	require.NoError(t, err)
	tx := requireSignedBy(t, txBytes, inputKey, stakeKey)

	certs := tx.Certificates()
	require.Len(t, certs, 1)
	cert, ok := certs[0].(*common.StakeRegistrationDelegationCertificate)
	require.True(t, ok, "registration delegation must use certificate type 11, got %T", certs[0])
	require.Equal(t, stakeHash, cert.StakeCredential.Credential.Bytes())
	require.Equal(t, samplePoolKeyHash, cert.PoolKeyHash.Bytes())
	require.Equal(t, int64(stakeKeyDeposit), cert.Amount)
	outputs := tx.Outputs()
	require.Len(t, outputs, 1)
	require.Equal(t, uint64(5_000_000)-MinFee-stakeKeyDeposit, outputs[0].Amount().Uint64())
}

func TestBuildDelegationTx_DelegatesRegisteredCredential(t *testing.T) {
	txBytes, err := BuildDelegationTx(
		sampleDelegInputs(), sampleStakeKeyHash, samplePoolKeyHash, 0, MinFee, sampleAddr, nil,
	)
	require.NoError(t, err)
	requireConwayDecode(t, txBytes)
	var tx conway.ConwayTransaction
	_, err = cbor.Decode(txBytes, &tx)
	require.NoError(t, err)
	certs := tx.Certificates()
	require.Len(t, certs, 1)
	_, ok := certs[0].(*common.StakeDelegationCertificate)
	require.True(t, ok, "plain delegation must use certificate type 2, got %T", certs[0])
}

func TestBuildDelegationTx_RejectsChangeBelowMinimumOutput(t *testing.T) {
	inputs := []UTxO{{TxHash: sampleHash, Amount: MinFee + stakeKeyDeposit + minSendAmount - 1}}
	_, err := BuildDelegationTx(
		inputs, sampleStakeKeyHash, samplePoolKeyHash, stakeKeyDeposit, MinFee, sampleAddr, nil,
	)
	require.ErrorContains(t, err, "below the minimum output")
}

func TestBuildDelegationTx_NoInputs(t *testing.T) {
	_, err := BuildDelegationTx(
		nil, sampleStakeKeyHash, samplePoolKeyHash, 0, MinFee, sampleAddr, nil,
	)
	require.Error(t, err)
}

func TestBuildDelegationTx_InvalidHashes(t *testing.T) {
	_, err := BuildDelegationTx(
		sampleDelegInputs(), []byte{}, samplePoolKeyHash, 0, MinFee, sampleAddr, nil,
	)
	require.Error(t, err)
	_, err = BuildDelegationTx(
		sampleDelegInputs(), sampleStakeKeyHash, []byte{}, 0, MinFee, sampleAddr, nil,
	)
	require.Error(t, err)
	_, err = BuildDelegationTx(
		[]UTxO{{TxHash: "not-hex", Amount: 5_000_000}},
		sampleStakeKeyHash, samplePoolKeyHash, 0, MinFee, sampleAddr, nil,
	)
	require.Error(t, err)
}

func TestBuildDelegationTx_FeeExceedsInputs(t *testing.T) {
	_, err := BuildDelegationTx(
		[]UTxO{{TxHash: sampleHash, Amount: MinFee - 1}},
		sampleStakeKeyHash, samplePoolKeyHash, 0, MinFee, sampleAddr, nil,
	)
	require.Error(t, err)
}

func TestBuildDelegationTx_MissingChangeAddr(t *testing.T) {
	_, err := BuildDelegationTx(
		sampleDelegInputs(), sampleStakeKeyHash, samplePoolKeyHash, 0, MinFee, nil, nil,
	)
	require.Error(t, err)
}

func TestBuildDelegationTx_IsDeterministic(t *testing.T) {
	key := testSigningKey(0x33)
	a, err := BuildDelegationTx(
		sampleDelegInputs(), sampleStakeKeyHash, samplePoolKeyHash, 0, MinFee, sampleAddr, key,
	)
	require.NoError(t, err)
	b, err := BuildDelegationTx(
		sampleDelegInputs(), sampleStakeKeyHash, samplePoolKeyHash, 0, MinFee, sampleAddr, key,
	)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildDelegationTx must be deterministic")
}
