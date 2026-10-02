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

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sampleDRepKeyHash = make([]byte, 28)

func init() {
	for i := range sampleDRepKeyHash {
		sampleDRepKeyHash[i] = byte(i + 0x20)
	}
}

func sampleGovInputs() []UTxO {
	return []UTxO{
		{TxHash: sampleHash, Index: 0, Amount: 600_000_000},
	}
}

func TestBuildDRepRegistrationTx_RegistersAndSigns(t *testing.T) {
	key := testSigningKey(0x44)
	drepHash := common.Blake2b224Hash(key.VKey).Bytes()
	inputs := []UTxO{{TxHash: sampleHash, Amount: 600_000_000, SigningKey: key}}

	txBytes, err := BuildDRepRegistrationTx(
		inputs, drepHash, drepDeposit, MinFee, sampleAddr, key,
	)
	require.NoError(t, err)
	tx := requireSignedBy(t, txBytes, key)

	certs := tx.Certificates()
	require.Len(t, certs, 1)
	cert, ok := certs[0].(*common.RegistrationDrepCertificate)
	require.True(t, ok, "DRep registration must use certificate type 16, got %T", certs[0])
	require.Equal(t, drepHash, cert.DrepCredential.Credential.Bytes())
	require.Equal(t, int64(drepDeposit), cert.Amount)
	outputs := tx.Outputs()
	require.Len(t, outputs, 1)
	require.Equal(t, uint64(600_000_000)-MinFee-drepDeposit, outputs[0].Amount().Uint64())
}

func TestBuildDRepUpdateTx_UpdatesAndSigns(t *testing.T) {
	key := testSigningKey(0x55)
	drepHash := common.Blake2b224Hash(key.VKey).Bytes()
	inputs := []UTxO{{TxHash: sampleHash, Amount: 5_000_000, SigningKey: key}}

	txBytes, err := BuildDRepUpdateTx(inputs, drepHash, MinFee, sampleAddr, key)
	require.NoError(t, err)
	tx := requireSignedBy(t, txBytes, key)

	certs := tx.Certificates()
	require.Len(t, certs, 1)
	cert, ok := certs[0].(*common.UpdateDrepCertificate)
	require.True(t, ok, "DRep update must use certificate type 18, got %T", certs[0])
	require.Equal(t, drepHash, cert.DrepCredential.Credential.Bytes())
}

func TestBuildDRepRegistrationTx_NoInputs(t *testing.T) {
	_, err := BuildDRepRegistrationTx(
		nil, sampleDRepKeyHash, drepDeposit, MinFee, sampleAddr, nil,
	)
	require.Error(t, err)
}

func TestBuildDRepRegistrationTx_EmptyKeyHash(t *testing.T) {
	_, err := BuildDRepRegistrationTx(
		sampleGovInputs(), nil, drepDeposit, MinFee, sampleAddr, nil,
	)
	require.Error(t, err)
	_, err = BuildDRepUpdateTx(sampleGovInputs(), nil, MinFee, sampleAddr, nil)
	require.Error(t, err)
}

func TestBuildDRepRegistrationTx_InsufficientForDeposit(t *testing.T) {
	_, err := BuildDRepRegistrationTx(
		[]UTxO{{TxHash: sampleHash, Amount: drepDeposit}},
		sampleDRepKeyHash, drepDeposit, MinFee, sampleAddr, nil,
	)
	require.Error(t, err)
}

func TestBuildDRepRegistrationTx_IsDeterministic(t *testing.T) {
	a, err := BuildDRepRegistrationTx(
		sampleGovInputs(), sampleDRepKeyHash, drepDeposit, MinFee, sampleAddr, nil,
	)
	require.NoError(t, err)
	b, err := BuildDRepRegistrationTx(
		sampleGovInputs(), sampleDRepKeyHash, drepDeposit, MinFee, sampleAddr, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildDRepRegistrationTx must be deterministic")
}
