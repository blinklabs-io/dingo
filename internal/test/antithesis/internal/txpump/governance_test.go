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
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sampleDRepKeyHash = make([]byte, 28)
var sampleGovActionTxHash []byte

func init() {
	for i := range sampleDRepKeyHash {
		sampleDRepKeyHash[i] = byte(i + 0x20)
	}
	h, err := hex.DecodeString(sampleHash)
	if err != nil {
		panic(fmt.Errorf("decode sampleHash: %w", err))
	}
	sampleGovActionTxHash = h
}

func sampleGovInputs() []UTxO {
	return []UTxO{
		{TxHash: sampleHash, Index: 0, Amount: 600_000_000},
	}
}

// ---- DRep registration tests ----

func TestBuildDRepRegistrationTx_Success(t *testing.T) {
	txBytes, err := BuildDRepRegistrationTx(
		sampleGovInputs(),
		sampleDRepKeyHash,
		500_000_000,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, txBytes)
}

func TestBuildDRepRegistrationTx_NoInputs(t *testing.T) {
	_, err := BuildDRepRegistrationTx(
		nil,
		sampleDRepKeyHash,
		500_000_000,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "input")
}

func TestBuildDRepRegistrationTx_EmptyKeyHash(t *testing.T) {
	_, err := BuildDRepRegistrationTx(
		sampleGovInputs(),
		nil,
		500_000_000,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DRep key hash")
}

func TestBuildDRepRegistrationTx_InvalidTxHash(t *testing.T) {
	inputs := []UTxO{{TxHash: "not-hex!", Index: 0, Amount: 10_000_000}}
	_, err := BuildDRepRegistrationTx(
		inputs,
		sampleDRepKeyHash,
		500_000_000,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tx hash")
}

func TestBuildDRepRegistrationTx_IsDeterministic(t *testing.T) {
	a, err := BuildDRepRegistrationTx(
		sampleGovInputs(),
		sampleDRepKeyHash,
		500_000_000,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	b, err := BuildDRepRegistrationTx(
		sampleGovInputs(),
		sampleDRepKeyHash,
		500_000_000,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildDRepRegistrationTx must be deterministic")
}

// ---- Vote transaction tests ----

func TestBuildVoteTx_Success(t *testing.T) {
	txBytes, err := BuildVoteTx(
		sampleGovInputs(),
		sampleDRepKeyHash,
		sampleGovActionTxHash,
		0,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, txBytes)
}

func TestBuildVoteTx_NoInputs(t *testing.T) {
	_, err := BuildVoteTx(
		nil,
		sampleDRepKeyHash,
		sampleGovActionTxHash,
		0,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "input")
}

func TestBuildVoteTx_EmptyVoterKeyHash(t *testing.T) {
	_, err := BuildVoteTx(
		sampleGovInputs(),
		nil,
		sampleGovActionTxHash,
		0,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "voter key hash")
}

func TestBuildVoteTx_EmptyGovActionTxHash(t *testing.T) {
	_, err := BuildVoteTx(
		sampleGovInputs(),
		sampleDRepKeyHash,
		nil,
		0,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "governance action tx hash")
}

func TestBuildVoteTx_InvalidInputTxHash(t *testing.T) {
	inputs := []UTxO{{TxHash: "not-hex!", Index: 0, Amount: 10_000_000}}
	_, err := BuildVoteTx(
		inputs,
		sampleDRepKeyHash,
		sampleGovActionTxHash,
		0,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tx hash")
}

func TestBuildVoteTx_IsDeterministic(t *testing.T) {
	a, err := BuildVoteTx(
		sampleGovInputs(),
		sampleDRepKeyHash,
		sampleGovActionTxHash,
		0,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	b, err := BuildVoteTx(
		sampleGovInputs(),
		sampleDRepKeyHash,
		sampleGovActionTxHash,
		0,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildVoteTx must be deterministic")
}
