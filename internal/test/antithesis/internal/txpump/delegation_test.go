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
