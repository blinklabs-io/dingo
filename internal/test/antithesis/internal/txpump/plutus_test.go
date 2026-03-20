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

var sampleScriptHash = make([]byte, 28)

func init() {
	for i := range sampleScriptHash {
		sampleScriptHash[i] = byte(i + 0x40)
	}
}

func samplePlutusInputs() []UTxO {
	return []UTxO{
		{TxHash: sampleHash, Index: 0, Amount: 10_000_000},
	}
}

// ---- PlutusLock tests ----

func TestBuildPlutusLockTx_Success(t *testing.T) {
	txBytes, err := BuildPlutusLockTx(
		samplePlutusInputs(),
		sampleScriptHash,
		5_000_000,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, txBytes)
}

func TestBuildPlutusLockTx_NoInputs(t *testing.T) {
	_, err := BuildPlutusLockTx(
		nil,
		sampleScriptHash,
		5_000_000,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "input")
}

func TestBuildPlutusLockTx_EmptyScriptHash(t *testing.T) {
	_, err := BuildPlutusLockTx(
		samplePlutusInputs(),
		nil,
		5_000_000,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "script hash")
}

func TestBuildPlutusLockTx_AmountBelowMinimum(t *testing.T) {
	_, err := BuildPlutusLockTx(
		samplePlutusInputs(),
		sampleScriptHash,
		minSendAmount-1,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "minimum")
}

func TestBuildPlutusLockTx_InvalidTxHash(t *testing.T) {
	inputs := []UTxO{{TxHash: "not-hex!", Index: 0, Amount: 10_000_000}}
	_, err := BuildPlutusLockTx(
		inputs,
		sampleScriptHash,
		5_000_000,
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tx hash")
}

func TestBuildPlutusLockTx_IsDeterministic(t *testing.T) {
	a, err := BuildPlutusLockTx(
		samplePlutusInputs(),
		sampleScriptHash,
		5_000_000,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	b, err := BuildPlutusLockTx(
		samplePlutusInputs(),
		sampleScriptHash,
		5_000_000,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildPlutusLockTx must be deterministic")
}

// ---- PlutusUnlock tests ----

func TestBuildPlutusUnlockTx_Success(t *testing.T) {
	txBytes, err := BuildPlutusUnlockTx(
		samplePlutusInputs(),
		alwaysSucceedsScript(),
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, txBytes)
}

func TestBuildPlutusUnlockTx_NilScriptUsesDefault(t *testing.T) {
	// Passing nil scriptBytes should fall back to the embedded always-succeeds
	// script without returning an error.
	txBytes, err := BuildPlutusUnlockTx(
		samplePlutusInputs(),
		nil,
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, txBytes)
}

func TestBuildPlutusUnlockTx_NoInputs(t *testing.T) {
	_, err := BuildPlutusUnlockTx(
		nil,
		alwaysSucceedsScript(),
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "input")
}

func TestBuildPlutusUnlockTx_InvalidTxHash(t *testing.T) {
	inputs := []UTxO{{TxHash: "not-hex!", Index: 0, Amount: 10_000_000}}
	_, err := BuildPlutusUnlockTx(
		inputs,
		alwaysSucceedsScript(),
		MinFee,
		sampleAddr,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tx hash")
}

func TestBuildPlutusUnlockTx_IsDeterministic(t *testing.T) {
	a, err := BuildPlutusUnlockTx(
		samplePlutusInputs(),
		alwaysSucceedsScript(),
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	b, err := BuildPlutusUnlockTx(
		samplePlutusInputs(),
		alwaysSucceedsScript(),
		MinFee,
		sampleAddr,
	)
	require.NoError(t, err)
	assert.Equal(t, a, b, "BuildPlutusUnlockTx must be deterministic")
}
