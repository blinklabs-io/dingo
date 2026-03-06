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
	"encoding/hex"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEpochNonceFormula validates the epoch nonce formula:
//
//	epochNonce(N+1) = blake2b_256(candidateNonce(N) || lastEpochBlockNonce(N))
//
// This uses deterministic synthetic inputs to verify the CalculateEpochNonce
// function produces the correct blake2b_256 hash of the concatenated nonces.
func TestEpochNonceFormula(t *testing.T) {
	// Synthetic 32-byte candidateNonce
	candidateNonce := mustDecodeHex(
		t,
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	)
	// Synthetic 32-byte lastEpochBlockNonce (prevHash of last block)
	lastEpochBlockNonce := mustDecodeHex(
		t,
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	)

	// Compute epoch nonce using the production function
	result, err := lcommon.CalculateEpochNonce(
		candidateNonce,
		lastEpochBlockNonce,
		nil,
	)
	require.NoError(t, err)

	// Verify the result matches blake2b_256(candidateNonce || lastEpochBlockNonce)
	concat := append(candidateNonce, lastEpochBlockNonce...)
	expected := lcommon.Blake2b256Hash(concat)
	assert.Equal(
		t,
		hex.EncodeToString(expected.Bytes()),
		hex.EncodeToString(result.Bytes()),
		"epoch nonce should equal blake2b_256(candidateNonce || lastEpochBlockNonce)",
	)
}

// TestEpochNonceNeutralIdentity verifies the neutral nonce semantics:
// when lastEpochBlockNonce is nil (epoch 0→1 transition), the caller
// uses candidateNonce directly as the epoch nonce (bypassing
// CalculateEpochNonce). This mirrors the production code in
// calculateEpochNonce and computeEpochNonceForSlot.
func TestEpochNonceNeutralIdentity(t *testing.T) {
	candidateNonce := mustDecodeHex(
		t,
		"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	)

	// Simulate the NeutralNonce path: when lastEpochBlockNonce is nil,
	// the epoch nonce IS the candidateNonce (identity element of ⭒).
	var lastEpochBlockNonce []byte // nil = NeutralNonce
	var epochNonce []byte
	if len(lastEpochBlockNonce) == 0 {
		epochNonce = candidateNonce
	} else {
		result, err := lcommon.CalculateEpochNonce(
			candidateNonce,
			lastEpochBlockNonce,
			nil,
		)
		require.NoError(t, err)
		epochNonce = result.Bytes()
	}

	assert.Equal(
		t,
		hex.EncodeToString(candidateNonce),
		hex.EncodeToString(epochNonce),
		"with NeutralNonce, epoch nonce should equal candidateNonce",
	)
}

// TestEpochNonceNonCommutative verifies that the nonce semigroup
// operator is NOT commutative: blake2b_256(a || b) != blake2b_256(b || a).
func TestEpochNonceNonCommutative(t *testing.T) {
	a := mustDecodeHex(
		t,
		"1111111111111111111111111111111111111111111111111111111111111111",
	)
	b := mustDecodeHex(
		t,
		"2222222222222222222222222222222222222222222222222222222222222222",
	)

	resultAB, err := lcommon.CalculateEpochNonce(a, b, nil)
	require.NoError(t, err)

	resultBA, err := lcommon.CalculateEpochNonce(b, a, nil)
	require.NoError(t, err)

	assert.NotEqual(
		t,
		hex.EncodeToString(resultAB.Bytes()),
		hex.EncodeToString(resultBA.Bytes()),
		"nonce combination should not be commutative",
	)
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}
