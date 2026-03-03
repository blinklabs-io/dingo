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

// TestEpochNonceFormula verifies the epoch nonce combining formula:
//
//	epochNonce(N+1) = blake2b_256(candidateNonce(N) || labNonce(N))
//
// where labNonce is the prevHash of the last block of epoch N.
// This uses a known candidateNonce value from pre-fix diagnostic logging
// and the real prevHash from preview epoch 1221's last block.
//
// TODO: update candidateNonce once captured from dingo running against
// preview with the fixed 4k/f stability window and Praos VRF nonce.
// With the correct candidateNonce, the result should match the Koios
// epoch 1222 nonce: 2dc1b3e5f315d34d4c43e5b94324ea6f5d251d0ffd0d9ff275738a8785847921
func TestEpochNonceFormula(t *testing.T) {
	// candidateNonce from pre-fix diagnostic logging (3k/f cutoff, 1816 blocks)
	candidateNonce := mustDecodeHex(
		t,
		"068b7734e8c969f13a38bf7df6ddc7d3809f8b2994ebaa2b998814761b1463b3",
	)
	// prevHash of the last block of epoch 1221 (slot 105580722)
	labNonce := mustDecodeHex(
		t,
		"3c4eb00b5efe1317e7c370c3a33cc182a6d74c64b8de633c5024163516ec29e8",
	)

	result, err := lcommon.CalculateEpochNonce(
		candidateNonce, labNonce, nil,
	)
	require.NoError(t, err)

	// blake2b_256(candidateNonce || labNonce) with these inputs
	expected := "95810c0dadf6f0a23c7093cb8c986a7ebd2194c358bfeb873056ed2749e8e45e"
	assert.Equal(
		t, expected, hex.EncodeToString(result.Bytes()),
		"epoch nonce formula should produce blake2b_256(candidateNonce || labNonce)",
	)
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}
