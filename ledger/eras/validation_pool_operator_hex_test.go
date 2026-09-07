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

package eras

import (
	"encoding/hex"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestCheckPoolMarginFloorErrorPoolIdIsSingleHexEncoded pins the pool id in
// the CIP-23 margin-floor rejection to its 56-character hex form.
//
// lcommon.PoolKeyHash is an alias for Blake2b224, which implements fmt.Stringer
// with a hex String method, and fmt routes the x verb through String for such
// operands. Formatting the value itself with %x therefore hex-encoded its hex
// string and named the pool by an id twice the correct length, matching no
// real pool key hash.
func TestCheckPoolMarginFloorErrorPoolIdIsSingleHexEncoded(t *testing.T) {
	operatorHex := "00112233445566778899aabbccddeeff00112233445566778899aabb"
	operatorBytes, err := hex.DecodeString(operatorHex)
	require.NoError(t, err)
	require.Len(t, operatorBytes, lcommon.Blake2b224Size)

	cert := cip23PoolCert(1, 1000) // 0.1%, below the floor below
	copy(cert.Operator[:], operatorBytes)

	err = checkPoolMarginFloor(
		[]lcommon.Certificate{cert},
		big.NewRat(150, 10_000), // 1.5%
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pool "+operatorHex+" margin")
	require.NotContains(
		t,
		err.Error(),
		hex.EncodeToString([]byte(operatorHex)),
		"pool id must not be hex-encoded twice",
	)
}
