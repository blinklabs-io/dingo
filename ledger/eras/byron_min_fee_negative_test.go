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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestByronMinFeeNegativeMultiplier pins calculateTxSizeLinear for a negative
// multiplier, which an update proposal can adopt: ceiling(multiplier * size)
// is converted to Word64 Lovelace, so a size term of zero charges only the
// summand while a negative one overflows the addition.
func TestByronMinFeeNegativeMultiplier(t *testing.T) {
	t.Parallel()

	got, err := byronMinFee(5_000_000_000, -1, 200)
	require.NoError(t, err)
	require.Zero(t, big.NewInt(5).Cmp(got), "got %s", got)

	got, err = byronMinFee(5_000_000_000, -4_999_999, 200)
	require.NoError(t, err)
	require.Zero(t, big.NewInt(5).Cmp(got), "got %s", got)

	var bound LovelaceBoundByronError
	_, err = byronMinFee(5_000_000_000, -5_000_000, 200)
	require.ErrorAs(t, err, &bound)
	_, err = byronMinFee(5_000_000_000, -1_000_000_000, 1)
	require.ErrorAs(t, err, &bound)
}
