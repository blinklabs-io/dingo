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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package eras

import (
	"math"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestCeilRatToUint64DistinguishesUnderflowFromOverflow(t *testing.T) {
	t.Parallel()
	huge := new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), 64))
	for _, tc := range []struct {
		name string
		in   *big.Rat
		want uint64
	}{
		{"nil", nil, 0},
		{"negative fraction", big.NewRat(-577, 10000), 0},
		{"large negative", new(big.Rat).Neg(huge), 0},
		{"zero", new(big.Rat), 0},
		{"fraction rounds up", big.NewRat(1, 3), 1},
		{"largest uint64", new(big.Rat).SetUint64(math.MaxUint64), math.MaxUint64},
		{"overflow saturates", huge, math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, ceilRatToUint64(tc.in))
		})
	}
}

// A negative execution price must not present as the largest possible fee.
func TestCalculateMinFeeNegativePricesDoNotSaturate(t *testing.T) {
	t.Parallel()
	got := CalculateMinFee(
		2000,
		lcommon.ExUnits{Memory: 2000000, Steps: 500000000},
		44,
		155381,
		big.NewRat(-577, 10000),
		big.NewRat(-721, 10000000),
	)
	require.Equal(t, uint64(44*2000+155381), got)
}
