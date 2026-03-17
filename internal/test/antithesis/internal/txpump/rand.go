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

// Package txpump implements a randomized Cardano transaction generator
// for Antithesis property-based testing. It uses crypto/rand (backed by
// /dev/urandom which Antithesis replaces with a deterministic source) so
// that test runs are reproducible inside the Antithesis platform.
package txpump

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
)

// IntRange returns a uniformly distributed random integer in [min, max].
// It panics if min > max.
func IntRange(min, max int) int {
	if min > max {
		panic("txpump: IntRange called with min > max")
	}
	if min == max {
		return min
	}
	// Compute span as big.Int to avoid int overflow when max - min + 1
	// exceeds the int range.
	span := new(big.Int).SetInt64(int64(max))
	span.Sub(span, new(big.Int).SetInt64(int64(min)))
	span.Add(span, big.NewInt(1))
	n, err := rand.Int(rand.Reader, span)
	if err != nil {
		panic(fmt.Errorf("crypto/rand.Int failed: %w", err))
	}
	minBig := big.NewInt(int64(min))
	resultBig := new(big.Int).Add(minBig, n)

	limit := new(big.Int).Lsh(big.NewInt(1), uint(strconv.IntSize-1))
	minAllowed := new(big.Int).Neg(new(big.Int).Set(limit))
	maxAllowed := new(big.Int).Sub(new(big.Int).Set(limit), big.NewInt(1))
	if resultBig.Cmp(minAllowed) < 0 || resultBig.Cmp(maxAllowed) > 0 {
		panic("txpump: IntRange result exceeded platform int range")
	}
	return int(resultBig.Int64())
}
