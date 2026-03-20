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

func TestIntRange_SingleValue(t *testing.T) {
	// When min == max the only legal result is that value.
	for i := 0; i < 20; i++ {
		got := IntRange(5, 5)
		assert.Equal(t, 5, got, "IntRange(5,5) should always return 5")
	}
}

func TestIntRange_InBounds(t *testing.T) {
	const iterations = 1000
	min, max := 3, 11
	for i := 0; i < iterations; i++ {
		got := IntRange(min, max)
		require.GreaterOrEqual(
			t, got, min,
			"IntRange result must be >= min",
		)
		require.LessOrEqual(
			t, got, max,
			"IntRange result must be <= max",
		)
	}
}

func TestIntRange_Coverage(t *testing.T) {
	// With enough draws we should hit every value in the range.
	const iterations = 10000
	min, max := 0, 4
	seen := make(map[int]bool)
	for i := 0; i < iterations; i++ {
		seen[IntRange(min, max)] = true
	}
	for v := min; v <= max; v++ {
		assert.True(t, seen[v], "value %d was never produced by IntRange", v)
	}
}

func TestIntRange_PanicsOnInvalidRange(t *testing.T) {
	require.Panics(t, func() {
		IntRange(10, 5)
	}, "IntRange with min > max should panic")
}

func TestIntRange_NegativeValues(t *testing.T) {
	const iterations = 500
	min, max := -10, -1
	for i := 0; i < iterations; i++ {
		got := IntRange(min, max)
		require.GreaterOrEqual(t, got, min, "negative range: result >= min")
		require.LessOrEqual(t, got, max, "negative range: result <= max")
	}
}
