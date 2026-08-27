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

package database

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSafeIntToUint32 guards both the clamping behavior and the regression
// this function was fixed for: comparing n directly against the untyped
// constant math.MaxUint32 fails to compile on a 32-bit platform, where int
// cannot represent that value. Comparing via int64(n) instead keeps the
// same clamping behavior on every platform width.
func TestSafeIntToUint32(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want uint32
	}{
		{name: "negative clamps to zero", n: -1, want: 0},
		{name: "zero", n: 0, want: 0},
		{name: "typical block offset", n: 90_000, want: 90_000},
		{name: "exactly MaxUint32", n: math.MaxUint32, want: math.MaxUint32},
		{
			name: "beyond MaxUint32 clamps",
			n:    math.MaxUint32 + 1,
			want: math.MaxUint32,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, safeIntToUint32(tc.n))
		})
	}
}
