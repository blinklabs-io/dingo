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

package dingo

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPluginInt64UintClamp drives pluginInt64's uint case either side of the
// int64 bound it clamps to. The inputs are uint64 and converted at run time:
// a constant conversion of a value above math.MaxUint32 does not compile on
// a 32-bit target, where uint is 32 bits wide and the clamp is unreachable.
func TestPluginInt64UintClamp(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value uint64
		want  int64
	}{
		{name: "zero", value: 0, want: 0},
		{
			name:  "max int64",
			value: uint64(math.MaxInt64),
			want:  math.MaxInt64,
		},
		{
			name:  "one past max int64 clamps",
			value: uint64(math.MaxInt64) + 1,
			want:  math.MaxInt64,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value := uint(tc.value)
			if uint64(value) != tc.value {
				t.Skipf(
					"uint is too narrow to represent %d on this target",
					tc.value,
				)
			}
			require.Equal(t, tc.want, pluginInt64(value))
		})
	}
}
