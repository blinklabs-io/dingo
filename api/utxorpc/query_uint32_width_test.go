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

package utxorpc

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUint32FromIntWidth drives uint32FromInt either side of the bound it
// enforces before narrowing a protocol parameter to uint32. The inputs are
// int64 and converted at run time: a constant conversion of a value above
// math.MaxInt32 does not compile on a 32-bit target.
func TestUint32FromIntWidth(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		value   int64
		want    uint32
		wantErr bool
	}{
		{name: "zero", value: 0, want: 0},
		{
			name:  "max uint32",
			value: int64(math.MaxUint32),
			want:  math.MaxUint32,
		},
		{
			name:    "one past max uint32",
			value:   int64(math.MaxUint32) + 1,
			wantErr: true,
		},
		{name: "negative", value: -1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			value := int(tc.value)
			if int64(value) != tc.value {
				t.Skipf(
					"int is too narrow to represent %d on this target",
					tc.value,
				)
			}
			got, err := uint32FromInt(value, "epochLength")
			if tc.wantErr {
				require.ErrorContains(t, err, "epochLength out of range")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
