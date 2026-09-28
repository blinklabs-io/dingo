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

package committimestamp

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDecodeLegacyRoundTrip proves an in-range value round-trips.
func TestDecodeLegacyRoundTrip(t *testing.T) {
	want := int64(1234567890)
	got, err := DecodeLegacy(big.NewInt(want).Bytes())
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestDecodeLegacyRejectsOversizedValue proves a value that does not fit
// in an int64 is rejected rather than silently truncated: big.Int.Int64()
// is undefined for such a value, so returning it as though it were a
// valid timestamp would hand callers a garbage value with no indication
// anything was wrong.
func TestDecodeLegacyRejectsOversizedValue(t *testing.T) {
	oversized := new(big.Int).Lsh(big.NewInt(1), 100).Bytes() // 2^100
	_, err := DecodeLegacy(oversized)
	require.ErrorIs(t, err, ErrOutOfRange)
}

// TestDecodeLegacyErrorMessageIsBounded proves a corrupted, very large
// stored value does not get its full decimal expansion formatted into the
// error message: that expansion is itself an expensive, unbounded
// allocation proportional to the corrupted object's size, which backends
// cap at hundreds of MiB, not to the (tiny) size an error message should
// be.
func TestDecodeLegacyErrorMessageIsBounded(t *testing.T) {
	huge := make([]byte, 1<<20) // 1 MiB of non-zero bytes
	for i := range huge {
		huge[i] = 0xff
	}
	_, err := DecodeLegacy(huge)
	require.Error(t, err)
	require.Less(t, len(err.Error()), 256)
}

// TestFromFixedWidthRoundTrip proves an in-range value round-trips.
func TestFromFixedWidthRoundTrip(t *testing.T) {
	got, err := FromFixedWidth(1234567890)
	require.NoError(t, err)
	require.Equal(t, int64(1234567890), got)
}

// TestFromFixedWidthRejectsValueAboveMaxInt64 proves an 8-byte fixed-width
// value whose high bit is set is rejected rather than silently wrapped
// into a negative int64 by a raw int64(v) cast.
func TestFromFixedWidthRejectsValueAboveMaxInt64(t *testing.T) {
	_, err := FromFixedWidth(math.MaxInt64 + 1)
	require.ErrorIs(t, err, ErrOutOfRange)
}
