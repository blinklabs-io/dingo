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

//go:build dingo_extra_plugins

package gcs

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDecodeCommitTimestampRoundTrip proves an in-range value round-trips.
func TestDecodeCommitTimestampRoundTrip(t *testing.T) {
	want := int64(1234567890)
	got, err := decodeCommitTimestamp(big.NewInt(want).Bytes())
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestDecodeCommitTimestampRejectsOversizedValue proves a value that does
// not fit in an int64 is rejected rather than silently truncated:
// big.Int.Int64() is undefined for such a value, so returning it as though
// it were a valid timestamp would hand callers a garbage value with no
// indication anything was wrong.
func TestDecodeCommitTimestampRejectsOversizedValue(t *testing.T) {
	oversized := new(big.Int).Lsh(big.NewInt(1), 100).Bytes() // 2^100
	_, err := decodeCommitTimestamp(oversized)
	require.Error(t, err)
}
