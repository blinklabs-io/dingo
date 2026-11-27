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

package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiveOutputOffsetRoundTrip(t *testing.T) {
	t.Parallel()
	in := LiveOutputOffset{
		TxHash: [32]byte{1, 2, 3}, OutputIndex: 7, ByteOffset: 9, ByteLength: 11,
	}
	encoded := in.Encode()
	require.Len(t, encoded, CborOffsetSize)
	require.True(t, IsLiveOutputOffset(encoded))
	require.False(t, IsEndorserOffset(encoded))
	_, err := DecodeCborOffset(encoded)
	require.Error(t, err, "a block offset decoder refuses a live-output offset")
	out, err := DecodeLiveOutputOffset(encoded)
	require.NoError(t, err)
	require.Equal(t, in, *out)
}
