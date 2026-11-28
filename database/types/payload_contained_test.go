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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// A datum identical to an earlier one in the block (a spent output's, or a
// witness datum) is located inside the output that carries it, so the offset
// names that output's own bytes.
func TestLocatePayloadInPrefersTheContainersOccurrence(t *testing.T) {
	t.Parallel()
	datum := []byte("duplicated-datum-bytes")
	decoy := append([]byte("spent-output:"), datum...)
	own := append([]byte("live-output:"), datum...)
	block := bytes.Join([][]byte{[]byte("hdr|"), decoy, []byte("|"), own, []byte("|end")}, nil)
	loc, err := NewBlockPayloadLocator(7, bytes.Repeat([]byte{1}, 32), block)
	require.NoError(t, err)
	contained, ok := loc.(ContainedPayloadLocator)
	require.True(t, ok)

	first, err := loc.LocatePayload(datum)
	require.NoError(t, err)
	firstOffset, err := DecodeCborOffset(first)
	require.NoError(t, err)
	require.Equal(t, uint32(bytes.Index(block, datum)), firstOffset.ByteOffset)

	encoded, err := contained.LocatePayloadIn(datum, own)
	require.NoError(t, err)
	offset, err := DecodeCborOffset(encoded)
	require.NoError(t, err)
	ownStart := bytes.Index(block, own)
	require.Equal(
		t, uint32(ownStart+len("live-output:")), offset.ByteOffset,
		"the offset names the datum inside its own output",
	)
	require.Equal(t, datum, block[offset.ByteOffset:offset.ByteOffset+offset.ByteLength])
	require.Greater(t, offset.ByteOffset, firstOffset.ByteOffset)

	// A container the block does not hold falls back to the first occurrence.
	fallback, err := contained.LocatePayloadIn(datum, []byte("absent"))
	require.NoError(t, err)
	require.Equal(t, first, fallback)
}
