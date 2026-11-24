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
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// The result of indexing a block names payloads by their position in that
// block, so a row written from it reads back the bytes of the block.
func TestComputeOffsetsNamesPayloadsInTheBlock(t *testing.T) {
	t.Parallel()
	block := findGapRollbackCandidateWithoutCertificates(t).consumerBlock
	offsets := mustBlockOffsets(t, block)
	require.NotNil(t, offsets.Payloads)

	payload := block.Cbor[len(block.Cbor)/2 : len(block.Cbor)/2+24]
	encoded, err := offsets.Payloads.LocatePayload(payload)
	require.NoError(t, err)
	offset, err := types.DecodeCborOffset(encoded)
	require.NoError(t, err)
	require.Equal(t, block.Slot, offset.BlockSlot)
	require.Equal(t, block.Hash, offset.BlockHash[:])
	require.Equal(
		t, payload,
		block.Cbor[offset.ByteOffset:offset.ByteOffset+offset.ByteLength],
	)
}
