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

package chain_test

import (
	"testing"

	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestAddBlockWithPointRejectsWrongLengthHash covers a caller-supplied point
// whose hash is not 32 bytes. The chain stores that hash as the block's key
// and later reads it back into fixed-width hash types, so it must refuse the
// block rather than persist the malformed hash.
func TestAddBlockWithPointRejectsWrongLengthHash(t *testing.T) {
	t.Parallel()
	c, _ := newHeaderStreamChain(t)
	blocks, err := testfixtures.GenerateConwayChain(1)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	block := blocks[0]

	err = c.AddBlockWithPoint(
		block,
		ocommon.Point{
			Slot: block.SlotNumber(),
			Hash: block.Hash().Bytes()[:31],
		},
		nil,
	)
	require.ErrorContains(t, err, "expected 32 bytes, got 31")
	require.Zero(t, c.Tip().Point.Slot)
	require.Empty(t, c.Tip().Point.Hash)

	require.NoError(t, c.AddBlockWithPoint(
		block,
		ocommon.Point{Slot: block.SlotNumber(), Hash: block.Hash().Bytes()},
		nil,
	))
	require.Equal(t, block.Hash().Bytes(), c.Tip().Point.Hash)
}
