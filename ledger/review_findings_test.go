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

package ledger

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestBlocksAppliedBetweenFollowsTipAncestry(t *testing.T) {
	t.Parallel()

	target := ocommon.NewPoint(8, bytes.Repeat([]byte{0x08}, 32))
	tip := ocommon.NewPoint(10, bytes.Repeat([]byte{0x0A}, 32))
	below := models.Block{
		Slot: 9, Hash: bytes.Repeat([]byte{0x09}, 32), PrevHash: target.Hash,
	}
	applied := models.Block{Slot: 10, Hash: tip.Hash, PrevHash: below.Hash}
	competingBelow := models.Block{
		Slot: 9, Hash: bytes.Repeat([]byte{0x19}, 32), PrevHash: target.Hash,
	}
	competingTip := models.Block{
		Slot: 10, Hash: bytes.Repeat([]byte{0x1A}, 32), PrevHash: competingBelow.Hash,
	}
	above := models.Block{
		Slot: 11, Hash: bytes.Repeat([]byte{0x0B}, 32), PrevHash: tip.Hash,
	}

	got, err := blocksAppliedBetween(
		[]models.Block{above, competingTip, applied, competingBelow, below},
		tip,
		target,
	)
	require.NoError(t, err)
	require.Equal(t, []models.Block{applied, below}, got)

	_, err = blocksAppliedBetween(
		[]models.Block{applied, competingBelow},
		tip,
		target,
	)
	require.ErrorIs(t, err, errRollbackUndoAncestry)

	_, err = blocksAppliedBetween(nil, tip, target)
	require.ErrorIs(t, err, errRollbackUndoAncestry)
}

func TestUtxoWholeDetachedPointOffChainIsRejected(t *testing.T) {
	t.Parallel()

	ls, db := newDiskTestLedger(t)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(300, bytes.Repeat([]byte{0xAB}, 32)),
	}, nil))
	ctx := withUtxoWholeDetach(
		t.Context(),
		func() (QueryPoint, bool) {
			return QueryPoint{
				Slot: 200,
				Hash: bytes.Repeat([]byte{0xCD}, 32),
			}, true
		},
	)

	_, err := ls.queryShelleyUtxoWhole(ctx, QueryPoint{}, nil)
	require.ErrorIs(t, err, ErrPointNotOnChain)
}

// unencodableOutput has no original CBOR and cannot be encoded: the channel
// field is unsupported by the CBOR encoder.
type unencodableOutput struct {
	lcommon.TransactionOutput
	Unsupported chan int
}

func (unencodableOutput) Cbor() []byte { return nil }

func TestOutputCborReportsEncodeFailure(t *testing.T) {
	t.Parallel()

	c, err := outputCbor(unencodableOutput{})
	require.Error(t, err)
	require.Empty(t, c)
}
