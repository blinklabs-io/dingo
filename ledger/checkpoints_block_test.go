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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"errors"
	"io"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/immutable"
)

// loadFirstImmutableBlock returns the first non-EBB block from the
// immutable testdata. It lets the checkpoint tests assert against a real
// block hash in the exact format produced by block.Hash().String(),
// which is what the checkpoint comparison relies on.
func loadFirstImmutableBlock(t *testing.T) ledger.Block {
	t.Helper()
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0})
	require.NoError(t, err)
	defer iter.Close()
	for {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if immBlock == nil {
			break
		}
		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		if _, isEbb := block.(*byron.ByronEpochBoundaryBlock); !isEbb {
			return block
		}
	}
	t.Fatal("no non-EBB block found in immutable testdata")
	return nil
}

func TestValidateBlockCheckpointRealBlockMatch(t *testing.T) {
	block := loadFirstImmutableBlock(t)
	ls := &LedgerState{
		checkpoints: map[uint64]string{
			block.BlockNumber(): block.Hash().String(),
		},
	}
	require.NoError(t, ls.validateBlockCheckpoint(block))
}

func TestValidateBlockCheckpointRealBlockMismatch(t *testing.T) {
	block := loadFirstImmutableBlock(t)
	ls := &LedgerState{
		checkpoints: map[uint64]string{
			block.BlockNumber(): "0000000000000000000000000000000000000000000000000000000000000000",
		},
	}
	err := ls.validateBlockCheckpoint(block)
	require.Error(t, err)
	var cpErr *CheckpointMismatchError
	require.True(t, errors.As(err, &cpErr))
	require.Equal(t, block.BlockNumber(), cpErr.BlockNo)
}

func TestValidateBlockCheckpointNoCheckpointsConfiguredForBlock(t *testing.T) {
	block := loadFirstImmutableBlock(t)
	ls := &LedgerState{} // nil checkpoints map
	require.NoError(t, ls.validateBlockCheckpoint(block))
}

func TestValidateBlockCheckpointSkipsByronEbb(t *testing.T) {
	// A Byron EBB shares the preceding block's number. Even if that number
	// is checkpointed with a different hash, the EBB itself must be skipped
	// so it cannot be falsely rejected. The skip happens before any block
	// method is called, so a zero-value EBB is sufficient here.
	ls := &LedgerState{
		checkpoints: map[uint64]string{0: "deadbeef"},
	}
	require.NoError(
		t,
		ls.validateBlockCheckpoint(&byron.ByronEpochBoundaryBlock{}),
	)
}
