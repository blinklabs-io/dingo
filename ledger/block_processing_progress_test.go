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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

func TestBlockProcessingProgressBeforeCommitAndAfterRetry(t *testing.T) {
	t.Parallel()
	ls, _, block := newByronShelleyBoundaryLedger(t)
	committed := ls.Tip()
	require.Zero(t, ls.BlockProcessingSlot())
	rejected := errors.New("reject batch before commit")
	calls := 0
	for attempt := range 3 {
		results := make(chan readChainResult, 1)
		results <- readChainResult{blocks: []gledger.Block{block}}
		close(results)
		err := ls.processBlocksFromSource(
			t.Context(),
			results,
			blockSourceOptions{
				beforeCommit: func(_ *database.Txn, applied []gledger.Block) error {
					if len(applied) == 0 {
						return nil
					}
					calls++
					require.Equal(
						t,
						block.SlotNumber(),
						ls.BlockProcessingSlot(),
						"completed block work must be visible before durable commit",
					)
					require.Equal(
						t,
						committed,
						ls.Tip(),
						"progress must not publish an uncommitted tip",
					)
					if attempt < 2 {
						return rejected
					}
					return nil
				},
			},
		)
		if attempt < 2 {
			require.ErrorIs(t, err, rejected)
			require.Equal(t, committed, ls.Tip())
		} else {
			require.NoError(t, err)
			require.Equal(t, block.SlotNumber(), ls.Tip().Point.Slot)
		}
		require.Equal(t, attempt+1, calls)
		require.Equal(t, block.SlotNumber(), ls.BlockProcessingSlot(),
			"retrying an already processed slot must not create new progress")
	}
}

func TestBlockProcessingSlotNeverRegresses(t *testing.T) {
	t.Parallel()
	var ls LedgerState
	for _, step := range []struct{ slot, want uint64 }{
		{100, 100}, {90, 100}, {100, 100}, {101, 101},
	} {
		ls.recordProcessedBlockSlot(step.slot)
		require.Equal(t, step.want, ls.BlockProcessingSlot(),
			"replaying earlier slots must not renew the progress signal")
	}
}
