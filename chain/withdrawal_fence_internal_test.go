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

package chain

import (
	"errors"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestWithdrawalInvalidatesEvidenceBeforeMutation(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"clear headers", "block rollback", "queued rollback", "batch restore", "caller rollback"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newBarrierTestDB(t)
			manager, err := NewManager(t.Context(), db, nil)
			require.NoError(t, err)
			manager.securityParam = 100
			c := manager.PrimaryChain()
			blocks, err := fixtures.GenerateBabbageChainAt(1, 2, 6)
			require.NoError(t, err)
			for _, b := range blocks[:3] {
				require.NoError(t, c.AddBlock(t.Context(), b, nil))
			}
			point := func(b gledger.Block) ocommon.Point {
				return ocommon.NewPoint(b.SlotNumber(), b.Hash().Bytes())
			}
			var mutate func() error
			switch name {
			case "clear headers":
				require.NoError(
					t,
					c.AddBlockHeader(t.Context(), blocks[3].Header()),
				)
				mutate = func() error { c.ClearHeaders(); return nil }
			case "block rollback":
				mutate = func() error { return c.Rollback(t.Context(), point(blocks[0])) }
			case "queued rollback":
				for _, b := range blocks[3:5] {
					require.NoError(
						t,
						c.AddBlockHeader(t.Context(), b.Header()),
					)
				}
				mutate = func() error { return c.Rollback(t.Context(), point(blocks[3])) }
			case "batch restore":
				mutate = func() error {
					if err := c.AddBlocks(t.Context(), []gledger.Block{blocks[3], blocks[5]}); err == nil {
						return errors.New(
							"noncontiguous batch unexpectedly succeeded",
						)
					}
					return nil
				}
			case "caller rollback":
				txn := db.BlockBlobTxn()
				t.Cleanup(func() { _ = txn.Rollback() })
				require.NoError(t, c.AddBlock(t.Context(), blocks[3], txn))
				mutate = txn.Rollback
			}
			before := c.WithdrawalGeneration()
			started := make(chan uint64, 1)
			release := make(chan struct{})
			var once sync.Once
			c.onWithdrawalStarted = func() {
				once.Do(func() {
					started <- c.WithdrawalGeneration()
					<-release
				})
			}
			done := make(chan error, 1)
			finished := make(chan struct{})
			var unblock sync.Once
			t.Cleanup(func() {
				unblock.Do(func() { close(release) })
				testutil.RequireReceive(
					t,
					finished,
					testutil.AsyncWait,
					"withdrawal cleanup",
				)
			})
			go func() {
				defer close(finished)
				done <- mutate()
			}()
			during := testutil.RequireReceive(
				t,
				started,
				testutil.AsyncWait,
				"withdrawal must fence evidence",
			)
			require.Greater(
				t,
				during,
				before,
				"old ancestry evidence must be invalid before the first withdrawal mutation",
			)
			unblock.Do(func() { close(release) })
			err = testutil.RequireReceive(
				t,
				done,
				testutil.AsyncWait,
				"withdrawal completion",
			)
			require.NoError(t, err)
			require.Greater(
				t,
				c.WithdrawalGeneration(),
				during,
				"a token sampled mid-withdrawal must be invalid before the chain lock is released",
			)
		})
	}
}
