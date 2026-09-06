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
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// pendingCommitHash builds a distinct block hash for a label.
func pendingCommitHash(label string) []byte {
	sum := sha256.Sum256([]byte(label))
	return sum[:]
}

// TestRollbackDoesNotResolveUncommittedBlockIndex pins that a rollback never
// resolves a block index that a chain-owned batch has applied to the in-memory
// chain but not yet committed.
//
// addRawBlocks advances c.tipBlockIndex inside its transaction's closure,
// under c.mutex, and txn.Do commits only after that closure has returned and
// both chain locks are released. A rollback that took c.mutex in between read
// a tip index the store could not serve -- ChainManager.removeBlockByIndex
// opens its own transaction, which cannot see another transaction's
// uncommitted writes -- and rollbackLocked's removal loop failed its first
// iteration with "remove block at index N: block not found" at an index the
// chain legitimately held. That is issue #3979, observed on CI as an
// intermittent failure of
// ledger.TestWindowedRewindConvergesWhilePrimaryChainExtends, whose appender
// goroutine and windowed rewind are the same pairing.
//
// The rollback target is origin so the rollback reaches its removal loop
// through the shortest path available (rollbackPointBlock is skipped for
// origin), which is what makes the pre-fix window observable often enough to
// be a regression test rather than a lottery: without the batch-commit
// barrier this reports a not-found index in roughly half of the rounds below.
func TestRollbackDoesNotResolveUncommittedBlockIndex(t *testing.T) {
	const (
		// Larger than any chain this test builds, so a rollback to origin is
		// never refused for exceeding K and every round exercises the
		// removal loop.
		securityParam = 5000
		// blockImportBatchSize, so the whole batch lands in one transaction.
		batch   = 50
		payload = 4 * 1024
		rounds  = 40
	)
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mustSetLedger(t, cm, securityParam)
	pc := cm.PrimaryChain()

	cbor := make([]byte, payload)
	cbor[0] = 0x80
	seq := 0
	nextBatch := func(n int) []chain.RawBlock {
		tip := pc.Tip()
		out := make([]chain.RawBlock, 0, n)
		prev := tip.Point.Hash
		slot := tip.Point.Slot
		num := tip.BlockNumber
		for range n {
			seq++
			slot++
			num++
			hash := pendingCommitHash(fmt.Sprintf("pending-commit-%d", seq))
			out = append(out, chain.RawBlock{
				Slot:        slot,
				Hash:        hash,
				BlockNumber: num,
				Type:        1,
				PrevHash:    prev,
				Cbor:        cbor,
			})
			prev = hash
		}
		return out
	}

	for round := range rounds {
		blocks := nextBatch(batch)
		applying := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The callback runs inside the batch transaction, under both
			// chain locks, so it marks the point at which the in-memory
			// chain has started moving ahead of the store.
			if err := pc.AddRawBlocksWithCallback(
				blocks,
				func(_ chain.RawBlock, _ *database.Txn) error {
					once.Do(func() {
						close(applying)
						<-release
					})
					return nil
				},
			); err != nil {
				t.Errorf("round %d: AddRawBlocksWithCallback: %v", round, err)
			}
		}()
		<-applying
		var rollbackErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			rollbackErr = pc.Rollback(ocommon.Point{})
		}()
		// Give the rollback time to park on the lock the batch holds, so it
		// runs in the window between the batch releasing that lock and its
		// transaction committing.
		time.Sleep(10 * time.Millisecond)
		close(release)
		wg.Wait()
		if errors.Is(rollbackErr, models.ErrBlockNotFound) {
			t.Fatalf(
				"round %d: rollback resolved a block index the batch had not committed: %v",
				round,
				rollbackErr,
			)
		}
		if rollbackErr != nil {
			t.Fatalf("round %d: rollback: %v", round, rollbackErr)
		}
	}
}
