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
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/common"
)

// lockedBuffer serializes the chain logger's writes against the test
// goroutine reading them.
type lockedBuffer struct {
	mutex sync.Mutex
	buf   bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buf.String()
}

// TestSkippedBatchRestoreIsRecorded pins that addRawBlocks records the one
// divergence it deliberately declines to repair.
//
// When a batch's commit fails, addRawBlocks restores the snapshot it took
// before the batch -- but only while the chain still shows exactly what that
// batch left behind. When something moved the chain in between, restoring
// would write that snapshot over the mutation, raising tipBlockIndex back
// above blocks a rollback deleted, so the restore is skipped. Skipping is the
// right choice and it is also the moment the in-memory chain starts claiming a
// batch the commit discarded, which surfaces later as a missing block or an
// inflated fork depth. It has to be attributable to the commit failure that
// caused it rather than only to the symptom.
//
// Each round below reads a key its batch does not write and then commits a
// separate write to that key, so the batch applies to the in-memory chain and
// its commit fails on conflict; a block add parked on the chain lock moves the
// chain in the window between the two. Whether the parked add or the batch's
// own restore reaches that lock first is scheduling, so rounds repeat until
// the add wins -- what the assertion pins is that when it does, the skip is on
// the record.
func TestSkippedBatchRestoreIsRecorded(t *testing.T) {
	const (
		securityParam = 100
		rounds        = 40
	)

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mustSetLedger(t, cm, securityParam)
	pc := cm.PrimaryChain()

	logged := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(
		logged,
		&slog.HandlerOptions{Level: slog.LevelError},
	)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	seed := chain.RawBlock{
		Slot:        10,
		Hash:        pendingCommitHash("restore-skip-seed"),
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	if err := pc.AddRawBlocks([]chain.RawBlock{seed}); err != nil {
		t.Fatalf("AddRawBlocks(seed): %v", err)
	}

	skipped := false
	for round := range rounds {
		// An index no chain block occupies, used only to conflict this
		// round's batch commit.
		conflictIndex := uint64(900001 + round)
		tip := pc.Tip()
		doomed := chain.RawBlock{
			Slot: tip.Point.Slot + 10,
			Hash: pendingCommitHash(
				fmt.Sprintf("restore-skip-doomed-%d", round),
			),
			BlockNumber: tip.BlockNumber + 1,
			Type:        1,
			PrevHash:    tip.Point.Hash,
			Cbor:        []byte{0x80},
		}
		// AddBlock takes the chain lock as its first action, so a goroutine
		// parked on it holds a place in that lock's queue rather than still
		// setting up behind it.
		mover := generateTestChain(
			t,
			doomed.BlockNumber+1,
			common.NewBlake2b256(doomed.Hash),
			doomed.Slot+10,
			10,
			1,
		)[0]

		applying := make(chan struct{})
		release := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := pc.AddRawBlocksWithCallback(
				[]chain.RawBlock{doomed},
				func(_ chain.RawBlock, txn *database.Txn) error {
					// Registers a read of a key this transaction does not
					// write, so a write committed to it before this
					// transaction commits fails that commit on conflict.
					_, _ = txn.DB().BlockByIndex(conflictIndex, txn)
					close(applying)
					<-release
					return nil
				},
			)
			if err == nil {
				t.Errorf("round %d: expected a commit failure", round)
			}
		}()
		<-applying
		if err := db.BlockCreate(models.Block{
			ID:     conflictIndex,
			Slot:   conflictIndex,
			Hash:   pendingCommitHash(fmt.Sprintf("restore-skip-conflict-%d", round)),
			Number: conflictIndex,
			Type:   1,
			Cbor:   []byte{0x80},
		}, nil); err != nil {
			t.Fatalf("round %d: conflicting write: %v", round, err)
		}
		moved := make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			moved <- pc.AddBlock(mover, nil)
		}()
		// Give the add time to park on the chain lock the batch holds.
		time.Sleep(5 * time.Millisecond)
		close(release)
		wg.Wait()
		if <-moved == nil {
			// The add reached the chain first, so the batch found the chain
			// moved and skipped its restore.
			skipped = true
			break
		}
	}
	if !skipped {
		t.Skip("the chain never moved inside the commit-failure window")
	}

	record := logged.String()
	if !strings.Contains(
		record,
		"skipped in-memory restore after batch commit failure",
	) {
		t.Fatalf("skipped restore was not recorded; log was:\n%s", record)
	}
	for _, field := range []string{
		"applied_tip_block_index=",
		"tip_block_index=",
		"applied_generation=",
		"mutation_generation=",
	} {
		if !strings.Contains(record, field) {
			t.Errorf(
				"skipped-restore record is missing %q; log was:\n%s",
				field,
				record,
			)
		}
	}
}
