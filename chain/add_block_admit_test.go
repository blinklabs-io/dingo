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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TestAddBlockWithPointDeferredIfRefusesWithoutMutating pins the fail-closed
// half of the admission predicate: a false answer abandons the add before the
// chain is read or written, and says so with a distinguishable error rather
// than one that looks like an invalid block.
func TestAddBlockWithPointDeferredIfRefusesWithoutMutating(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	tipBefore := c.Tip()

	next := testBlocks[3]
	point := ocommon.NewPoint(next.MockSlot, next.Hash().Bytes())
	calls := 0
	_, err = c.AddBlockWithPointDeferredIf(next, point, nil, func() bool {
		calls++
		return false
	})
	if !errors.Is(err, chain.ErrBlockAddNotAdmitted) {
		t.Fatalf("expected ErrBlockAddNotAdmitted, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("admit must be evaluated exactly once, got %d", calls)
	}
	if got := c.Tip(); !bytes.Equal(got.Point.Hash, tipBefore.Point.Hash) {
		t.Fatalf(
			"a refused add must not move the tip: %x -> %x",
			tipBefore.Point.Hash,
			got.Point.Hash,
		)
	}

	// The same block is added once the predicate admits it, so the refusal
	// above was the predicate's doing and not an unrelated rejection.
	if _, err := c.AddBlockWithPointDeferredIf(
		next, point, nil, func() bool { return true },
	); err != nil {
		t.Fatalf("unexpected error adding admitted block: %s", err)
	}
	if got := c.Tip(); !bytes.Equal(got.Point.Hash, next.Hash().Bytes()) {
		t.Fatalf("admitted block did not become the tip: %x", got.Point.Hash)
	}
}

// TestAddBlockWithPointDeferredIfEvaluatesAdmitUnderTheChainMutex is the
// property the predicate exists for. A caller that tests its own precondition
// and then calls an ordinary add has released nothing but holds nothing
// either: a concurrent mutation can land in between and make the test stale.
// Evaluating the predicate inside the add, under the mutex that serializes
// every chain mutation, is what makes the test and the mutation it guards one
// atomic step.
//
// Proven by observing that another goroutine's add cannot make progress while
// the predicate is running: if the predicate ran outside the lock, that add
// would complete immediately.
func TestAddBlockWithPointDeferredIfEvaluatesAdmitUnderTheChainMutex(
	t *testing.T,
) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}

	next := testBlocks[3]
	point := ocommon.NewPoint(next.MockSlot, next.Hash().Bytes())
	competitor := &MockBlock{
		MockBlockNumber: next.MockBlockNumber,
		MockSlot:        next.MockSlot + 1,
		MockHash:        testHashPrefix + "00fd",
		MockPrevHash:    testBlocks[2].MockHash,
	}
	competitorDone := make(chan struct{})
	blockedDuringAdmit := false

	_, err = c.AddBlockWithPointDeferredIf(next, point, nil, func() bool {
		go func() {
			defer close(competitorDone)
			_ = c.AddBlock(competitor, nil)
		}()
		// The competing add must be parked on the chain mutex we are
		// holding. A generous window keeps this from depending on
		// scheduling speed: the assertion is that it does NOT finish.
		select {
		case <-competitorDone:
			blockedDuringAdmit = false
		case <-time.After(250 * time.Millisecond):
			blockedDuringAdmit = true
		}
		return true
	})
	if err != nil {
		t.Fatalf("unexpected error adding block: %s", err)
	}
	<-competitorDone
	if !blockedDuringAdmit {
		t.Fatal(
			"a concurrent add completed while the admission predicate was " +
				"running, so the predicate is not evaluated under the chain mutex",
		)
	}
}
