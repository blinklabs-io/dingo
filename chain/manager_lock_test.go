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
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func TestRewindPrimaryChainToPointDoesNotDeadlockWithIterator(t *testing.T) {
	cm, err := NewManager(nil, nil)
	if err != nil {
		t.Fatalf("NewManager: %s", err)
	}
	primaryChain := cm.PrimaryChain()
	primaryChain.persistent = true

	cm.mutex.Lock()
	iter := &ChainIterator{
		chain:          primaryChain,
		nextBlockIndex: initialBlockIndex,
		ctx:            context.Background(),
	}
	iterDone := make(chan error, 1)
	go func() {
		_, err := primaryChain.iterNext(iter, false)
		iterDone <- err
	}()
	testutil.WaitForCondition(t, func() bool {
		if primaryChain.mutex.TryLock() {
			primaryChain.mutex.Unlock()
			return false
		}
		return true
	}, time.Second, "iterator should hold primary chain lock")

	rewindStarted := make(chan struct{})
	rewindDone := make(chan error, 1)
	go func() {
		close(rewindStarted)
		rewindDone <- cm.RewindPrimaryChainToPoint(ocommon.NewPointOrigin())
	}()
	testutil.RequireReceive(
		t,
		rewindStarted,
		time.Second,
		"rewind goroutine should start",
	)
	runtime.Gosched()
	cm.mutex.Unlock()

	err = testutil.RequireReceive(
		t,
		iterDone,
		time.Second,
		"iterator should complete after manager lock is released",
	)
	if !errors.Is(err, ErrIteratorChainTip) {
		t.Fatalf("iterator error = %v, want %v", err, ErrIteratorChainTip)
	}
	err = testutil.RequireReceive(
		t,
		rewindDone,
		time.Second,
		"rewind should complete after iterator releases chain lock",
	)
	if err != nil {
		t.Fatalf("rewind: %s", err)
	}
}
