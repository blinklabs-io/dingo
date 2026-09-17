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
	"testing"

	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestWithTipHoldsLockAndPropagatesCallbackError(t *testing.T) {
	t.Parallel()
	tip := chainsync.Tip{
		Point:       common.Point{Slot: 10, Hash: []byte{1, 2, 3}},
		BlockNumber: 5,
	}
	c := &Chain{currentTip: tip}
	wantErr := errors.New("callback failed")
	called := false
	err := c.WithTip(func(got chainsync.Tip) error {
		called = true
		require.Equal(t, tip, got)
		// No other goroutine holds this mutex. Successful acquisition here
		// would mean the production callback is outside its critical section.
		if c.mutex.TryLock() {
			c.mutex.Unlock()
			t.Error("WithTip invoked callback without holding the chain lock")
		}
		return wantErr
	})
	require.True(t, called)
	require.ErrorIs(t, err, wantErr)
	require.True(t, c.mutex.TryLock(), "callback error left chain locked")
	c.mutex.Unlock()
}

func TestWithTipNilChainDoesNotInvokeCallback(t *testing.T) {
	t.Parallel()
	var c *Chain
	err := c.WithTip(func(chainsync.Tip) error {
		t.Fatal("nil chain invoked callback")
		return nil
	})
	require.Error(t, err)
}
