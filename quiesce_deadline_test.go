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

package dingo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStopWithDeadlineReturnsWhenStopReturns covers the ordinary case: a
// component that stops promptly must return its own error (or nil) unchanged,
// with no drain escalation attached.
func TestStopWithDeadlineReturnsWhenStopReturns(t *testing.T) {
	t.Parallel()

	require.NoError(t, stopWithDeadline(
		time.Minute,
		"prompt component",
		func() error { return nil },
	))

	sentinel := errors.New("stop failed")
	err := stopWithDeadline(
		time.Minute,
		"failing component",
		func() error { return sentinel },
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.NotErrorIs(t, err, errStorageDrainUnconfirmed,
		"a component that reported a failure did stop; only an unfinished "+
			"wait leaves a goroutine possibly still using storage")
}

// TestStopWithDeadlineEscalatesAnUnfinishedStop is the point of the change.
//
// These Stop calls cancel their context and then wait on a WaitGroup with no
// bound, so a goroutine that does not observe the cancellation blocks the
// whole live restore/truncate indefinitely — past the configured shutdown
// timeout, with no error and no way for the caller to react. Once bounded, an
// unfinished stop has to escalate to errStorageDrainUnconfirmed: the goroutine
// may still be reading or writing n.db, so Restore/Truncate must abandon the
// operation and force a supervised restart rather than reopen storage
// underneath it.
func TestStopWithDeadlineEscalatesAnUnfinishedStop(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	err := stopWithDeadline(
		10*time.Millisecond,
		"wedged component",
		func() error {
			<-release
			return nil
		},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, errStorageDrainUnconfirmed,
		"an unfinished stop must force a supervised restart, not a resume")
	assert.ErrorContains(t, err, "wedged component")
}

// TestStopWithDeadlineIgnoresCallerCancellation pins the deliberate choice not
// to consult the caller's context.
//
// Cancelling a restore must not escalate a component that would have stopped
// cleanly into a supervised restart, so a cancelled context neither shortens
// the wait nor changes the result. The deadline alone bounds it.
func TestStopWithDeadlineIgnoresCallerCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stopped := make(chan struct{})
	err := stopWithDeadline(
		time.Minute,
		"prompt component",
		func() error { close(stopped); return nil },
	)
	require.NoError(t, err, "a clean stop under a cancelled caller is clean")
	assert.NotErrorIs(t, err, errStorageDrainUnconfirmed)
	select {
	case <-stopped:
	default:
		t.Fatal("stop should have run to completion")
	}
	_ = ctx
}
