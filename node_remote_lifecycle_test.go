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
	"encoding/hex"
	"testing"
	"time"

	lifecyclev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/lifecycle"
	"github.com/blinklabs-io/dingo/bark"
	"github.com/blinklabs-io/dingo/event"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRemoteLifecycleTestNode(t *testing.T) *Node {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	return &Node{
		eventBus:  bus,
		startedAt: time.Now(),
		remoteLifecycle: remoteLifecycle{
			requests: make(chan ShutdownRequest, 1),
		},
	}
}

var _ bark.NodeControl = (*Node)(nil)

func TestRequestShutdownAcceptsOnlyTheFirstRequest(t *testing.T) {
	t.Parallel()

	n := newRemoteLifecycleTestNode(t)
	_, sub := n.eventBus.Subscribe(event.NodeLifecycleEventType)

	before := time.Now()
	timeout, deadline, err := n.RequestShutdown(false, 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, timeout)
	assert.False(t, deadline.Before(before.Add(5*time.Second)))

	req := testutil.RequireReceive(
		t, n.ShutdownRequests(), 5*time.Second, "shutdown request",
	)
	assert.Equal(t, ShutdownRequest{
		Timeout: 5 * time.Second, Deadline: deadline,
	}, req)

	evt := testutil.RequireReceive(t, sub, 5*time.Second, "lifecycle event")
	payload, ok := evt.Data.(event.NodeLifecycleEvent)
	require.True(t, ok)
	assert.Equal(t, event.NodeLifecycleStopping, payload.State)
	assert.Equal(t, 5*time.Second, payload.Timeout)
	assert.True(t, payload.Deadline.Equal(deadline))

	_, _, err = n.RequestShutdown(false, time.Second)
	require.ErrorIs(t, err, bark.ErrShutdownInProgress)
	_, _, err = n.RequestShutdown(true, time.Second)
	require.ErrorIs(t, err, bark.ErrShutdownInProgress)
	testutil.RequireNoReceive(
		t, n.ShutdownRequests(), 50*time.Millisecond, "second request",
	)
}

func TestRequestShutdownDefaultsAndCapsTimeout(t *testing.T) {
	t.Parallel()

	n := newRemoteLifecycleTestNode(t)
	timeout, _, err := n.RequestShutdown(false, 0)
	require.NoError(t, err)
	assert.Equal(t, n.configuredShutdownTimeout(), timeout)

	n = newRemoteLifecycleTestNode(t)
	timeout, _, err = n.RequestShutdown(false, 24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, n.configuredShutdownTimeout(), timeout)
}

func TestRequestShutdownRestart(t *testing.T) {
	t.Parallel()

	n := newRemoteLifecycleTestNode(t)
	_, sub := n.eventBus.Subscribe(event.NodeLifecycleEventType)
	_, _, err := n.RequestShutdown(true, time.Second)
	if !restartSupported {
		require.ErrorIs(t, err, bark.ErrRestartUnsupported)
		return
	}
	require.NoError(t, err)
	req := testutil.RequireReceive(
		t, n.ShutdownRequests(), 5*time.Second, "restart request",
	)
	assert.True(t, req.Restart)
	evt := testutil.RequireReceive(t, sub, 5*time.Second, "lifecycle event")
	payload, ok := evt.Data.(event.NodeLifecycleEvent)
	require.True(t, ok)
	assert.Equal(t, event.NodeLifecycleRestarting, payload.State)
}

func TestLifecycleStatusReportsShutdownState(t *testing.T) {
	t.Parallel()

	n := newRemoteLifecycleTestNode(t)
	status := n.LifecycleStatus()
	assert.Equal(
		t,
		lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_RUNNING,
		status.GetState(),
	)
	assert.Nil(t, status.Deadline)
	assert.NotEmpty(t, status.GetVersion())
	assert.False(t, status.GetSync().GetSynced())
	assert.Equal(
		t,
		lifecyclev1alpha1.HealthStatus_HEALTH_STATUS_UNHEALTHY,
		status.GetHealth(),
	)

	_, deadline, err := n.RequestShutdown(false, time.Minute)
	require.NoError(t, err)
	status = n.LifecycleStatus()
	assert.Equal(
		t,
		lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_STOPPING,
		status.GetState(),
	)
	assert.True(
		t,
		status.GetDeadline().AsTime().Equal(deadline.Truncate(time.Nanosecond)),
	)
}

func TestLifecycleStatusReportsSyncAndHealth(t *testing.T) {
	t.Parallel()

	n := newRemoteLifecycleTestNode(t)
	n.health.mu.Lock()
	n.health.tipGapKnown = true
	n.health.tipGapSlots = 5
	n.health.mu.Unlock()
	status := n.LifecycleStatus()
	assert.True(t, status.GetSync().GetSynced())
	assert.Equal(
		t,
		lifecyclev1alpha1.HealthStatus_HEALTH_STATUS_HEALTHY,
		status.GetHealth(),
	)

	n.health.mu.Lock()
	n.health.tipGapSlots = 1_000_000
	n.health.mu.Unlock()
	status = n.LifecycleStatus()
	assert.False(t, status.GetSync().GetSynced())
	assert.Equal(t, uint64(1_000_000), status.GetSync().GetSlotsBehind())
	assert.Equal(
		t,
		lifecyclev1alpha1.HealthStatus_HEALTH_STATUS_DEGRADED,
		status.GetHealth(),
	)
}

// TestRequestShutdownNeverExceedsConfiguredShutdownTimeout pins the reported
// timeout to the bound Node.Stop enforces: Node.shutdown runs under the
// configured shutdown timeout, so a longer request cannot extend the drain
// and must not be reported as if it did.
func TestRequestShutdownNeverExceedsConfiguredShutdownTimeout(t *testing.T) {
	t.Parallel()

	newNode := func() *Node {
		n := newRemoteLifecycleTestNode(t)
		n.config.cfg = &internalconfig.Config{ShutdownTimeout: "2m"}
		return n
	}
	timeout, deadline, err := newNode().RequestShutdown(false, 5*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, timeout)
	assert.False(t, deadline.After(time.Now().Add(2*time.Minute)))

	timeout, _, err = newNode().RequestShutdown(false, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, time.Minute, timeout)
}

// TestEndShutdownRequestsRefusesLaterRequests covers a stop or restart that
// reaches Bark after the run ended without one, as after a signal: Bark is
// still serving, so the request must be refused rather than acknowledged and
// never performed.
func TestEndShutdownRequestsRefusesLaterRequests(t *testing.T) {
	t.Parallel()

	n := newRemoteLifecycleTestNode(t)
	_, ok := n.EndShutdownRequests()
	require.False(t, ok)

	_, _, err := n.RequestShutdown(false, time.Second)
	require.ErrorIs(t, err, bark.ErrShutdownInProgress)
	if restartSupported {
		_, _, err = n.RequestShutdown(true, time.Second)
		require.ErrorIs(t, err, bark.ErrShutdownInProgress)
	}
	testutil.RequireNoReceive(
		t, n.ShutdownRequests(), 50*time.Millisecond, "late request",
	)
	status := n.LifecycleStatus()
	assert.Equal(
		t,
		lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_STOPPING,
		status.GetState(),
	)
	assert.NotNil(t, status.GetDeadline())
}

// TestEndShutdownRequestsReturnsTheAcceptedRequest covers a request whose
// signal was already consumed from ShutdownRequests: ending the run still
// returns it, every time it is asked.
func TestEndShutdownRequestsReturnsTheAcceptedRequest(t *testing.T) {
	t.Parallel()

	n := newRemoteLifecycleTestNode(t)
	timeout, deadline, err := n.RequestShutdown(false, time.Second)
	require.NoError(t, err)
	testutil.RequireReceive(
		t, n.ShutdownRequests(), 5*time.Second, "shutdown request",
	)
	want := ShutdownRequest{Timeout: timeout, Deadline: deadline}
	for range 2 {
		req, ok := n.EndShutdownRequests()
		require.True(t, ok)
		assert.Equal(t, want, req)
	}
}

// TestLifecycleStatusReportsLedgerTip covers the chain tip in GetStatus, and
// its omission while a live restore or truncate holds liveLifecycleMu.
func TestLifecycleStatusReportsLedgerTip(t *testing.T) {
	t.Parallel()

	hash := []byte{0xde, 0xad, 0xbe, 0xef}
	ls := &ledger.LedgerState{}
	ls.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.NewPoint(4242, hash),
		BlockNumber: 17,
	})
	n := newRemoteLifecycleTestNode(t)
	n.ledgerState = ls

	tip := n.LifecycleStatus().GetSync().GetTip()
	require.NotNil(t, tip)
	assert.Equal(t, hex.EncodeToString(hash), tip.GetHash())
	assert.Equal(t, uint64(4242), tip.GetSlot())
	assert.Equal(t, uint64(17), tip.GetBlockNumber())

	n.liveLifecycleMu.Lock()
	defer n.liveLifecycleMu.Unlock()
	assert.Nil(t, n.LifecycleStatus().GetSync().GetTip())
}
