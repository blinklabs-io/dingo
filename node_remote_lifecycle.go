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
	"sync"
	"time"

	lifecyclev1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/lifecycle"
	"github.com/blinklabs-io/dingo/bark"
	"github.com/blinklabs-io/dingo/event"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/health"
	"github.com/blinklabs-io/dingo/internal/version"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ShutdownRequest is a stop or restart accepted over the remote lifecycle
// service. Timeout is the graceful timeout already resolved and capped.
type ShutdownRequest struct {
	Restart  bool
	Timeout  time.Duration
	Deadline time.Time
}

// remoteLifecycle holds the single stop or restart a node accepts.
type remoteLifecycle struct {
	mu       sync.Mutex
	state    lifecyclev1alpha1.LifecycleState
	deadline time.Time
	// accepted is the request RequestShutdown accepted, kept so
	// EndShutdownRequests returns it however the run ended.
	accepted *ShutdownRequest
	// requests signals the accepted request to the process owner.
	requests chan ShutdownRequest
}

// ShutdownRequests delivers the remote stop or restart request, at most
// once. The caller that owns the process lifetime acts on it.
func (n *Node) ShutdownRequests() <-chan ShutdownRequest {
	return n.remoteLifecycle.requests
}

// RequestShutdown accepts a remote stop, or a restart when restart is true,
// and returns the graceful timeout and deadline it will enforce. A zero
// timeout, or one longer than the configured shutdown timeout, selects the
// configured shutdown timeout: Stop bounds itself by that value, so a longer
// request could not extend the drain. Only the first request is accepted.
func (n *Node) RequestShutdown(
	restart bool,
	timeout time.Duration,
) (time.Duration, time.Time, error) {
	if restart && !restartSupported {
		return 0, time.Time{}, bark.ErrRestartUnsupported
	}
	configured := n.configuredShutdownTimeout()
	if timeout <= 0 || timeout > configured {
		timeout = configured
	}
	state := lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_STOPPING
	eventState := event.NodeLifecycleStopping
	if restart {
		state = lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_RESTARTING
		eventState = event.NodeLifecycleRestarting
	}
	deadline := time.Now().Add(timeout)

	r := &n.remoteLifecycle
	r.mu.Lock()
	if r.state != lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_UNSPECIFIED {
		r.mu.Unlock()
		return 0, time.Time{}, bark.ErrShutdownInProgress
	}
	r.state = state
	r.deadline = deadline
	n.eventBus.Publish(
		event.NodeLifecycleEventType,
		event.NewEvent(
			event.NodeLifecycleEventType,
			event.NodeLifecycleEvent{
				State:    eventState,
				Timeout:  timeout,
				Deadline: deadline,
			},
		),
	)
	req := ShutdownRequest{
		Restart: restart, Timeout: timeout, Deadline: deadline,
	}
	r.accepted = &req
	// The buffer holds the one accepted request, so this never blocks. Publish
	// the accepted state before making shutdown visible to the process owner.
	r.requests <- req
	r.mu.Unlock()
	return timeout, deadline, nil
}

// EndShutdownRequests closes the remote lifecycle intake and returns the
// request RequestShutdown accepted, if any. The process owner calls it once
// the run has ended, whatever ended it. Bark keeps serving until shutdown
// stops it, so a stop or restart arriving after this call is refused with
// ErrShutdownInProgress instead of being acknowledged and never performed.
func (n *Node) EndShutdownRequests() (ShutdownRequest, bool) {
	r := &n.remoteLifecycle
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.accepted != nil {
		return *r.accepted, true
	}
	if r.state == lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_UNSPECIFIED {
		r.state = lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_STOPPING
		r.deadline = time.Now().Add(n.configuredShutdownTimeout())
	}
	return ShutdownRequest{}, false
}

// LifecycleStatus reports whether a stop or restart is underway, probe
// equivalent health, uptime, version and chain synchronization.
func (n *Node) LifecycleStatus() *lifecyclev1alpha1.GetStatusResponse {
	r := &n.remoteLifecycle
	r.mu.Lock()
	state, deadline := r.state, r.deadline
	r.mu.Unlock()

	resp := &lifecyclev1alpha1.GetStatusResponse{
		State:   lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_RUNNING,
		Version: version.GetVersionString(),
		Uptime:  durationpb.New(time.Since(n.startedAt)),
	}
	if state != lifecyclev1alpha1.LifecycleState_LIFECYCLE_STATE_UNSPECIFIED {
		resp.State = state
		resp.Deadline = timestamppb.New(deadline)
	}

	readyGap := uint64(internalconfig.DefaultHealthReadyGapSlots)
	if n.config.cfg != nil && n.config.cfg.HealthReadyGapSlots != 0 {
		readyGap = uint64(n.config.cfg.HealthReadyGapSlots)
	}
	probe := health.Evaluate(n.TipGapSlots, readyGap)
	resp.Sync = &lifecyclev1alpha1.SyncStatus{Synced: probe.Ready}
	switch {
	case probe.Ready:
		resp.Health = lifecyclev1alpha1.HealthStatus_HEALTH_STATUS_HEALTHY
	case probe.TipGapSlots == nil:
		resp.Health = lifecyclev1alpha1.HealthStatus_HEALTH_STATUS_UNHEALTHY
	default:
		resp.Health = lifecyclev1alpha1.HealthStatus_HEALTH_STATUS_DEGRADED
		resp.Sync.SlotsBehind = *probe.TipGapSlots
	}

	// A live restore or truncate replaces n.ledgerState while holding this
	// lock, so a status poll omits the tip rather than race it or wait out
	// the operation.
	if n.liveLifecycleMu.TryLock() {
		defer n.liveLifecycleMu.Unlock()
		if n.ledgerState != nil {
			tip := n.ledgerState.Tip()
			if len(tip.Point.Hash) > 0 {
				hash := hex.EncodeToString(tip.Point.Hash)
				resp.Sync.Tip = &lifecyclev1alpha1.BlockRef{
					Hash:        &hash,
					Slot:        &tip.Point.Slot,
					BlockNumber: &tip.BlockNumber,
				}
			}
		}
	}
	return resp
}
