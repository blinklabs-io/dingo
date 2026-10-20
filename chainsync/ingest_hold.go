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

package chainsync

import (
	"sync"
	"sync/atomic"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// IngestHold carries the network ingest hold to whichever State is current.
// The node creates one for its lifetime and gives it to the ledger as the
// hold tracker, so a hold and its release always reach the live State and a
// State rebuilt by a live restore inherits the hold. Neither call may be
// skipped: a lost hold leaves connections unmarked, and a lost release marks
// every peer that reconnects afterwards.
type IngestHold struct {
	// mu orders hold changes against Attach, so a State attached while a
	// change is in progress ends with the final value.
	mu    sync.Mutex
	held  bool
	state atomic.Pointer[State]
}

// SetNetworkIngestHeld records the hold and applies it to the attached State.
func (h *IngestHold) SetNetworkIngestHeld(held bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.held = held
	if state := h.state.Load(); state != nil {
		state.SetNetworkIngestHeld(held)
	}
}

// ConnectionIngestHeld reports whether the attached State has marked connId.
func (h *IngestHold) ConnectionIngestHeld(
	connId ouroboros.ConnectionId,
) bool {
	state := h.state.Load()
	return state != nil && state.ConnectionIngestHeld(connId)
}

// Attach makes state the current State, applying the current hold to it.
// A nil state detaches the previous one while it is rebuilt.
func (h *IngestHold) Attach(state *State) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state.Store(state)
	if state != nil && h.held {
		state.SetNetworkIngestHeld(true)
	}
}
