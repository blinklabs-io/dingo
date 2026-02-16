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
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	// ClientAddedEventType is emitted when a new chainsync
	// client is registered.
	ClientAddedEventType event.EventType = "chainsync.client_added"

	// ClientRemovedEventType is emitted when a chainsync client
	// is unregistered (e.g. on disconnect).
	ClientRemovedEventType event.EventType = "chainsync.client_removed"

	// ClientSyncedEventType is emitted when a chainsync client
	// reaches the upstream chain tip.
	ClientSyncedEventType event.EventType = "chainsync.client_synced"

	// ClientStalledEventType is emitted when a chainsync client
	// has not received any headers within the stall timeout.
	ClientStalledEventType event.EventType = "chainsync.client_stalled"

	// ForkDetectedEventType is emitted when two clients report
	// different block hashes for the same slot.
	ForkDetectedEventType event.EventType = "chainsync.fork_detected"
)

// ClientAddedEvent contains details about a newly registered
// chainsync client.
type ClientAddedEvent struct {
	ConnId       ouroboros.ConnectionId
	TotalClients int
}

// ClientRemovedEvent contains details about a removed chainsync
// client.
type ClientRemovedEvent struct {
	ConnId       ouroboros.ConnectionId
	TotalClients int
	WasPrimary   bool
}

// ClientSyncedEvent is published when a client reaches the
// upstream chain tip.
type ClientSyncedEvent struct {
	ConnId ouroboros.ConnectionId
	Slot   uint64
}

// ClientStalledEvent is published when a client exceeds the
// stall timeout.
type ClientStalledEvent struct {
	ConnId ouroboros.ConnectionId
	Slot   uint64
}

// ForkDetectedEvent is published when two clients report
// different block hashes at the same slot.
type ForkDetectedEvent struct {
	Slot    uint64
	HashA   []byte
	HashB   []byte
	ConnIdA ouroboros.ConnectionId
	ConnIdB ouroboros.ConnectionId
	Point   ocommon.Point
}
