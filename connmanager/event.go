// Copyright 2024 Blink Labs Software
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

package connmanager

import (
	"net"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

const (
	InboundConnectionEventType          = "connmanager.inbound_conn"
	ConnectionClosedEventType           = "connmanager.conn_closed"
	ConnectionRecycleRequestedEventType = "connmanager.connection_recycle_requested"
)

type InboundConnectionEvent struct {
	ConnectionId ouroboros.ConnectionId
	LocalAddr    net.Addr
	RemoteAddr   net.Addr
}

type ConnectionClosedEvent struct {
	ConnectionId ouroboros.ConnectionId
	Error        error
}

// ConnectionRecycleRequestedEvent requests that a specific
// connection be recycled.
type ConnectionRecycleRequestedEvent struct {
	ConnectionId ouroboros.ConnectionId
	ConnKey      string
	Reason       string
}
