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

package ouroboros

import (
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/connection"
	okeepalive "github.com/blinklabs-io/gouroboros/protocol/keepalive"
)

// keepaliveTimeoutErrSubstring is the fragment of gouroboros protocol.go's
// state-transition timeout error
// ("%s: timeout waiting on transition from protocol state %s") that survives
// regardless of which state the client was waiting in.
const keepaliveTimeoutErrSubstring = "timeout waiting on transition"

// classifyKeepaliveTimeoutClose reports whether err is this connection's
// keep-alive client timing out waiting for a pong -- gouroboros' generic
// per-protocol state-transition timeout (protocol.go), scoped to the
// keep-alive protocol by its "keep-alive: " prefix (keepalive.ProtocolName).
// gouroboros v0.208.0 has no dedicated keep-alive timeout type or hook to
// match on instead (see blockfetch_forward.go's package doc and #4782's
// discussion of the same gap for a keep-alive RTT hook), so this is a string
// match against the one existing error shape.
//
// Returns false for a nil error, a close from any other protocol, or a clean
// shutdown -- this counts keep-alive timeouts specifically, not connection
// closes in general.
func classifyKeepaliveTimeoutClose(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.HasPrefix(msg, okeepalive.ProtocolName+":") &&
		strings.Contains(msg, keepaliveTimeoutErrSubstring)
}

func (o *Ouroboros) keepaliveConnOpts() []okeepalive.KeepAliveOptionFunc {
	opts := []okeepalive.KeepAliveOptionFunc{
		okeepalive.WithOnKeepAliveResponseReceived(
			o.instrumentKeepaliveResponse(o.keepaliveClientResponse),
		),
	}
	// Raise the client's wait-for-pong deadline when configured (Musashi). The
	// gouroboros default is a tight 10s (DefaultKeepAliveTimeout), so on a
	// single relay whose shared muxer is saturated by block/EB traffic a pong
	// delayed past 10s makes dingo drop the connection and pay a
	// reconnect+fork-rollback — even though the relay is merely slow, not dead.
	// The configured value is bounded to okeepalive.ServerTimeout, the longest
	// a client may wait for a server pong, so dingo tolerates a muxer-delayed
	// pong instead of a false-positive drop.
	// Zero leaves the gouroboros default in place (unchanged on other networks).
	if o.config.KeepAliveTimeout > 0 {
		timeout := min(o.config.KeepAliveTimeout, okeepalive.ServerTimeout)
		opts = append(opts, okeepalive.WithTimeout(timeout))
	}
	return opts
}

func (o *Ouroboros) instrumentKeepaliveResponse(
	fn func(connection.ConnectionId, uint16),
) func(connection.ConnectionId, uint16) {
	return func(connId connection.ConnectionId, cookie uint16) {
		start := time.Now()
		fn(connId, cookie)
		o.recordProtocolMessage("keepalive", nil, time.Since(start))
	}
}

func (o *Ouroboros) keepaliveClientResponse(
	connId connection.ConnectionId,
	_ uint16,
) {
	if o.eventBus == nil {
		return
	}
	evt := event.NewEvent(
		chainselection.PeerActivityEventType,
		chainselection.PeerActivityEvent{
			ConnectionId: connId,
		},
	)
	o.eventBus.Publish(chainselection.PeerActivityEventType, evt)
}
