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
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/connection"
	okeepalive "github.com/blinklabs-io/gouroboros/protocol/keepalive"
)

// keepalivePongTimeoutErr is the exact message gouroboros protocol.go
// reports when the keep-alive client's transition timer for the Server state
// (waiting on the peer's pong) fires. The server side's ping-wait timeout
// reports the Client state instead, so it does not match.
var keepalivePongTimeoutErr = fmt.Sprintf(
	"%s: timeout waiting on transition from protocol state %s",
	okeepalive.ProtocolName,
	okeepalive.StateServer,
)

// classifyKeepaliveTimeoutClose reports whether err is a keep-alive pong
// timeout. gouroboros v0.208.0 has no typed error for it, and its connection
// wraps every forwarded mini-protocol error ("protocol error: %w" in
// connection.go), so this matches the protocol.go message exactly at any
// level of the wrap chain rather than against the outer string.
func classifyKeepaliveTimeoutClose(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if err.Error() == keepalivePongTimeoutErr {
			return true
		}
	}
	return false
}

func (o *Ouroboros) keepaliveConnOpts() []okeepalive.KeepAliveOptionFunc {
	opts := []okeepalive.KeepAliveOptionFunc{
		// Server side: a downstream peer pinging us is not idle.
		okeepalive.WithOnKeepAliveReceived(
			func(connId connection.ConnectionId, _ uint16) {
				o.recordServedActivity(connId)
			},
		),
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

// servedActivityReportInterval bounds how often one connection's server-side
// activity is forwarded to the peer governor. It is far below
// InboundPruneAfter, and keeps the per-header chainsync path off the governor
// lock.
const servedActivityReportInterval = 10 * time.Second

type servedActivityKey struct {
	local, remote netip.AddrPort
}

func servedActivityKeyFor(
	connId connection.ConnectionId,
) (servedActivityKey, bool) {
	l, lok := connId.LocalAddr.(*net.TCPAddr)
	r, rok := connId.RemoteAddr.(*net.TCPAddr)
	if !lok || !rok || l == nil || r == nil {
		return servedActivityKey{}, false
	}
	return servedActivityKey{local: l.AddrPort(), remote: r.AddrPort()}, true
}

// recordServedActivity tells the peer governor that the downstream peer on
// connId is consuming from this node, at most once per
// servedActivityReportInterval per connection. The hot path is a sync.Map
// load and an atomic compare-and-swap; it never takes the governor lock
// between reports.
func (o *Ouroboros) recordServedActivity(connId connection.ConnectionId) {
	report := o.reportServedActivity
	key, ok := servedActivityKeyFor(connId)
	if !ok {
		// Not TCP: a node-to-client unix-socket connection sharing these
		// server handlers. The governor tracks no peer for it, and reporting
		// unthrottled would take the governor lock per served header.
		return
	}
	now := time.Now().UnixNano()
	v, loaded := o.servedActivityLast.Load(key)
	if !loaded {
		v, _ = o.servedActivityLast.LoadOrStore(key, new(atomic.Int64))
	}
	last := v.(*atomic.Int64)
	prev := last.Load()
	if prev != 0 && now-prev < int64(servedActivityReportInterval) {
		return
	}
	if !last.CompareAndSwap(prev, now) {
		return // a concurrent caller is reporting
	}
	report(connId)
}

func (o *Ouroboros) reportServedActivity(connId connection.ConnectionId) {
	if o.servedActivityHook != nil {
		o.servedActivityHook(connId)
		return
	}
	if o.peerGov != nil {
		o.peerGov.RecordServedActivityByConnId(connId)
	}
}

func (o *Ouroboros) forgetServedActivity(connId connection.ConnectionId) {
	if key, ok := servedActivityKeyFor(connId); ok {
		o.servedActivityLast.Delete(key)
	}
}
