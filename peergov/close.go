// Copyright 2025 Blink Labs Software
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

package peergov

import (
	"io"
	"log/slog"

	"github.com/blinklabs-io/dingo/connmanager"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// closeConnAndLog closes conn, logging at Debug level if it fails. Every
// caller already logs the reason the connection is being torn down, so a
// close failure here is a secondary diagnostic, not the primary event.
func closeConnAndLog(
	logger *slog.Logger,
	conn io.Closer,
	msg string,
	attrs ...any,
) {
	if conn == nil {
		return
	}
	if err := conn.Close(); err != nil {
		logger.Debug(msg, append(attrs, "error", err)...)
	}
}

// deferredClose is a connection a governor pass detached from its peer with
// p.mu held. The pass closes it only after releasing p.mu: a connection's
// Close may wait for its running protocol callbacks, and chain-sync and
// keep-alive callbacks take p.mu, so a Close under p.mu can deadlock.
type deferredClose struct {
	conn    io.Closer
	msg     string
	address string
}

// connCloserLocked returns the live connection with the given id, or nil if
// the connection manager no longer holds it. Called with p.mu held; it does
// not close anything.
func (p *PeerGovernor) connCloserLocked(id ouroboros.ConnectionId) io.Closer {
	if p.connCloser != nil {
		return p.connCloser(id)
	}
	if p.config.ConnManager == nil {
		return nil
	}
	// A nil *ouroboros.Connection stored in an io.Closer is not a nil
	// interface, so check before converting. closeDeferred runs after p.mu
	// is released and outside any callback of conn, so the close may wait
	// for conn's callbacks.
	if conn := p.config.ConnManager.GetConnectionById(id); conn != nil {
		return connmanager.OuroborosCloser{Conn: conn}
	}
	return nil
}

// deferCloseLocked queues the live connection with the given id for closing
// once the caller has released p.mu.
func (p *PeerGovernor) deferCloseLocked(
	closes []deferredClose,
	id ouroboros.ConnectionId,
	msg string,
	address string,
) []deferredClose {
	conn := p.connCloserLocked(id)
	if conn == nil {
		return closes
	}
	return append(closes, deferredClose{
		conn:    conn,
		msg:     msg,
		address: address,
	})
}

// closeDeferred closes connections queued by deferCloseLocked. Call without
// p.mu held.
func (p *PeerGovernor) closeDeferred(closes []deferredClose) {
	for _, c := range closes {
		closeConnAndLog(p.config.Logger, c.conn, c.msg, "address", c.address)
	}
}
