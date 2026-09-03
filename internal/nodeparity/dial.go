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

// Package nodeparity compares Dingo's and a reference cardano-node's ledger
// state over their node-to-client (NtC) LocalStateQuery interfaces, for
// blinklabs-io/dingo#1900. It is the shared logic behind cmd/node-parity;
// see that command for the on-demand check / polling watch CLI built on
// top of this package.
package nodeparity

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// dialTimeout bounds how long Dial waits for the initial connection and
// handshake.
const dialTimeout = 10 * time.Second

// Dial opens a node-to-client (NtC) Ouroboros connection to addr, which is
// treated as a Unix socket path when it begins with "/" (real cardano-node's
// NtC endpoint) and as a TCP host:port otherwise (e.g. a socat-bridged
// devnet endpoint). The caller is responsible for closing the returned
// connection.
//
// The connection is closed early if ctx is cancelled before the caller
// closes it: ouroboros.New performs the handshake synchronously with no
// context of its own and no per-call timeout, so a peer that accepts the
// connection and then never completes the handshake (or, once connected,
// never replies to a later query) would otherwise leave a caller blocked
// indefinitely with no way to interrupt it.
func Dial(
	ctx context.Context, addr string, magic uint32,
) (*ouroboros.Connection, error) {
	proto := protoFromAddr(addr)

	dialCtx, cancelDial := context.WithTimeout(ctx, dialTimeout)
	defer cancelDial()
	rawConn, err := (&net.Dialer{}).DialContext(dialCtx, proto, addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s %s: %w", proto, addr, err)
	}

	// rawConn is held independently of whatever ouroboros.New does with it,
	// so closing it ourselves on cancellation makes the muxer's blocked
	// read fail immediately, which New already treats as a shutdown signal
	// internally and returns an error for -- New itself cannot be passed a
	// context to cancel directly. Registered against dialCtx, not ctx: a
	// peer that accepts the socket and then stalls the handshake must be
	// bounded by dialTimeout here, the same as a peer that never accepts
	// the socket at all -- registering against the long-lived outer ctx
	// would leave this phase unbounded except by the caller's own eventual
	// cancellation (e.g. process shutdown), defeating dialTimeout's whole
	// purpose for exactly the case it exists to cover.
	stopDialCancel := context.AfterFunc(
		dialCtx, func() { rawConn.Close() },
	) //nolint:errcheck
	conn, err := ouroboros.New(
		ouroboros.WithConnection(rawConn),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
	)
	stopDialCancel()
	if err != nil {
		rawConn.Close() //nolint:errcheck
		return nil, fmt.Errorf("ouroboros.New: %w", err)
	}

	// Keep closing conn for the rest of its life if ctx is cancelled, so a
	// caller blocked in a later synchronous call (ReadTip, QuerySnapshot)
	// against an unresponsive peer is interrupted rather than hanging.
	// Released as soon as the connection closes on its own (the caller's
	// normal conn.Close()) via the ErrorChan watch below, so a caller
	// dialing repeatedly against one long-lived ctx (cmd/node-parity's
	// watch loop, calling Check every cycle against cmd.Context(), which
	// lives for the whole process) does not accumulate one of these per
	// past cycle for the life of the process.
	stop := context.AfterFunc(ctx, func() { conn.Close() }) //nolint:errcheck
	go func() {
		<-conn.ErrorChan() // closed when the connection shuts down
		stop()
	}()

	return conn, nil
}

// protoFromAddr returns "unix" for paths starting with "/" and "tcp"
// otherwise, matching internal/test/antithesis/internal/txpump's
// convention for the same choice.
func protoFromAddr(addr string) string {
	if strings.HasPrefix(addr, "/") {
		return "unix"
	}
	return "tcp"
}
