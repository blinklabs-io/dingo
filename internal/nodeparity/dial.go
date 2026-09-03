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
// indefinitely with no way to interrupt it. The context.AfterFunc
// registration this sets up is released when ctx itself is cancelled, same
// as any other derived-from-ctx resource; a caller dialing repeatedly
// against one long-lived ctx (cmd/node-parity's watch loop, calling Check
// every cycle against cmd.Context(), which lives for the whole process)
// accumulates one small registration per past cycle until then -- an
// accepted, bounded-by-process-lifetime tradeoff for a monitoring tool,
// not a per-call goroutine or unbounded leak.
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
	// context to cancel directly.
	stopDialCancel := context.AfterFunc(ctx, func() { rawConn.Close() }) //nolint:errcheck
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

	context.AfterFunc(ctx, func() { conn.Close() }) //nolint:errcheck

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
