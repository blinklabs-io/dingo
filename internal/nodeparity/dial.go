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
	"fmt"
	"strings"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// dialTimeout bounds how long Dial waits for the initial handshake.
const dialTimeout = 10 * time.Second

// Dial opens a node-to-client (NtC) Ouroboros connection to addr, which is
// treated as a Unix socket path when it begins with "/" (real cardano-node's
// NtC endpoint) and as a TCP host:port otherwise (e.g. a socat-bridged
// devnet endpoint). The caller is responsible for closing the returned
// connection.
func Dial(addr string, magic uint32) (*ouroboros.Connection, error) {
	proto := protoFromAddr(addr)
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
	)
	if err != nil {
		return nil, fmt.Errorf("ouroboros.New: %w", err)
	}
	if err := conn.DialTimeout(proto, addr, dialTimeout); err != nil {
		conn.Close() //nolint:errcheck
		return nil, fmt.Errorf("dial %s %s: %w", proto, addr, err)
	}
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
