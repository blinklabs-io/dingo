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

package connmanager

import "net"

// enableTCPLingerZero sets SO_LINGER to 0 on a TCP connection so
// Close sends RST instead of FIN and the kernel skips TIME_WAIT.
// This is required when OutboundSourcePort source-port reuse is
// active: both inbound and outbound connections share the local
// listen port, so a closed connection's 4-tuple stays in TIME_WAIT
// and the next dial to the same peer fails with EADDRNOTAVAIL until
// the kernel releases it (typically tcp_fin_timeout, default 60s on
// Linux). With many peers and any churn (handshake failure, intersect
// lookup failure, plateau recovery), the source port saturates with
// TIME_WAIT 4-tuples and outbound dials fail consistently.
//
// Safe for Cardano N2N protocols: chainsync, blockfetch, and
// txsubmission state is reconstructed from peer history on reconnect,
// and ouroboros multiplexer shutdowns already drop in-flight messages
// when a connection is closed for stall recovery. Unix-socket NtC
// connections are unaffected because the type assertion fails for
// non-TCP connections and the call is a no-op.
func enableTCPLingerZero(conn net.Conn) error {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}
	return tc.SetLinger(0)
}
