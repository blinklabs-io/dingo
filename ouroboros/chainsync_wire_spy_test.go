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
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

const csjTestNetworkMagic = 42

// chainsyncWireMessage is one ChainSync message the node wrote to its peer.
type chainsyncWireMessage uint8

// wireSpyConn records the ChainSync messages written to the bearer. Mux
// segments carry an 8-byte header (timestamp, protocol id, payload length)
// followed by the CBOR payload, whose first two bytes are the array header and
// the message type of every ChainSync message.
type wireSpyConn struct {
	net.Conn
	mu       sync.Mutex
	pending  []byte
	messages chan chainsyncWireMessage
}

func newWireSpyConn(c net.Conn) *wireSpyConn {
	return &wireSpyConn{Conn: c, messages: make(chan chainsyncWireMessage, 256)}
}

func (c *wireSpyConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.pending = append(c.pending, p...)
	for len(c.pending) >= 8 {
		payloadLen := int(binary.BigEndian.Uint16(c.pending[6:8]))
		if len(c.pending) < 8+payloadLen {
			break
		}
		protocolId := binary.BigEndian.Uint16(c.pending[4:6]) & 0x7fff
		payload := c.pending[8 : 8+payloadLen]
		if protocolId == ochainsync.ProtocolIdNtN && len(payload) >= 2 {
			select {
			case c.messages <- chainsyncWireMessage(payload[1]):
			default:
			}
		}
		c.pending = c.pending[8+payloadLen:]
	}
	c.mu.Unlock()
	return c.Conn.Write(p)
}

// next blocks until the node writes its next ChainSync message.
func (c *wireSpyConn) next(t *testing.T, what string) chainsyncWireMessage {
	t.Helper()
	return testutil.RequireReceive(t, c.messages, 5*time.Second, what)
}

// requireQuiet fails when a ChainSync message is written within a short
// window. The muxer writes from its own goroutine, so a message the action
// queued can reach the wire after the action returns.
func (c *wireSpyConn) requireQuiet(t *testing.T) {
	t.Helper()
	testutil.RequireNoReceive(t, c.messages, 100*time.Millisecond,
		"unexpected ChainSync message on the wire")
}
