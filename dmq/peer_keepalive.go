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

package dmq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/keepalive"
)

// keepAliveProtocolID is the mini-protocol number dmq-node runs keep-alive on.
// Cardano's keep-alive uses 8, which is what the gouroboros keep-alive package
// is fixed to, so a DMQ connection runs its own instance of the same protocol.
const keepAliveProtocolID uint16 = 12

// keepAliveName names the protocol in logs.
const keepAliveName = "dmq-keep-alive"

const (
	// keepAliveDefaultPeriod is the pause between a reply and the next ping.
	// It is well inside the 97 seconds a peer's keep-alive server waits for
	// one before ending the connection.
	keepAliveDefaultPeriod = 60 * time.Second
	// keepAliveDefaultTimeout is how long a ping may go unanswered.
	keepAliveDefaultTimeout = keepalive.ServerTimeout
)

// keepAliveClient pings the peer and checks the cookie in each reply. It is the
// gouroboros keep-alive protocol on DMQ's mini-protocol number: the same
// states and messages, so the peer's keep-alive server answers it unchanged.
type keepAliveClient struct {
	*protocol.Protocol
	period time.Duration

	// replies counts the pings the peer has answered.
	replies atomic.Int64

	mu      sync.Mutex
	cookie  uint16
	timer   *time.Timer
	stopped bool
}

func newKeepAliveClient(
	opts protocol.ProtocolOptions,
	period, responseTimeout time.Duration,
) *keepAliveClient {
	c := &keepAliveClient{period: period}
	stateMap := keepalive.StateMap.Copy()
	if entry, ok := stateMap[keepalive.StateServer]; ok {
		entry.Timeout = responseTimeout
		stateMap[keepalive.StateServer] = entry
	}
	c.Protocol = protocol.New(protocol.ProtocolConfig{
		Name:                keepAliveName,
		ProtocolId:          keepAliveProtocolID,
		Muxer:               opts.Muxer,
		Logger:              opts.Logger,
		ErrorChan:           opts.ErrorChan,
		Mode:                opts.Mode,
		Role:                protocol.ProtocolRoleClient,
		MessageHandlerFunc:  c.handle,
		MessageFromCborFunc: keepalive.NewMsgFromCbor,
		StateMap:            stateMap,
		InitialState:        keepalive.StateClient,
	})
	return c
}

// Start begins the protocol and sends the first ping.
func (c *keepAliveClient) Start() {
	c.Protocol.Start()
	c.ping()
}

func (c *keepAliveClient) ping() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	cookie := c.cookie
	c.mu.Unlock()
	if err := c.SendMessage(keepalive.NewMsgKeepAlive(cookie)); err != nil {
		c.SendError(err)
	}
}

func (c *keepAliveClient) handle(msg protocol.Message) error {
	resp, ok := msg.(*keepalive.MsgKeepAliveResponse)
	if !ok {
		return fmt.Errorf(
			"%s: unexpected message type %d", keepAliveName, msg.Type(),
		)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if resp.Cookie != c.cookie {
		return fmt.Errorf(
			"%s: reply carries cookie %d, expected %d",
			keepAliveName, resp.Cookie, c.cookie,
		)
	}
	c.cookie++
	c.replies.Add(1)
	if !c.stopped {
		c.timer = time.AfterFunc(c.period, c.ping)
	}
	return nil
}

// Stop cancels the next ping and stops the protocol.
func (c *keepAliveClient) Stop() {
	c.mu.Lock()
	c.stopped = true
	if c.timer != nil {
		c.timer.Stop()
	}
	c.mu.Unlock()
	c.Protocol.Stop()
}

// keepAliveServer answers the peer's pings.
type keepAliveServer struct {
	*protocol.Protocol
}

func newKeepAliveServer(opts protocol.ProtocolOptions) *keepAliveServer {
	s := &keepAliveServer{}
	s.Protocol = protocol.New(protocol.ProtocolConfig{
		Name:                keepAliveName,
		ProtocolId:          keepAliveProtocolID,
		Muxer:               opts.Muxer,
		Logger:              opts.Logger,
		ErrorChan:           opts.ErrorChan,
		Mode:                opts.Mode,
		Role:                protocol.ProtocolRoleServer,
		MessageHandlerFunc:  s.handle,
		MessageFromCborFunc: keepalive.NewMsgFromCbor,
		StateMap:            keepalive.StateMap,
		InitialState:        keepalive.StateClient,
	})
	return s
}

func (s *keepAliveServer) handle(msg protocol.Message) error {
	switch m := msg.(type) {
	case *keepalive.MsgKeepAlive:
		return s.SendMessage(keepalive.NewMsgKeepAliveResponse(m.Cookie))
	case *keepalive.MsgDone:
		return nil
	default:
		return fmt.Errorf(
			"%s: unexpected message type %d", keepAliveName, msg.Type(),
		)
	}
}
