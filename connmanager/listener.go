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

package connmanager

import (
	"context"
	"fmt"
	"net"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

type ListenerConfig struct {
	Listener       net.Listener
	ListenNetwork  string
	ListenAddress  string
	ConnectionOpts []ouroboros.ConnectionOptionFunc
	UseNtC         bool
	ReuseAddress   bool
}

func (c *ConnectionManager) startListeners() error {
	for _, l := range c.config.Listeners {
		if err := c.startListener(l); err != nil {
			return err
		}
	}
	return nil
}

func (c *ConnectionManager) startListener(l ListenerConfig) error {
	// Create listener if none is provided
	if l.Listener == nil {
		listenConfig := net.ListenConfig{}
		if l.ReuseAddress {
			listenConfig.Control = socketControl
		}
		listener, err := listenConfig.Listen(
			context.Background(),
			l.ListenNetwork,
			l.ListenAddress,
		)
		if err != nil {
			return fmt.Errorf("failed to open listening socket: %w", err)
		}
		l.Listener = listener
		if l.UseNtC {
			c.config.Logger.Info(
				"listening for ouroboros node-to-client connections on " + l.ListenAddress,
			)
		} else {
			c.config.Logger.Info(
				"listening for ouroboros node-to-node connections on " + l.ListenAddress,
			)
		}
	}
	// Build connection options
	defaultConnOpts := []ouroboros.ConnectionOptionFunc{
		ouroboros.WithLogger(c.config.Logger),
		ouroboros.WithNodeToNode(!l.UseNtC),
		ouroboros.WithServer(true),
	}
	defaultConnOpts = append(
		defaultConnOpts,
		l.ConnectionOpts...,
	)
	go func() {
		for {
			// Accept connection
			conn, err := l.Listener.Accept()
			if err != nil {
				c.config.Logger.Error(
					fmt.Sprintf("listener: accept failed: %s", err),
				)
				continue
			}
			// Wrap UNIX connections
			if uConn, ok := conn.(*net.UnixConn); ok {
				tmpConn, err := NewUnixConn(uConn)
				if err != nil {
					c.config.Logger.Error(
						fmt.Sprintf("listener: accept failed: %s", err),
					)
					_ = conn.Close()
					continue
				}
				conn = tmpConn
			}
			c.config.Logger.Info(
				fmt.Sprintf(
					"listener: accepted connection from %s",
					conn.RemoteAddr(),
				),
			)
			// Setup Ouroboros connection
			connOpts := append(
				defaultConnOpts,
				ouroboros.WithConnection(conn),
			)
			oConn, err := ouroboros.NewConnection(connOpts...)
			if err != nil {
				c.config.Logger.Error(
					fmt.Sprintf(
						"listener: failed to setup connection: %s",
						err,
					),
				)
				continue
			}
			// Add to connection manager
			c.AddConnection(oConn)
			// Generate event
			c.config.EventBus.Publish(
				InboundConnectionEventType,
				event.NewEvent(
					InboundConnectionEventType,
					InboundConnectionEvent{
						ConnectionId: oConn.Id(),
						LocalAddr:    conn.LocalAddr(),
						RemoteAddr:   conn.RemoteAddr(),
					},
				),
			)
		}
	}()
	return nil
}
