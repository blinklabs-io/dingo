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
	"errors"
	"fmt"
	"net"
	"runtime"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// Accept loop backoff constants
const (
	acceptBackoffMin = 10 * time.Millisecond // Initial backoff duration
	acceptBackoffMax = 1 * time.Second       // Maximum backoff duration
	acceptBackoffCap = 6                     // Max consecutive errors before capping (2^6 * 10ms = 640ms)
)

type ListenerConfig struct {
	Listener       net.Listener
	ListenNetwork  string
	ListenAddress  string
	ConnectionOpts []ouroboros.ConnectionOptionFunc
	UseNtC         bool
	ReuseAddress   bool
}

func (c *ConnectionManager) startListeners(ctx context.Context) error {
	for _, l := range c.config.Listeners {
		if err := c.startListener(ctx, l); err != nil {
			return err
		}
	}
	return nil
}

func (c *ConnectionManager) startListener(
	ctx context.Context,
	l ListenerConfig,
) error {
	// Create listener if none is provided
	if l.Listener == nil {
		// On Windows, the "unix" network type is repurposed to create named pipes
		// for compatibility with configurations that specify "unix" network on Unix systems.
		if runtime.GOOS == "windows" && l.ListenNetwork == "unix" {
			listener, err := createPipeListener(
				l.ListenNetwork,
				l.ListenAddress,
			)
			if err != nil {
				return fmt.Errorf("failed to open listening pipe: %w", err)
			}
			l.Listener = listener
		} else {
			listenConfig := net.ListenConfig{}
			if l.ReuseAddress {
				listenConfig.Control = socketControl
			}
			listener, err := listenConfig.Listen(
				ctx,
				l.ListenNetwork,
				l.ListenAddress,
			)
			if err != nil {
				return fmt.Errorf("failed to open listening socket: %w", err)
			}
			l.Listener = listener
		}
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
	// Track listener for shutdown
	c.listenersMutex.Lock()
	c.listeners = append(c.listeners, l.Listener)
	c.listenersMutex.Unlock()

	// Build connection options
	defaultConnOpts := make(
		[]ouroboros.ConnectionOptionFunc,
		0,
		3+len(l.ConnectionOpts),
	)
	defaultConnOpts = append(defaultConnOpts,
		ouroboros.WithLogger(c.config.Logger),
		ouroboros.WithNodeToNode(!l.UseNtC),
		ouroboros.WithServer(true),
	)
	defaultConnOpts = append(
		defaultConnOpts,
		l.ConnectionOpts...,
	)
	c.goroutineWg.Add(1)
	go func() {
		defer c.goroutineWg.Done()
		var consecutiveErrors int
		for {
			// Accept connection
			conn, err := l.Listener.Accept()
			if err != nil {
				// During shutdown, closing the listener will cause Accept to return
				// a net.ErrClosed. Treat this as a normal termination and exit the loop
				if errors.Is(err, net.ErrClosed) {
					c.config.Logger.Debug(
						"listener: closed, stopping accept loop",
					)
					return
				}
				// If we're closing, exit quietly
				c.listenersMutex.Lock()
				isClosing := c.closing
				c.listenersMutex.Unlock()
				if isClosing {
					c.config.Logger.Debug(
						"listener: shutting down, stopping accept loop",
					)
					return
				}
				// Some platforms may return timeout errors; handle and continue
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					c.config.Logger.Warn(
						fmt.Sprintf("listener: accept timeout: %s", err),
					)
					continue
				}
				// Otherwise, log at error level and apply exponential backoff
				c.config.Logger.Error(
					fmt.Sprintf("listener: accept failed: %s", err),
				)
				// Calculate backoff with exponential increase
				consecutiveErrors++
				backoff := c.calculateAcceptBackoff(consecutiveErrors)
				c.config.Logger.Debug(
					fmt.Sprintf(
						"listener: backing off for %v after %d consecutive errors",
						backoff,
						consecutiveErrors,
					),
				)
				// Backoff with cancellation awareness
				timer := time.NewTimer(backoff)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return
				}
				continue
			}
			// Successful accept - reset consecutive error count
			consecutiveErrors = 0
			// Atomically check and reserve an inbound slot before any further processing
			if !c.tryReserveInboundSlot() {
				c.config.Logger.Warn(
					fmt.Sprintf(
						"listener: inbound connection limit reached (%d), rejecting connection from %s",
						c.config.MaxInboundConns,
						conn.RemoteAddr(),
					),
				)
				conn.Close()
				continue
			}
			// From here on, we hold a reserved inbound slot.
			// If setup fails, we must release it.

			// Wrap UNIX connections
			if uConn, ok := conn.(*net.UnixConn); ok {
				tmpConn, err := NewUnixConn(uConn)
				if err != nil {
					c.config.Logger.Error(
						fmt.Sprintf("listener: accept failed: %s", err),
					)
					_ = conn.Close()
					c.releaseInboundSlot()
					continue
				}
				conn = tmpConn
			}
			// Per-IP rate limiting: reject if this IP has too many
			// connections already
			ipKey := ipKeyFromAddr(conn.RemoteAddr())
			if !c.acquireIPSlot(ipKey) {
				c.config.Logger.Warn(
					fmt.Sprintf(
						"listener: rejected connection from %s: per-IP limit (%d) reached",
						conn.RemoteAddr(),
						c.config.MaxConnectionsPerIP,
					),
				)
				conn.Close()
				c.releaseInboundSlot()
				continue
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
				// Release the IP slot since the connection failed
				c.releaseIPSlot(ipKey)
				conn.Close()
				c.releaseInboundSlot()
				continue
			}
			// Consume the reserved slot and add to connection manager.
			// The reservation is released because AddConnection will
			// add the actual connection entry to the map.
			c.consumeInboundSlot()
			peerAddr := "unknown"
			if conn.RemoteAddr() != nil {
				peerAddr = conn.RemoteAddr().String()
			}
			c.addConnectionWithIPKey(oConn, true, peerAddr, ipKey)
			// Generate event
			if c.config.EventBus != nil {
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
		}
	}()
	return nil
}

// calculateAcceptBackoff computes an exponential backoff duration based on
// the number of consecutive Accept() errors. The backoff starts at
// acceptBackoffMin and doubles with each subsequent error up to acceptBackoffMax.
func (c *ConnectionManager) calculateAcceptBackoff(
	consecutiveErrors int,
) time.Duration {
	if consecutiveErrors <= 0 {
		return acceptBackoffMin
	}
	// Cap the exponent to avoid overflow and limit max backoff
	// Use (consecutiveErrors-1) so first error yields acceptBackoffMin
	exponent := min(consecutiveErrors-1, acceptBackoffCap)
	// Calculate backoff: min * 2^exponent
	backoff := min(acceptBackoffMin<<exponent, acceptBackoffMax)
	return backoff
}
