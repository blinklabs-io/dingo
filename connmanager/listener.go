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
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// Accept loop backoff constants
const (
	acceptBackoffMin       = 10 * time.Millisecond // Initial backoff duration
	acceptBackoffMax       = 1 * time.Second       // Maximum backoff duration
	acceptBackoffCap       = 6                     // Max consecutive errors before capping (2^6 * 10ms = 640ms)
	unixSocketProbeTimeout = 250 * time.Millisecond
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
	for _, l := range c.listenerConfigList() {
		if err := c.startListener(ctx, l); err != nil {
			return err
		}
	}
	return nil
}

func prepareUnixSocketPath(path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("failed to check socket file %s: %w", path, err)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf(
			"listen address %s exists and is not a unix socket",
			path,
		)
	}

	conn, dialErr := net.DialTimeout("unix", path, unixSocketProbeTimeout)
	if dialErr == nil {
		_ = conn.Close()
		return fmt.Errorf(
			"listen address %s is already in use by a live unix socket",
			path,
		)
	}
	if errors.Is(dialErr, os.ErrNotExist) {
		// The path disappeared after Lstat. There is nothing left to remove;
		// the subsequent bind remains authoritative.
		return nil
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf(
			"failed to determine whether unix socket %s is stale: %w",
			path,
			dialErr,
		)
	}

	// Re-read non-following metadata after the failed connection attempt. A
	// replacement at the path must never inherit the stale verdict from the
	// socket that was probed.
	currentFi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("failed to recheck socket file %s: %w", path, err)
	}
	if currentFi.Mode()&os.ModeSocket == 0 || !os.SameFile(fi, currentFi) {
		return fmt.Errorf(
			"listen address %s changed while checking for a stale unix socket",
			path,
		)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf(
			"failed to remove existing socket file %s: %w",
			path,
			err,
		)
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
			// staticcheck sees only the non-Windows build, where
			// createPipeListener is a stub that always returns an error, so it
			// calls the comparison always true. It is a real check on Windows,
			// where the call can succeed. SA4023 is reported against the call
			// and the comparison separately, so both carry the directive.
			//nolint:staticcheck // SA4023: always true on the stub build only
			listener, err := createPipeListener(
				l.ListenNetwork,
				l.ListenAddress,
			)
			//nolint:staticcheck // SA4023: always true on the stub build only
			if err != nil {
				return fmt.Errorf("failed to open listening pipe: %w", err)
			}
			l.Listener = listener
		} else {
			// For Unix domain sockets, remove only a confirmed stale socket file
			// before binding.
			if l.ListenNetwork == "unix" {
				if err := prepareUnixSocketPath(l.ListenAddress); err != nil {
					return err
				}
			}
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
	c.goroutineWg.Go(func() {
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

			// Bound socket writes before handing the bearer to the
			// connection setup goroutine. This must cover NtC as well as
			// N2N: a local client that stops reading wedges a write just
			// like a remote peer does. Helpers below that need the concrete
			// socket type (SO_LINGER, Unix peer credentials) unwrap through
			// the wrapper.
			conn = withSocketDeadlines(conn)
			if !c.trackPendingConnection(conn) {
				continue
			}

			// NtC connections bypass the inbound slot budget and per-IP
			// limiting. Their handshake is still moved off the accept loop.
			if l.UseNtC {
				c.goroutineWg.Go(func() {
					c.setupAcceptedConnection(
						ctx,
						conn,
						l,
						defaultConnOpts,
						false,
					)
				})
				continue
			}

			// N2N path: when source-port reuse is in use, force RST
			// on close so the 4-tuple does not get stuck in TIME_WAIT
			// and block a subsequent outbound dial to the same peer
			// with EADDRNOTAVAIL on the matching local-listen-port
			// 4-tuple.
			if c.config.OutboundSourcePort > 0 {
				if lingerErr := enableTCPLingerZero(conn); lingerErr != nil {
					c.config.Logger.Warn(
						fmt.Sprintf(
							"listener: failed to enable SO_LINGER 0 on inbound connection from %s: %s",
							conn.RemoteAddr(),
							lingerErr,
						),
					)
				}
			}

			// N2N path: reserve an inbound slot before spawning setup. The
			// handshake is intentionally outside this accept loop: one silent
			// peer must not prevent the listener from accepting another peer.
			if !c.tryReserveInboundSlot() {
				c.config.Logger.Warn(
					fmt.Sprintf(
						"listener: inbound connection limit reached (%d), rejecting connection from %s",
						c.config.MaxInboundConns,
						conn.RemoteAddr(),
					),
				)
				_ = conn.Close()
				c.untrackPendingConnection(conn)
				continue
			}
			c.goroutineWg.Go(func() {
				c.setupAcceptedConnection(
					ctx,
					conn,
					l,
					defaultConnOpts,
					true,
				)
			})
		}
	})
	return nil
}

// trackPendingConnection records a bearer accepted before its handshake has
// completed. Stop closes these bearers as well as registered connections so a
// shutdown does not wait for the handshake deadline to expire.
func (c *ConnectionManager) trackPendingConnection(conn net.Conn) bool {
	c.listenersMutex.Lock()
	defer c.listenersMutex.Unlock()
	if c.closing {
		closeConnAndLog(
			c.config.Logger,
			conn,
			"listener: close connection accepted during shutdown failed",
		)
		return false
	}
	c.pendingConns[conn] = struct{}{}
	return true
}

func (c *ConnectionManager) untrackPendingConnection(conn net.Conn) {
	c.listenersMutex.Lock()
	delete(c.pendingConns, conn)
	c.listenersMutex.Unlock()
}

// setupAcceptedConnection performs the potentially blocking handshake outside
// the listener's accept loop. The absolute deadline is capped by
// handshakeDeadlineConn so the muxer's longer segment deadline cannot extend
// an unauthenticated connection's lifetime.
func (c *ConnectionManager) setupAcceptedConnection(
	ctx context.Context,
	conn net.Conn,
	l ListenerConfig,
	defaultConnOpts []ouroboros.ConnectionOptionFunc,
	inboundSlotReserved bool,
) {
	pendingConn := conn
	defer c.untrackPendingConnection(pendingConn)
	stopOnCancel := context.AfterFunc(ctx, func() {
		_ = pendingConn.Close()
	})
	defer func() {
		stopOnCancel()
	}()

	// Wrap UNIX connections before applying the handshake wrapper so peer
	// credentials and the unique Unix remote address remain available.
	if uConn, ok := conn.(*net.UnixConn); ok {
		tmpConn, err := NewUnixConn(uConn)
		if err != nil {
			c.config.Logger.Error("listener: accept failed", "error", err)
			closeConnAndLog(c.config.Logger, conn, "listener: close failed")
			if inboundSlotReserved {
				c.releaseInboundSlot()
			}
			return
		}
		conn = tmpConn
	}

	ipKey := ""
	if inboundSlotReserved {
		ipKey = ipKeyFromAddr(conn.RemoteAddr())
		if !c.acquireIPSlot(ipKey) {
			c.config.Logger.Warn(
				"listener: inbound connection rejected by per-IP limit",
				"remote_addr", conn.RemoteAddr(),
				"limit", c.config.MaxConnectionsPerIP,
			)
			closeConnAndLog(
				c.config.Logger,
				conn,
				"listener: close rejected connection failed",
			)
			c.releaseInboundSlot()
			return
		}
	}

	deadlineConn := withHandshakeDeadline(conn)
	if err := deadlineConn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		c.config.Logger.Info(
			"listener: failed to set handshake deadline",
			"error",
			err,
		)
		closeConnAndLog(c.config.Logger, conn, "listener: close failed")
		if ipKey != "" {
			c.releaseIPSlot(ipKey)
		}
		if inboundSlotReserved {
			c.releaseInboundSlot()
		}
		return
	}
	conn = deadlineConn

	connOpts := append(defaultConnOpts, ouroboros.WithConnection(conn))
	oConn, err := ouroboros.NewConnection(connOpts...)
	if err != nil {
		if l.UseNtC {
			c.config.Logger.Error(
				"listener: failed to setup NtC connection",
				"error",
				err,
			)
		} else {
			c.config.Logger.Info("listener: inbound connection failed", "error", err)
		}
		closeConnAndLog(c.config.Logger, conn, "listener: close failed")
		if ipKey != "" {
			c.releaseIPSlot(ipKey)
		}
		if inboundSlotReserved {
			c.releaseInboundSlot()
		}
		return
	}

	// The handshake is complete; return the bearer to normal protocol-managed
	// deadlines before registering it with the connection manager.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		c.config.Logger.Warn(
			"listener: failed to clear handshake deadline",
			"error",
			err,
		)
		closeConnAndLog(c.config.Logger, oConn, "listener: close failed")
		if ipKey != "" {
			c.releaseIPSlot(ipKey)
		}
		if inboundSlotReserved {
			c.releaseInboundSlot()
		}
		return
	}

	peerAddr := "unknown"
	if conn.RemoteAddr() != nil {
		peerAddr = conn.RemoteAddr().String()
	}
	if l.UseNtC {
		c.config.Logger.Info(
			"listener: accepted NtC connection",
			"remote_addr",
			peerAddr,
		)
		if !c.addNtCConnectionWithIPKey(oConn, true, peerAddr, "") {
			return
		}
	} else {
		c.config.Logger.Info("listener: inbound connection", "remote_addr", peerAddr)
		c.consumeInboundSlot()
		if !c.addConnectionWithIPKey(oConn, true, peerAddr, ipKey) {
			return
		}
	}

	if c.config.EventBus != nil {
		c.config.EventBus.Publish(
			InboundConnectionEventType,
			event.NewEvent(
				InboundConnectionEventType,
				InboundConnectionEvent{
					ConnectionId:         oConn.Id(),
					LocalAddr:            conn.LocalAddr(),
					RemoteAddr:           conn.RemoteAddr(),
					NormalizedRemoteAddr: NormalizePeerAddr(peerAddr),
					IsNtC:                l.UseNtC,
					IsDuplex:             connectionIsDuplex(oConn),
				},
			),
		)
	}
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
