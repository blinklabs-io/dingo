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
	"io"
	"log/slog"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// ouroborosCloseTimeout bounds how long closing an Ouroboros connection
// waits for its mini-protocol and muxer goroutines. A callback that does not
// return holds Close until it does; teardown still proceeds after the bound.
const ouroborosCloseTimeout = 5 * time.Second

// CloseOuroborosConn closes conn and waits, for at most ouroborosCloseTimeout
// or until ctx is done, until no callback of conn is still running. Code
// running inside one of conn's mini-protocol callbacks must not use it with a
// context that is not that callback's CallbackContext Context, or Close waits
// for the goroutine that is running it; use BeginOuroborosClose there.
func CloseOuroborosConn(ctx context.Context, conn *ouroboros.Connection) error {
	if conn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, ouroborosCloseTimeout)
	defer cancel()
	return conn.Close(ctx)
}

// BeginOuroborosClose starts conn's shutdown and returns without waiting for
// its callbacks: Close signals shutdown before it waits, and returns at once
// when its context is already done. Use it where the caller may be a callback
// of conn, or holds a lock a callback of conn can be waiting for.
func BeginOuroborosClose(conn *ouroboros.Connection) {
	if conn == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = conn.Close(ctx)
}

// OuroborosCloser adapts an Ouroboros connection to io.Closer for helpers
// that close an io.Closer. By default Close waits like CloseOuroborosConn
// with a background context; with Begin set it only starts the shutdown, as
// BeginOuroborosClose does.
type OuroborosCloser struct {
	Conn  *ouroboros.Connection
	Begin bool
}

// Close closes the connection as described on OuroborosCloser.
func (c OuroborosCloser) Close() error {
	if c.Begin {
		BeginOuroborosClose(c.Conn)
		return nil
	}
	return CloseOuroborosConn(context.Background(), c.Conn)
}

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

// joinCloseErr closes closer and folds any close error into err, so a
// connection that fails to close on an already-failing setup path isn't
// silently lost.
func joinCloseErr(err error, closer io.Closer) error {
	if closeErr := closer.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}
