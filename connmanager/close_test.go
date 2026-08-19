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
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// fakeCloser stands in for a real *ouroboros.Connection, whose Close()
// unconditionally returns nil -- there is no way to drive a genuine close
// failure through the real type, so closeConnAndLog and joinCloseErr are
// tested against this controllable io.Closer instead.
type fakeCloser struct {
	err error
}

func (f *fakeCloser) Close() error {
	return f.err
}

// A close failure must still surface: the message, the underlying error, and
// any caller-supplied attributes (e.g. the peer address) all need to make it
// into the log line, or a real cleanup failure would be undiagnosable.
func TestCloseConnAndLogLogsOnCloseError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(
		slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)
	closeErr := errors.New("connection reset")

	closeConnAndLog(
		logger,
		&fakeCloser{err: closeErr},
		"error closing connection",
		"peer_addr", "10.0.0.1:3001",
	)

	out := buf.String()
	if !strings.Contains(out, "error closing connection") {
		t.Fatalf("expected log message in output, got: %s", out)
	}
	if !strings.Contains(out, "connection reset") {
		t.Fatalf("expected close error in output, got: %s", out)
	}
	if !strings.Contains(out, "10.0.0.1:3001") {
		t.Fatalf("expected caller-supplied attrs in output, got: %s", out)
	}
}

// The common case -- Close() succeeds -- must stay silent. Logging on every
// routine connection teardown would bury the genuine failures this helper
// exists to surface.
func TestCloseConnAndLogStaysQuietOnSuccess(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(
		slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}),
	)

	closeConnAndLog(
		logger,
		&fakeCloser{err: nil},
		"error closing connection",
		"peer_addr", "10.0.0.1:3001",
	)

	if buf.Len() != 0 {
		t.Fatalf(
			"expected no log output on successful close, got: %s",
			buf.String(),
		)
	}
}

// Several callers pass a *ouroboros.Connection straight from
// GetConnectionById, which returns nil for an unknown connection ID.
// closeConnAndLog must treat a nil closer as a no-op rather than panic.
func TestCloseConnAndLogHandlesNilCloser(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	closeConnAndLog(logger, nil, "error closing connection")

	if buf.Len() != 0 {
		t.Fatalf(
			"expected no log output for a nil closer, got: %s",
			buf.String(),
		)
	}
}

// When the original operation already failed and the connection also fails
// to close, both errors matter: the close failure can be the only sign that
// the socket was never actually released. Neither may be dropped for the
// other.
func TestJoinCloseErrJoinsOnCloseFailure(t *testing.T) {
	origErr := errors.New("dial failed")
	closeErr := errors.New("already closed")

	got := joinCloseErr(origErr, &fakeCloser{err: closeErr})

	if !errors.Is(got, origErr) {
		t.Fatalf("expected joined error to wrap original error: %v", got)
	}
	if !errors.Is(got, closeErr) {
		t.Fatalf("expected joined error to wrap close error: %v", got)
	}
}

// A clean close must not manufacture a joined error out of nothing -- the
// caller's original error should come back unwrapped.
func TestJoinCloseErrReturnsOriginalOnCloseSuccess(t *testing.T) {
	origErr := errors.New("dial failed")

	got := joinCloseErr(origErr, &fakeCloser{err: nil})

	if !errors.Is(got, origErr) {
		t.Fatalf("expected original error, got: %v", got)
	}
	if errors.Unwrap(got) != nil {
		t.Fatalf("expected no joined error when close succeeds, got: %v", got)
	}
}
