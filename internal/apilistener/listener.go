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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package apilistener owns the start/stop protocol shared by Dingo's HTTP API
// servers (api/blockfrost, api/mesh, api/utxorpc).
//
// The protocol exists because http.Server.Shutdown only closes listeners that
// Serve has already registered, and each server binds its socket before
// handing it to Serve in a goroutine it does not wait for. A Stop landing in
// that window returns with the port still bound, which the capability restart
// in node_lifecycle.go -- reached by any live database restore or truncate --
// then fails to rebind with EADDRINUSE.
//
// Releasing the port is therefore something Stop has to do itself, and doing
// it safely needs three pieces that only make sense together: exactly one
// caller may detach and tear down a server (Take), a Stop must not outrun a
// bind still in flight (bindDone), and the caller that loses the detach must
// not report the server down before the winner has finished (teardown).
//
// It lives here rather than in each API package because those pieces are
// subtle in the same way in all three -- see awaitSignal's doc comment for the
// recheck that a second copy would be most likely to lose.
package apilistener

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/tlsutil"
)

// Listener holds the lifecycle state of one API server: the running
// http.Server, the socket it is bound to, and the channels the shutdown
// protocol coordinates on.
//
// A Listener is reusable. After a completed shutdown it holds no server, so
// the same instance can bring another one up on the same address -- which is
// exactly what a capability restart does.
type Listener struct {
	// name identifies the server in errors and logs, e.g. "Mesh API". It is
	// the subject of every message, so it reads as a noun phrase that "server"
	// can follow.
	name   string
	logger *slog.Logger

	mu  sync.Mutex
	srv *http.Server
	ln  net.Listener
	// bindDone is closed by Bind once the listening socket has been either
	// published on ln or closed again. Shutdown waits on it so a bind still in
	// flight cannot outlive the Stop that raced it.
	bindDone chan struct{}
	// teardown is closed once the caller that detached the server has finished
	// shutting it down. A server's Stop and the context monitor its Start
	// launched race to detach; the loser gets no server back and would
	// otherwise report the server down while the winner was still releasing
	// the port.
	teardown chan struct{}
}

// New returns a Listener that names itself name in errors and logs. A nil
// logger discards.
func New(name string, logger *slog.Logger) *Listener {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &Listener{name: name, logger: logger}
}

// Publish builds a server under the Listener's lock and registers it as the
// current one, returning it with the bind channel that must be handed to Bind.
// It reports an error if a server is already running, which is what makes a
// second Start fail rather than strand the first server's socket.
//
// build runs only once that check has passed, so a caller can initialize the
// state its handler chain reads -- its credential verifier, typically --
// atomically with publication, and a rejected second Start cannot disturb the
// running server's. build must not call back into the Listener: the lock is
// already held.
func (l *Listener) Publish(
	build func() *http.Server,
) (*http.Server, chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.srv != nil {
		return nil, nil, errors.New("server already started")
	}
	srv := build()
	bindDone := make(chan struct{})
	l.srv, l.bindDone = srv, bindDone
	return srv, bindDone, nil
}

// Unpublish clears srv, for a Start that failed after publishing it.
//
// Guarded on purpose: an overlapping Stop or restart may already have detached
// or replaced this server, and clearing unconditionally would discard the newer
// one. Cleared as a set, matching Take, so "no server present" never leaves a
// listener or bind channel behind for the next caller to find.
func (l *Listener) Unpublish(srv *http.Server) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.srv == srv {
		l.srv, l.ln, l.bindDone = nil, nil, nil
	}
}

// Server returns the running server, or nil when none is published. It is for
// inspecting what was brought up; taking it down goes through Take, which is
// what keeps a single caller responsible for the socket.
func (l *Listener) Server() *http.Server {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.srv
}

// Job is everything one caller detaches from a Listener in order to tear it
// down, including the channel it must close when finished.
type Job struct {
	srv      *http.Server
	ln       net.Listener
	bindDone chan struct{}
	done     chan struct{}
}

// Take detaches the running server and its listener so exactly one caller
// shuts them down: a server's Stop and the context monitor its Start launched
// both race for them.
//
// The winner gets a job and owns closing job.done, via Shutdown. The loser gets
// a nil job and the winner's completion channel, which it must wait on with
// AwaitTeardown -- returning early would report the server down while the port
// was still bound, and an immediate restart on the same port would then fail to
// bind.
func (l *Listener) Take() (*Job, chan struct{}) {
	return l.take(nil)
}

// TakeIf is Take restricted to srv: it detaches only while srv is still the
// published server.
//
// This is what a context monitor must use. A monitor outlives the server it was
// launched for -- it sits on ctx.Done() until its caller's context ends, which
// can be long after that server was stopped and a restart published another one
// on the same Listener. An unconditional Take there would tear down a
// replacement the monitor never published.
//
// A caller whose server is already gone gets a nil job and nothing to wait on:
// its own server is down either way, and a teardown in flight belongs to
// whoever detached it.
func (l *Listener) TakeIf(srv *http.Server) (*Job, chan struct{}) {
	return l.take(srv)
}

// take detaches the current server, optionally only when it is match.
func (l *Listener) take(match *http.Server) (*Job, chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.srv == nil {
		// Either never started, or someone else is already tearing it down.
		return nil, l.teardown
	}
	if match != nil && l.srv != match {
		return nil, nil
	}
	job := &Job{
		srv:      l.srv,
		ln:       l.ln,
		bindDone: l.bindDone,
		done:     make(chan struct{}),
	}
	l.srv, l.ln, l.bindDone = nil, nil, nil
	l.teardown = job.done
	return job, nil
}

// ShutdownFunc drains a detached server's in-flight requests. It must return
// only once the server is done with the listeners Serve registered; closing
// the socket this package recorded is Shutdown's job, not its own.
//
// It exists so a server whose graceful shutdown needs more than
// http.Server.Shutdown can supply it -- api/utxorpc escalates to a hard Close,
// because an unbounded streaming RPC can otherwise keep Shutdown blocked
// indefinitely. Errors are returned unwrapped; Shutdown adds the context.
type ShutdownFunc func(ctx context.Context, srv *http.Server) error

// Graceful is the default ShutdownFunc: http.Server.Shutdown bounded by ctx.
func Graceful(ctx context.Context, srv *http.Server) error {
	return srv.Shutdown(ctx)
}

// Shutdown runs one detached job to completion and reports what went wrong.
//
// The bind wait is not allowed to skip the teardown: a caller whose context
// expires mid-wait still holds the only reference to a bound socket, so
// returning early would leave the port bound with nothing left to close it.
func (l *Listener) Shutdown(
	ctx context.Context,
	job *Job,
	fn ShutdownFunc,
) error {
	waitErr := l.awaitBind(ctx, job.bindDone)
	stopErr := l.shutdownServer(ctx, job.srv, job.ln, fn)
	if waitErr == nil {
		close(job.done)
		return stopErr
	}
	// The bind is still in flight, so Bind still owns a socket this call cannot
	// close. Closing job.done now would let a waiting Stop report the server
	// down while that socket was still bound. Bind always closes bindDone on
	// its way out -- and closes its own listener once it sees the detach -- so
	// hand the signalling off until then, which also bounds this goroutine.
	go func() {
		<-job.bindDone
		close(job.done)
	}()
	return errors.Join(waitErr, stopErr)
}

// shutdownServer drains in-flight requests, then closes the listening socket.
// Closing after the drain, not before, keeps Serve's exit quiet: Shutdown marks
// the server as shutting down first, so the resulting accept failure surfaces
// as http.ErrServerClosed rather than an error log.
func (l *Listener) shutdownServer(
	ctx context.Context,
	srv *http.Server,
	ln net.Listener,
	fn ShutdownFunc,
) error {
	err := fn(ctx, srv)
	if ln != nil {
		// Serve closes the listener on its own way out, so an already-closed
		// listener is the expected case, not a failure.
		if closeErr := ln.Close(); closeErr != nil &&
			!errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}
	if err != nil {
		return fmt.Errorf(
			"failed to shutdown %s server: %w", l.name, err,
		)
	}
	return nil
}

// AwaitTeardown waits for another caller's in-flight shutdown to finish. It is
// what the loser of Take must call before reporting the server down.
func (l *Listener) AwaitTeardown(
	ctx context.Context,
	done chan struct{},
) error {
	return awaitSignal(
		ctx, done, "an in-flight "+l.name+" shutdown",
	)
}

// awaitBind waits for an in-flight Bind to finish releasing or publishing its
// socket. Detaching the server first (Take) is what makes that bind close its
// own listener, so waiting here is what lets Stop promise the port is free by
// the time it returns rather than merely started closing.
func (l *Listener) awaitBind(
	ctx context.Context,
	bindDone chan struct{},
) error {
	return awaitSignal(
		ctx, bindDone, "the "+l.name+" listener bind to settle",
	)
}

// awaitSignal waits for ch to close, bounded by ctx. A nil channel has nothing
// to wait for and succeeds immediately.
//
// One implementation on purpose. Every caller here needs the same recheck: when
// ch closes at the same moment ctx expires, select picks at random, and
// reporting a timeout for work that actually finished turns a clean shutdown
// into a spurious error -- and, for the bind, defers the teardown signal that
// another caller is blocked on. Written once, the recheck cannot be present in
// one copy and missing from the next.
func awaitSignal(ctx context.Context, ch chan struct{}, what string) error {
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		select {
		case <-ch:
			return nil
		default:
		}
		return fmt.Errorf(
			"timed out waiting for %s: %w", what, ctx.Err(),
		)
	}
}

// Bind opens srv's listening socket and serves it in the background, closing
// bindDone once the socket has been either published or closed again. It
// reports whether the socket is actually being served: false means srv was
// detached before the socket could be published, so Bind closed it instead, and
// the caller must not report that a listener came up.
//
// The socket is opened synchronously so a port conflict -- and, when TLS is
// enabled, a bad keypair -- surfaces as an error from Start rather than in a
// log line from a goroutine nobody is watching.
func (l *Listener) Bind(
	srv *http.Server,
	bindDone chan struct{},
	tls apiconfig.EffectiveTLS,
) (bool, error) {
	// Closed on every exit path -- keypair failure, bind failure, publication,
	// or closing our own socket after losing the race to Stop -- so a waiting
	// Stop is never left hanging on a bind that already finished.
	defer close(bindDone)
	useTLS := tls.Enabled
	if useTLS {
		if err := tlsutil.ConfigureServerTLS(
			srv, tls.CertFilePath, tls.KeyFilePath,
		); err != nil {
			return false, fmt.Errorf(
				"failed to load TLS keypair for %s server: %w",
				l.name, err,
			)
		}
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return false, fmt.Errorf(
			"failed to listen for %s server: %w", l.name, err,
		)
	}
	// Recorded so Shutdown can close the socket itself rather than relying on
	// the Serve goroutine below having registered it, but only while this
	// call's server is still the current one. Stop can detach it between
	// Publish and this point; a bare assignment would then strand a bound
	// socket no later Stop can reach, because Take hands back a nil server and
	// shutdownServer never runs. The same guard stops an overlapping restart
	// from overwriting the newer server's listener with this one.
	l.mu.Lock()
	current := l.srv == srv
	if current {
		l.ln = ln
	}
	l.mu.Unlock()
	if !current {
		// Already stopped or replaced: close our own socket instead of leaving
		// it bound, and never hand it to Serve. Reported by log rather than
		// returned, because Start's error path unpublishes and would clobber
		// the newer server here.
		if closeErr := ln.Close(); closeErr != nil &&
			!errors.Is(closeErr, net.ErrClosed) {
			l.logger.Error(
				"failed to close the listener of a stopped "+
					l.name+" server",
				"error", closeErr,
			)
		}
		return false, nil
	}
	go func() {
		var serveErr error
		if useTLS {
			serveErr = srv.ServeTLS(ln, "", "")
		} else {
			serveErr = srv.Serve(ln)
		}
		if serveErr != nil &&
			!errors.Is(serveErr, http.ErrServerClosed) {
			l.logger.Error(
				l.name+" server error", "error", serveErr,
			)
		}
	}()
	return true, nil
}
