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
// it safely needs four pieces that only make sense together: a start is not
// invisible to a concurrent Stop (BeginStart), exactly one caller may detach
// and tear down a server (take), a Stop must not outrun a bind still in
// flight (bindDone), and the caller that loses the detach must not report the
// server down before the winner has finished (teardown).
//
// It lives here rather than in each API package because those pieces are
// subtle in the same way in all of them -- see awaitSignal's doc comment for
// the recheck that a second copy would be most likely to lose. Each server
// keeps only what genuinely differs: the http.Server it builds, and its
// ShutdownFunc. Everything else a server can get wrong about the lifecycle is
// behind Start's four calls and Stop's one, so there is nowhere for a server
// to grow a second mechanism of its own.
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
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/tlsutil"
)

// watchShutdownTimeout bounds the shutdown a context monitor runs. The monitor
// fires when the caller's context is already cancelled, so it cannot take that
// context's deadline and needs one of its own; without it a stuck handler
// would keep the monitor, and any Stop waiting on its teardown, running
// unboundedly.
const watchShutdownTimeout = 30 * time.Second

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
	// published on ln or closed again. Stop waits on it so a bind still in
	// flight cannot outlive the Stop that raced it.
	bindDone chan struct{}
	// gone is closed when the current server is detached, by whichever of take
	// and Unpublish gets there. It is what lets the context monitor Watch
	// launched exit at teardown, instead of sitting on a context that stays
	// live for the rest of the process.
	gone chan struct{}
	// teardown is closed once the caller that detached the server has finished
	// shutting it down. A server's Stop and the context monitor its Start
	// launched race to detach; the loser gets no server back and would
	// otherwise report the server down while the winner was still releasing
	// the port.
	teardown chan struct{}
	// startDone is non-nil while a Start is in flight, and is closed when that
	// Start returns. It is what makes a start visible to a Stop that arrives
	// before the server has been published: without it that Stop finds nothing
	// to detach and reports the server down, and the start it never saw goes
	// on to bind the port behind it.
	startDone chan struct{}
}

// New returns a Listener that names itself name in errors and logs. A nil
// logger discards.
func New(name string, logger *slog.Logger) *Listener {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &Listener{name: name, logger: logger}
}

// BeginStart marks a start as in flight and returns the channel the caller
// hands back to EndStart, which every Start does with a defer before it can
// fail. It reports an error if another start is already in flight.
//
// This is the first thing a Start does, before it builds anything, because the
// window it closes opens before publication: a Stop arriving while a start is
// between its first statement and Publish has no server to detach, so it
// returns reporting the server down, and the start it could not see then binds
// the port behind it. Stop waits that start out rather than racing it.
func (l *Listener) BeginStart() (chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.startDone != nil {
		return nil, errors.New(
			l.name + " server start already in progress",
		)
	}
	done := make(chan struct{})
	l.startDone = done
	return done, nil
}

// EndStart releases the start gate BeginStart took.
//
// Guarded on the identity of the channel, so a start that failed at BeginStart
// -- and therefore holds no gate -- cannot release the gate held by the start
// that beat it. Clearing the field before closing keeps a waiting Stop from
// ever observing a non-nil, already-closed gate and spinning on it.
func (l *Listener) EndStart(done chan struct{}) {
	if done == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.startDone == done {
		l.startDone = nil
		close(done)
	}
}

// Publish builds a server under the Listener's lock and registers it as the
// current one, returning it with the bind channel that must be handed to Bind.
// It reports an error if a server is already running, which is what makes a
// second Start fail rather than strand the first server's socket.
//
// build runs only once that check has passed, so a caller can construct its
// handler chain atomically with publication, and a rejected second Start
// cannot disturb the running server. build must not call back into the
// Listener: the lock is already held.
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
	l.srv, l.bindDone, l.gone = srv, bindDone, make(chan struct{})
	return srv, bindDone, nil
}

// Unpublish clears srv, for a Start that failed after publishing it.
//
// Guarded on purpose: an overlapping teardown or restart may already have
// detached or replaced this server, and clearing unconditionally would discard
// the newer one. Cleared as a set, matching take, so "no server present" never
// leaves a listener, bind channel, or unsignalled monitor behind for the next
// caller to find.
func (l *Listener) Unpublish(srv *http.Server) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.srv == srv {
		l.detachLocked()
	}
}

// Server returns the running server, or nil when none is published. It is for
// inspecting what was brought up; taking it down goes through Stop, which is
// what keeps a single caller responsible for the socket.
func (l *Listener) Server() *http.Server {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.srv
}

// job is everything one caller detaches from a Listener in order to tear it
// down, including the channel it must close when finished.
type job struct {
	srv      *http.Server
	ln       net.Listener
	bindDone chan struct{}
	done     chan struct{}
}

// detachLocked clears the current server and everything published with it,
// signalling that server's context monitor to exit. Callers hold l.mu.
func (l *Listener) detachLocked() {
	if l.gone != nil {
		close(l.gone)
	}
	l.srv, l.ln, l.bindDone, l.gone = nil, nil, nil, nil
}

// take detaches the running server and its listener so exactly one caller
// shuts them down: a server's Stop and the context monitor its Start launched
// both race for them. A non-nil match restricts the detach to that server.
//
// The winner gets a job and owns closing job.done, via shutdown. The loser
// gets a nil job and the winner's completion channel, which it must wait on
// with awaitTeardown -- returning early would report the server down while the
// port was still bound, and an immediate restart on the same port would then
// fail to bind.
//
// A match is what a context monitor must pass. A monitor outlives the server
// it was launched for, so an unconditional detach there could tear down a
// replacement it never published. A caller whose named server is already gone
// gets a nil job and nothing to wait on: its own server is down either way,
// and a teardown in flight belongs to whoever detached it.
func (l *Listener) take(match *http.Server) (*job, chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// The identity check comes first, and deliberately also covers l.srv being
	// nil. A caller that named a server and did not get it has nothing of its
	// own left either way -- already detached, or replaced by a later Start --
	// and the teardown that may be in flight belongs to whoever detached it.
	// Falling through to the nil-server branch below would hand a monitor that
	// unrelated teardown to block on.
	if match != nil && l.srv != match {
		return nil, nil
	}
	if l.srv == nil {
		// Either never started, or someone else is already tearing it down.
		// Only an unmatched take reaches this, and it is the loser of a
		// genuine race, so the winner's teardown is exactly what it must wait
		// on.
		return nil, l.teardown
	}
	j := &job{
		srv:      l.srv,
		ln:       l.ln,
		bindDone: l.bindDone,
		done:     make(chan struct{}),
	}
	l.detachLocked()
	l.teardown = j.done
	return j, nil
}

// ShutdownFunc drains a detached server's in-flight requests. It must return
// only once the server is done with the listeners Serve registered; closing
// the socket this package recorded is the protocol's job, not its own.
//
// It exists so a server whose graceful shutdown needs more than
// http.Server.Shutdown can supply it -- api/utxorpc escalates to a hard Close,
// because an unbounded streaming RPC can otherwise keep Shutdown blocked
// indefinitely. Errors are returned unwrapped; the protocol adds the context.
type ShutdownFunc func(ctx context.Context, srv *http.Server) error

// Graceful is the default ShutdownFunc: http.Server.Shutdown bounded by ctx.
func Graceful(ctx context.Context, srv *http.Server) error {
	return srv.Shutdown(ctx)
}

// Stop tears the running server down and does not return until its listening
// socket has been released, so a restart on the same port can rebind.
//
// It is the whole of a server's Stop: waiting out a start still in flight,
// detaching, and then either running the teardown or waiting on the one it
// lost. Every wait is bounded by ctx, so the caller's shutdown deadline bounds
// this call whatever the server is doing, except for whatever bound a
// ShutdownFunc chooses for itself.
func (l *Listener) Stop(ctx context.Context, fn ShutdownFunc) error {
	for {
		l.mu.Lock()
		startDone := l.startDone
		l.mu.Unlock()
		if startDone != nil {
			// A start is in flight and may not have published its server yet.
			// Detaching now would find nothing, report the server down, and
			// leave that start to bind the port afterwards.
			if err := awaitSignal(
				ctx, startDone, "an in-flight "+l.name+" start",
			); err != nil {
				return err
			}
			// EndStart clears the field before closing the channel, so the
			// next pass sees either no start or a genuinely newer one. It
			// cannot observe this one again, which is what keeps the loop from
			// spinning.
			continue
		}
		j, inFlight := l.take(nil)
		if j == nil {
			return l.awaitTeardown(ctx, inFlight)
		}
		l.logger.Debug("shutting down " + l.name + " server")
		return l.shutdown(ctx, j, fn)
	}
}

// shutdown runs one detached job to completion and reports what went wrong.
//
// The bind wait is not allowed to skip the teardown: a caller whose context
// expires mid-wait still holds the only reference to a bound socket, so
// returning early would leave the port bound with nothing left to close it.
func (l *Listener) shutdown(
	ctx context.Context,
	j *job,
	fn ShutdownFunc,
) error {
	waitErr := l.awaitBind(ctx, j.bindDone)
	stopErr := l.shutdownServer(ctx, j.srv, j.ln, fn)
	if waitErr == nil {
		close(j.done)
		return stopErr
	}
	// The bind is still in flight, so Bind still owns a socket this call cannot
	// close. Closing j.done now would let a waiting Stop report the server
	// down while that socket was still bound. Bind always closes bindDone on
	// its way out -- and closes its own listener once it sees the detach -- so
	// hand the signalling off until then, which also bounds this goroutine.
	go func() {
		<-j.bindDone
		close(j.done)
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

// awaitTeardown waits for another caller's in-flight shutdown to finish. It is
// what the loser of take must do before reporting the server down.
func (l *Listener) awaitTeardown(
	ctx context.Context,
	done chan struct{},
) error {
	return awaitSignal(
		ctx, done, "an in-flight "+l.name+" shutdown",
	)
}

// awaitBind waits for an in-flight Bind to finish releasing or publishing its
// socket. Detaching the server first (take) is what makes that bind close its
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

// Watch runs the context monitor for the server Start has just published: when
// ctx ends, the server comes down whatever its own Stop is doing. It returns a
// channel closed once the monitor has exited, which is what a test asserts on;
// production callers ignore it.
//
// The monitor also watches for the server being detached, and exits then. The
// context it is given outlives the server by design -- every production caller
// passes the node context, which stays live across a capability restart and
// for the rest of the process -- so a monitor waiting only on ctx.Done() sits
// there holding the whole stopped server, its handler chain, and through that
// the database those handlers were built over, until the node itself shuts
// down. There is one monitor per Start, and every live Restore or Truncate
// performs another Start.
func (l *Listener) Watch(
	ctx context.Context,
	srv *http.Server,
	fn ShutdownFunc,
) <-chan struct{} {
	exited := make(chan struct{})
	l.mu.Lock()
	gone := l.gone
	current := l.srv == srv
	l.mu.Unlock()
	if !current {
		// Detached between Publish and here, so there is nothing left to
		// monitor and whoever detached it owns the teardown.
		close(exited)
		return exited
	}
	go func() { //nolint:gosec // G118: the shutdown below intentionally outlives ctx
		defer close(exited)
		select {
		case <-gone:
			// Detached by a Stop, by a restart, or by a Start unpublishing
			// after a failed bind: that caller owns the teardown.
			return
		case <-ctx.Done():
		}
		j, _ := l.take(srv)
		// Nil when a concurrent Stop won the detach -- it owns the teardown
		// and its caller is already waiting on it -- or when this server was
		// already stopped and a restart published another one, which is not
		// this monitor's to touch. Either way there is nothing to do here.
		if j == nil {
			return
		}
		l.logger.Debug(
			"context cancelled, shutting down " + l.name + " server",
		)
		//nolint:contextcheck // ctx is already done; the drain needs its own
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(), watchShutdownTimeout,
		)
		defer cancel()
		//nolint:contextcheck // see above
		if err := l.shutdown(shutdownCtx, j, fn); err != nil {
			l.logger.Error(
				"failed to shutdown "+l.name+
					" server on context cancellation",
				"error", err,
			)
		}
	}()
	return exited
}

// Bind opens srv's listening socket and serves it in the background, closing
// bindDone once the socket has been either published or closed again. It
// reports whether it handed the socket to Serve: false means srv was detached
// before the socket could be published, so Bind closed it instead, and the
// caller must not report that a listener came up.
//
// True is a statement about what this call did, not a promise that the listener
// is still up: a teardown can detach and close the socket between the ownership
// check below and Serve being entered, leaving Serve nothing to accept. That
// window is inert -- Serve reports ErrServerClosed, which the goroutine's error
// filter drops, and the port is released by the teardown that closed it -- so
// its only trace is the caller having logged that the listener came up. Closing
// it would need Serve to signal that it registered the listener, which net/http
// does not expose, and any such signal would still lose to a teardown landing
// an instant after it. See TestServeEnteredAfterShutdownStaysQuiet.
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
	// or closing our own socket after losing the race to a teardown -- so a
	// waiting Stop is never left hanging on a bind that already finished.
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
	// Recorded so the teardown can close the socket itself rather than relying
	// on the Serve goroutine below having registered it, but only while this
	// call's server is still the current one. A context monitor can detach it
	// between Publish and this point; a bare assignment would then strand a
	// bound socket no later Stop can reach, because take hands back a nil
	// server and shutdownServer never runs. The same guard stops an
	// overlapping restart from overwriting the newer server's listener with
	// this one.
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
