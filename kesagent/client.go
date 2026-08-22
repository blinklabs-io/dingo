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

package kesagent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/prometheus/client_golang/prometheus"
)

// Errors returned by the client.
var (
	// ErrNoKeyYet is returned by KESSign in serve-key mode before the agent
	// has pushed a key (e.g. immediately after startup, or while the agent
	// is unreachable and no key was ever received).
	ErrNoKeyYet = errors.New("kesagent: no KES key received from agent yet")
	// ErrExhausted is returned when the requested period is beyond the
	// key's remaining evolutions.
	ErrExhausted = errors.New(
		"kesagent: KES key exhausted for requested period",
	)
	// ErrPastPeriod is returned when a period below the key's current period
	// is requested (KES keys only evolve forward).
	ErrPastPeriod = errors.New("kesagent: requested KES period is in the past")
	// ErrNotStarted is returned by sign-mode KESSign before Start.
	ErrNotStarted = errors.New("kesagent: client not started")
	// ErrClosed is returned when KESSign is called after Close.
	ErrClosed = errors.New("kesagent: client is closed")
	// ErrInvalidSignature is returned when the agent's signature fails KES
	// verification.
	ErrInvalidSignature = errors.New(
		"kesagent: agent returned an invalid signature",
	)
	// ErrNotReady is returned by WaitForReady when the client could not reach
	// a state where it can produce a signature within the given timeout.
	ErrNotReady = errors.New("kesagent: KES agent not ready")
	// ErrAgentSign is returned when the agent answered a sign request with an
	// error of its own (an exhausted key, a refused period). It is a reply, not
	// a transport failure, so the connection stays up and the request is not
	// retried.
	ErrAgentSign = errors.New("kesagent: agent sign error")
)

const (
	defaultMinReconnect = 500 * time.Millisecond
	defaultMaxReconnect = 5 * time.Second
	// defaultSignTimeout must stay below one slot. A mainnet slot is one
	// second, checkAndForgeProduction runs synchronously on the slot-aligned
	// forging loop and ignores its context, so a sign timeout above a slot
	// parks block production for several slots when the agent stops
	// answering. A unix-socket round trip to a healthy agent is sub-
	// millisecond, so this is generous; SignTimeout overrides it.
	defaultSignTimeout = 500 * time.Millisecond
	// defaultHandshakeTimeout bounds the Hello exchange so an agent that
	// accepts the socket without speaking cannot wedge the client.
	defaultHandshakeTimeout = 5 * time.Second
	// connectWarnRepeat is how many consecutive failed connection attempts
	// pass between Warn records. The first failure always warns; the rest stay
	// at Debug so a long agent outage does not flood the log while still
	// re-reporting itself roughly once a minute at the backoff ceiling.
	connectWarnRepeat = 12
	// rejectLogInterval bounds how often one rejection reason reaches the log.
	// An agent looping on a refused push emitted 5000 Error records in about
	// two seconds, which buries every other block-producer log line.
	rejectLogInterval = 30 * time.Second
)

// Config configures a Client.
type Config struct {
	// SocketPath is the path to the agent's Unix-domain service socket.
	SocketPath string
	// Mode is ModeServeKey or ModeSign and must match the agent's socket.
	Mode string
	// OpCert is the operational certificate that anchors validation. It comes
	// from the local --shelley-opcert file and is authoritative: it is placed
	// in the block header, and in serve-key mode every pushed key's KES
	// verification key is cross-checked against it. Required.
	OpCert *forging.OpCert
	// Logger for client events. Defaults to slog.Default().
	Logger *slog.Logger
	// Dial, when non-nil, overrides the default Unix-domain dialer. Tests use
	// this to connect to an in-process fake agent.
	Dial func(ctx context.Context) (net.Conn, error)
	// SignTimeout bounds a single sign-mode round-trip. Zero uses the default.
	SignTimeout time.Duration
	// MinReconnect / MaxReconnect bound the reconnect backoff. Zero uses the
	// defaults.
	MinReconnect time.Duration
	MaxReconnect time.Duration
	// HandshakeTimeout bounds the Hello exchange on a freshly accepted
	// connection. Zero uses the default.
	HandshakeTimeout time.Duration
	// PromRegistry, when non-nil, receives the client's connection and key
	// state metrics.
	PromRegistry prometheus.Registerer
}

// Client sources KES material from a bursa KES agent and implements
// forging.KESSigner. In serve-key mode it holds the current KES signing key
// (refreshed on every agent push) and signs headers locally, so the node
// keeps forging with the last received key even if the agent drops. In sign
// mode it forwards header bodies to the agent and returns the agent's
// signature; the key never enters the node.
type Client struct {
	cfg     Config
	logger  *slog.Logger
	opCert  *forging.OpCert
	kesVKey []byte // expected KES vkey (from opCert), used to vet pushes
	start   uint64 // opCert KES start period
	metrics *clientMetrics

	// ready is closed once the client can produce a signature: a validated key
	// is held (serve-key) or a session is established (sign). WaitForReady
	// blocks on it so a misconfigured socket surfaces at startup instead of at
	// the first slot win.
	ready     chan struct{}
	readyOnce sync.Once

	// rejectMu guards the rejection-log throttle. It is separate from mu
	// because a rejection is reported from inside the locked section of
	// applyKeyPush.
	rejectMu         sync.Mutex
	rejectLast       map[string]time.Time
	rejectSuppressed map[string]uint64

	mu sync.Mutex
	// serve-key: the current local signing key, evolving forward. nil until
	// the first push.
	kesSKey *kes.SecretKey

	// sign: request pump.
	reqCh chan *signReq

	startOnce sync.Once
	started   bool
	// closed is set by Close. Once set, no further key push may be applied,
	// so a push already in flight cannot repopulate key material after the
	// shutdown wipe.
	closed bool
	// stopped is set when the Start context is cancelled. It closes the
	// registration race between a successful dial and the cancellation watcher.
	stopped bool
	// stop cancels the background loop's own context, so Close stops the
	// loop rather than leaving it running against a cancelled-later node
	// context. conn is the loop's current connection, closed by Close and by
	// the single cancellation watcher so a blocked read always unblocks.
	stop context.CancelFunc
	conn net.Conn
}

var _ forging.KESSigner = (*Client)(nil)

type signReq struct {
	period uint64
	msg    []byte
	resp   chan signResp
}

type signResp struct {
	sig []byte
	err error
}

// New creates a Client. It does not connect; call Start to begin.
func New(cfg Config) (*Client, error) {
	if cfg.SocketPath == "" && cfg.Dial == nil {
		return nil, errors.New("kesagent: socket path is required")
	}
	if cfg.Mode != ModeServeKey && cfg.Mode != ModeSign {
		return nil, fmt.Errorf("kesagent: invalid mode %q", cfg.Mode)
	}
	if cfg.OpCert == nil {
		return nil, errors.New("kesagent: operational certificate is required")
	}
	if len(cfg.OpCert.KESVKey) == 0 {
		return nil, errors.New(
			"kesagent: operational certificate has no KES vkey",
		)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.MinReconnect <= 0 {
		cfg.MinReconnect = defaultMinReconnect
	}
	if cfg.MaxReconnect <= 0 {
		cfg.MaxReconnect = defaultMaxReconnect
	}
	if cfg.SignTimeout <= 0 {
		cfg.SignTimeout = defaultSignTimeout
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = defaultHandshakeTimeout
	}
	c := &Client{
		cfg:              cfg,
		logger:           logger.With("component", "kesagent"),
		opCert:           cfg.OpCert,
		kesVKey:          bytes.Clone(cfg.OpCert.KESVKey),
		start:            cfg.OpCert.KESPeriod,
		reqCh:            make(chan *signReq),
		ready:            make(chan struct{}),
		rejectLast:       make(map[string]time.Time),
		rejectSuppressed: make(map[string]uint64),
	}
	if cfg.PromRegistry != nil {
		c.metrics = initClientMetrics(cfg.PromRegistry)
	}
	return c, nil
}

// Start begins the client's background loop and returns immediately. The loop
// runs until ctx is cancelled. Calling Start more than once is a no-op.
func (c *Client) Start(ctx context.Context) {
	c.startOnce.Do(func() {
		// A per-client context so Close stops this loop even when the caller's
		// context outlives the client, which happens on a live restore or
		// truncate: the node context survives and a fresh client replaces this
		// one, leaving the old loop dialling and re-populating key material in
		// an object nothing wipes.
		runCtx, cancel := context.WithCancel(ctx)
		c.mu.Lock()
		c.started = true
		c.stop = cancel
		c.mu.Unlock()
		// One watcher for the client's lifetime, not one per reconnect. It
		// closes whatever connection the loop currently holds, which unblocks
		// a read parked in readFrame.
		go func() {
			<-runCtx.Done()
			c.mu.Lock()
			conn := c.conn
			c.stopped = true
			c.conn = nil
			c.mu.Unlock()
			if conn != nil {
				_ = conn.Close()
			}
		}()
		switch c.cfg.Mode {
		case ModeServeKey:
			go c.runServeKey(runCtx)
		case ModeSign:
			go c.runSign(runCtx)
		}
	})
}

// setConn records or clears the loop's current connection so Close can unblock
// a parked read. Passing nil clears it.
func (c *Client) setConn(conn net.Conn) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
}

// registerConn atomically records a newly connected socket with the client
// lifecycle state. A context cancellation or Close racing the dial cannot
// leave an untracked connection behind.
func (c *Client) registerConn(ctx context.Context, conn net.Conn) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if c.stopped {
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// dial connects to the agent and reads/verifies the Hello handshake.
func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	var (
		conn net.Conn
		err  error
	)
	if c.cfg.Dial != nil {
		conn, err = c.cfg.Dial(ctx)
	} else {
		var d net.Dialer
		conn, err = d.DialContext(ctx, "unix", c.cfg.SocketPath)
	}
	if err != nil {
		return nil, fmt.Errorf("kesagent: dial: %w", err)
	}
	// Bound the handshake. An agent that accepts the socket and never sends
	// Hello would otherwise park this read forever, and because it happens
	// before the loop records the connection, cancelling the context could not
	// reach it either. Close on cancellation as well, so a shutdown mid
	// handshake does not wait out the deadline.
	handshakeDone := make(chan struct{})
	defer close(handshakeDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-handshakeDone:
		}
	}()
	_ = conn.SetReadDeadline(time.Now().Add(c.cfg.HandshakeTimeout))
	var hello Hello
	if err := readFrame(conn, &hello); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("kesagent: read hello: %w", err)
	}
	// Clear the deadline: subsequent reads are long-lived by design in
	// serve-key mode, and bounded per request in sign mode.
	_ = conn.SetReadDeadline(time.Time{})
	if hello.Protocol != ProtocolID {
		_ = conn.Close()
		return nil, fmt.Errorf(
			"kesagent: unexpected protocol %q (want %q)",
			hello.Protocol, ProtocolID,
		)
	}
	if hello.Mode != c.cfg.Mode {
		_ = conn.Close()
		return nil, fmt.Errorf(
			"kesagent: agent socket mode %q does not match configured mode %q",
			hello.Mode, c.cfg.Mode,
		)
	}
	return conn, nil
}

// runServeKey maintains a connection to the agent, receiving key pushes and
// applying them to local state. It reconnects with backoff on any error and
// exits when ctx is cancelled. The last received key remains usable across
// reconnects.
func (c *Client) runServeKey(ctx context.Context) {
	backoff := c.cfg.MinReconnect
	failures := uint64(0)
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := c.dial(ctx)
		if err != nil {
			failures++
			c.noteConnectFailure(ctx, ModeServeKey, failures, err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, c.cfg.MaxReconnect)
			continue
		}
		failures = 0
		c.setConnectedMetric(true)
		c.logger.Info("connected to KES agent (serve-key)")
		// Recorded atomically with the lifecycle state so cancellation racing
		// this dial cannot leave the connection untracked.
		if err := c.registerConn(ctx, conn); err != nil {
			_ = conn.Close()
			return
		}
		installedKey := false
		for {
			var kp KeyPush
			if err := readFrame(conn, &kp); err != nil {
				_ = conn.Close()
				c.setConn(nil)
				c.setConnectedMetric(false)
				if ctx.Err() == nil {
					c.logger.Warn(
						"KES agent connection lost; reconnecting",
						"error",
						err,
					)
				}
				break
			}
			if c.applyKeyPush(ctx, kp) {
				installedKey = true
			}
		}
		// A session that ended must back off too. Previously only a failed
		// dial slept, so an agent that completed the handshake and then closed
		// -- a shutdown window, or any listener that answers and hangs up --
		// sent this loop straight back to dial with no delay: measured at
		// thousands of reconnects per second, each emitting the Warn above.
		// MinReconnect/MaxReconnect are documented as bounding reconnect, and
		// on this path they bounded nothing.
		//
		// Only a session that actually installed a key is productive, so a
		// long-lived connection that drops reconnects promptly. A session that
		// pushed something applyKeyPush refused -- an agent holding the wrong
		// pool's key, a mislabelled frame, a stale period -- installed nothing,
		// so it escalates like a session that pushed nothing at all. Counting a
		// refused push as productive pinned the backoff at MinReconnect forever
		// and logged a rejection every interval.
		if installedKey {
			backoff = c.cfg.MinReconnect
		}
		if !sleepCtx(ctx, backoff) {
			return
		}
		if !installedKey {
			backoff = nextBackoff(backoff, c.cfg.MaxReconnect)
		}
	}
}

// applyKeyPush installs a pushed key into local state after cross-checking it
// against the operational certificate. It reports whether the key was
// installed; every rejection path returns false, which is what lets the
// reconnect loop tell a productive session from one that delivered nothing
// usable.
func (c *Client) applyKeyPush(ctx context.Context, kp KeyPush) bool {
	// Refuse anything that is not a key push. Trusting the frame's contents
	// without checking what it claims to be would let a mislabelled or
	// out-of-band frame install key material. The type is required, not
	// merely checked when present: protocol.go documents every serve-key frame
	// as carrying "type":"key_push", so an agent that omits it is not speaking
	// this protocol and its frame's other fields mean nothing.
	if kp.Type != KeyPushType {
		c.rejectPush(
			ctx,
			slog.LevelError,
			"ignoring KES key push: unexpected frame type",
			"type", kp.Type,
		)
		wipe(kp.KESSignKey)
		return false
	}
	// Compare unconditionally. Treating an absent vkey as "nothing to check"
	// let a push with no vkey skip the operational-certificate cross-check
	// entirely and be installed as trusted.
	if !bytes.Equal(kp.KESVKey, c.kesVKey) {
		c.rejectPush(
			ctx,
			slog.LevelError,
			"ignoring KES key push: pushed KES vkey missing or does not match operational certificate",
		)
		wipe(kp.KESSignKey)
		return false
	}
	if kp.Period < c.start {
		c.rejectPush(
			ctx,
			slog.LevelWarn,
			"ignoring KES key push: pushed period before opcert start",
			"pushed_period", kp.Period,
			"opcert_start", c.start,
		)
		wipe(kp.KESSignKey)
		return false
	}
	// An omitted depth means the Shelley depth. Shelley fixes it, so there is
	// exactly one value a conforming agent can mean, and the checks below still
	// require the resolved value to be that one. The key's own material is
	// bound by the derivation and probe checks further down regardless of what
	// this field claims.
	depth := kp.Depth
	if depth == 0 {
		depth = kes.CardanoKesDepth
	}
	// Shelley fixes the KES tree depth, so any other value is either a
	// misconfigured agent or a hostile one. Rejecting it here bounds the work
	// the agent can make this node do: the vkey derivation below walks the
	// whole tree, so an arbitrarily deep key would be arbitrarily expensive to
	// reject, and a key at any other depth could not produce Shelley-valid
	// signatures even if it were installed.
	if depth != kes.CardanoKesDepth {
		c.rejectPush(
			ctx,
			slog.LevelError,
			"ignoring KES key push: unsupported key depth",
			"depth", depth,
			"want", kes.CardanoKesDepth,
		)
		wipe(kp.KESSignKey)
		return false
	}
	// Validate the key's layout before it can reach kes.Sign. A short or
	// malformed buffer indexes out of range while deriving the public key, so
	// the size check has to come first.
	if want := secretKeySize(depth); len(kp.KESSignKey) != want {
		c.rejectPush(
			ctx,
			slog.LevelError,
			"ignoring KES key push: unexpected signing key size",
			"size", len(kp.KESSignKey),
			"want", want,
			"depth", depth,
		)
		wipe(kp.KESSignKey)
		return false
	}
	internal := kp.Period - c.start
	maxPeriod := kes.MaxPeriod(depth)
	if internal >= maxPeriod {
		c.rejectPush(
			ctx,
			slog.LevelWarn,
			"ignoring KES key push: pushed period is at or beyond key expiry",
			"relative_period", internal,
			"max_period", maxPeriod,
		)
		wipe(kp.KESSignKey)
		return false
	}
	// Derive the public key from the pushed secret and require it to match the
	// operational certificate. The vkey compared above is only what the agent
	// asserts; this checks the key actually sent.
	candidate := &kes.SecretKey{
		Depth:  depth,
		Period: kp.Period - c.start,
		Data:   bytes.Clone(kp.KESSignKey),
	}
	derived := kes.PublicKey(candidate)
	if !bytes.Equal(derived, c.kesVKey) {
		c.rejectPush(
			ctx,
			slog.LevelError,
			"ignoring KES key push: signing key does not derive the operational certificate's KES vkey",
		)
		wipe(candidate.Data)
		wipe(kp.KESSignKey)
		return false
	}
	// The comparison above is necessary but nowhere near sufficient. For any
	// depth above 0, kes.PublicKey with no cached public key falls through to
	// publicKeyInternal, which returns HashPair of the two root public keys
	// *stored in the buffer* rather than deriving them from the child secret.
	// At depth 6 that validates 64 of 608 bytes -- and those 64 bytes are
	// public: they appear verbatim as sig[384:448] of every KES signature this
	// pool has ever published. A key assembled from random material plus those
	// bytes copied out of any past block header passes the check above.
	//
	// Signing a probe and verifying it against the operational certificate's
	// vkey is what actually exercises the secret material. kes.Sign requires
	// period == candidate.Period and does not mutate the key.
	probe := []byte("dingo kes-agent pushed-key verification")
	probeSig, err := kes.Sign(candidate, candidate.Period, probe)
	if err != nil {
		c.rejectPush(
			ctx,
			slog.LevelError,
			"ignoring KES key push: pushed key could not sign a verification probe",
			"error",
			err,
		)
		wipe(candidate.Data)
		wipe(kp.KESSignKey)
		return false
	}
	if !kes.VerifySignedKES(c.kesVKey, candidate.Period, probe, probeSig) {
		c.rejectPush(
			ctx,
			slog.LevelError,
			"ignoring KES key push: pushed key does not sign for the operational certificate's KES vkey",
		)
		wipe(candidate.Data)
		wipe(kp.KESSignKey)
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// A push racing shutdown must not repopulate key material after the wipe.
	if c.closed {
		wipe(candidate.Data)
		wipe(kp.KESSignKey)
		return false
	}
	// Reject a push that would move the key backward (stale/duplicate). Say so:
	// the reconnect loop counts this session as unproductive and escalates its
	// backoff to MaxReconnect, and without a record the escalation has no
	// explanation anywhere. An agent stuck on a period the node has already
	// evolved past is the common cause.
	if c.kesSKey != nil && internal < c.kesSKey.Period {
		wipe(candidate.Data)
		wipe(kp.KESSignKey)
		c.rejectPush(
			ctx,
			slog.LevelWarn,
			"ignoring KES key push: pushed period is behind the key already held",
			"pushed_relative_period",
			internal,
			"held_relative_period",
			c.kesSKey.Period,
		)
		return false
	}
	if c.kesSKey != nil {
		wipe(c.kesSKey.Data)
	}
	c.kesSKey = candidate
	wipe(kp.KESSignKey)
	c.setKeyMetric(true, kp.Period)
	c.markReady()
	c.logger.Info("received KES key from agent", "absolute_period", kp.Period)
	return true
}

// markReady releases WaitForReady. Called on the first installed key in
// serve-key mode and on the first established session in sign mode.
func (c *Client) markReady() {
	c.readyOnce.Do(func() { close(c.ready) })
}

// noteConnectFailure reports a failed connection attempt.
//
// The first failure and every connectWarnRepeat-th consecutive failure are
// logged at Warn. At Debug they were invisible: a wrong
// --shelley-kes-agent-socket path produced no record at Info or above while the
// node logged "KES signing key sourced from agent" and "block forger started in
// production mode", so the node reported a healthy producer and forged nothing
// until a slot was lost. The intervening failures stay at Debug so a long agent
// outage does not flood the log.
func (c *Client) noteConnectFailure(
	ctx context.Context,
	mode string,
	failures uint64,
	err error,
) {
	if c.metrics != nil {
		c.metrics.connectFailures.Inc()
	}
	level := slog.LevelDebug
	if failures == 1 || failures%connectWarnRepeat == 0 {
		level = slog.LevelWarn
	}
	c.logger.Log(
		ctx,
		level,
		"could not connect to KES agent; block production will fail until it is reachable",
		"mode",
		mode,
		"socket",
		c.cfg.SocketPath,
		"consecutive_failures",
		failures,
		"error",
		err,
	)
}

// rejectPush records a refused key push.
//
// Every rejection increments the rejected-push counter, but at most one record
// per reason reaches the log per rejectLogInterval: an agent looping on a
// refused push produced 5000 Error records in about two seconds. The suppressed
// count rides along on the next record for that reason, so throttling hides the
// repetition and not the volume.
func (c *Client) rejectPush(
	ctx context.Context,
	level slog.Level,
	msg string,
	attrs ...any,
) {
	if c.metrics != nil {
		c.metrics.rejectedPushes.Inc()
	}
	now := time.Now()
	c.rejectMu.Lock()
	if last, seen := c.rejectLast[msg]; seen &&
		now.Sub(last) < rejectLogInterval {
		c.rejectSuppressed[msg]++
		c.rejectMu.Unlock()
		return
	}
	suppressed := c.rejectSuppressed[msg]
	delete(c.rejectSuppressed, msg)
	c.rejectLast[msg] = now
	c.rejectMu.Unlock()
	if suppressed > 0 {
		attrs = append(attrs, "suppressed_since_last_record", suppressed)
	}
	c.logger.Log(ctx, level, msg, attrs...)
}

// runSign keeps a connection to the agent and pumps sign requests over it,
// reconnecting on error, until ctx is cancelled.
//
// The connection is established eagerly and re-established while idle rather
// than lazily on the first request. Two reasons, both about the gap between
// slot wins, which on a real pool is hours:
//
//   - A wrong socket path, a permission problem, or a mode mismatch is then
//     reported at startup instead of at the first slot win.
//   - An agent that closes an idle connection (an idle timeout, a restart, a
//     socket recycle) is noticed while nothing is waiting on it, instead of by
//     the write that follows the FIN: parked in a select on reqCh the pump never
//     read the socket, so the close went unseen and the next slot win failed
//     with "write: broken pipe" and forfeited the block.
func (c *Client) runSign(ctx context.Context) {
	var (
		conn     net.Conn
		watcher  *idleWatcher
		failures uint64
		// served records whether the current session answered a request. A
		// session that served nothing is unproductive, so it backs off like a
		// failed dial: an agent that accepts and immediately closes -- its
		// shutdown window, or a listener that answers and hangs up -- otherwise
		// sends this loop straight back to dial with no delay.
		served bool
	)
	defer func() {
		if watcher != nil {
			watcher.stop()
		}
		if conn != nil {
			_ = conn.Close()
		}
	}()
	backoff := c.cfg.MinReconnect
	for {
		if ctx.Err() != nil {
			return
		}
		if conn == nil {
			newConn, err := c.dial(ctx)
			if err != nil {
				failures++
				c.noteConnectFailure(ctx, ModeSign, failures, err)
				ok, servedOne := c.waitBeforeReconnect(ctx, backoff, &conn)
				if !ok {
					return
				}
				if servedOne {
					served = true
					backoff = c.cfg.MinReconnect
					continue
				}
				backoff = nextBackoff(backoff, c.cfg.MaxReconnect)
				continue
			}
			// Recorded so the client's cancellation watcher can close a
			// connection parked in a read; otherwise a shutdown while a request
			// is in flight leaks this goroutine and the socket until the agent
			// replies.
			if err := c.registerConn(ctx, newConn); err != nil {
				_ = newConn.Close()
				return
			}
			conn = newConn
			failures = 0
			served = false
			c.setConnectedMetric(true)
			c.markReady()
			c.logger.Info("connected to KES agent (sign)")
		}
		if watcher == nil {
			watcher = newIdleWatcher(conn)
		}
		select {
		case <-ctx.Done():
			return
		case <-watcher.broken:
			watcher.stop()
			watcher = nil
			c.dropSignConn(&conn)
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn(
				"KES agent closed the idle sign connection; reconnecting",
			)
			if served {
				backoff = c.cfg.MinReconnect
			}
			ok, servedOne := c.waitBeforeReconnect(ctx, backoff, &conn)
			if !ok {
				return
			}
			if servedOne {
				served = true
				backoff = c.cfg.MinReconnect
			} else if !served {
				backoff = nextBackoff(backoff, c.cfg.MaxReconnect)
			}
		case req := <-c.reqCh:
			// The idle read has to stop before the connection carries a
			// request, or it consumes the response's first byte.
			if !watcher.stop() {
				c.dropSignConn(&conn)
			}
			watcher = nil
			sig, err := c.serveSignRequest(ctx, &conn, req)
			req.resp <- signResp{sig: sig, err: err}
			if err == nil {
				served = true
				backoff = c.cfg.MinReconnect
			}
		}
	}
}

// dropSignConn closes and forgets the pump's connection.
func (c *Client) dropSignConn(conn *net.Conn) {
	current := *conn
	*conn = nil
	if current == nil {
		return
	}
	_ = current.Close()
	c.setConn(nil)
	c.setConnectedMetric(false)
}

// serveSignRequest performs one sign round trip, dialling if the pump holds no
// connection and retrying exactly once on a transport error.
//
// The retry is what stops an idle-closed connection from costing a block. Even
// with the idle watcher there is a window: a FIN that arrives between stopping
// the watcher and writing the request is only discovered by that write, as
// "write: broken pipe". Retrying on a fresh connection turns a lost slot into a
// slightly slower one. An error the agent actually replied with is not a
// transport fault and is not retried — asking the same question twice would get
// the same answer.
func (c *Client) serveSignRequest(
	ctx context.Context,
	conn *net.Conn,
	req *signReq,
) ([]byte, error) {
	var lastErr error
	for attempt := range 2 {
		if *conn == nil {
			newConn, err := c.dial(ctx)
			if err != nil {
				c.noteConnectFailure(ctx, ModeSign, 1, err)
				if lastErr != nil {
					return nil, lastErr
				}
				return nil, err
			}
			if err := c.registerConn(ctx, newConn); err != nil {
				_ = newConn.Close()
				return nil, err
			}
			*conn = newConn
			c.setConnectedMetric(true)
			c.markReady()
		}
		sig, err := roundTripSign(
			*conn,
			c.kesVKey,
			c.start,
			req.period,
			req.msg,
			c.cfg.SignTimeout,
		)
		if err == nil {
			return sig, nil
		}
		lastErr = err
		if c.metrics != nil {
			c.metrics.signFailures.Inc()
		}
		// An agent-level refusal came back over a working connection; keep it.
		if errors.Is(err, ErrAgentSign) {
			return nil, err
		}
		c.dropSignConn(conn)
		var transport *transportError
		if attempt == 0 && errors.As(err, &transport) && ctx.Err() == nil {
			c.logger.Warn(
				"KES agent sign failed on a stale connection; retrying once on a fresh one",
				"error",
				err,
			)
			continue
		}
		c.logger.Warn("KES agent sign failed; will reconnect", "error", err)
		return nil, err
	}
	return nil, lastErr
}

// waitBeforeReconnect holds off the next dial for d while continuing to serve
// sign requests. It reports whether the loop should continue and whether any
// request was served during the wait.
//
// A request arriving during the backoff dials on its own rather than being
// refused or left queued: the backoff exists to stop a hot reconnect loop
// against an agent that keeps hanging up, not to decline a slot win. The forging
// loop calls KESSign synchronously, so a request parked for a whole backoff
// costs slots.
func (c *Client) waitBeforeReconnect(
	ctx context.Context,
	d time.Duration,
	conn *net.Conn,
) (bool, bool) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	served := false
	for {
		select {
		case <-ctx.Done():
			return false, served
		case <-timer.C:
			return true, served
		case req := <-c.reqCh:
			sig, err := c.serveSignRequest(ctx, conn, req)
			req.resp <- signResp{sig: sig, err: err}
			if err == nil {
				served = true
			}
		}
	}
}

// idleWatcher reads one byte from an otherwise idle sign connection so a FIN
// from the agent is noticed while nothing is waiting on the socket.
//
// The read must be stopped before the connection carries a request, or it would
// consume the response's first byte. stop unblocks it with a read deadline in
// the past, waits for it to exit, then clears the deadline again.
type idleWatcher struct {
	conn net.Conn
	// broken is closed when the connection cannot carry the next request.
	broken chan struct{}
	// done is closed when the reader goroutine has exited.
	done chan struct{}
}

func newIdleWatcher(conn net.Conn) *idleWatcher {
	w := &idleWatcher{
		conn:   conn,
		broken: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go func() {
		defer close(w.done)
		var b [1]byte
		_, err := conn.Read(b[:])
		// A deadline error is stop() unblocking this read, and is the only
		// outcome that leaves the connection usable. Anything else -- EOF, a
		// reset, a closed socket, or the agent sending a frame nobody asked
		// for -- means it cannot carry the next request.
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			close(w.broken)
		}
	}()
	return w
}

// stop ends the idle read and reports whether the connection is still usable.
func (w *idleWatcher) stop() bool {
	// A deadline already in the past unblocks the read immediately.
	_ = w.conn.SetReadDeadline(time.Now().Add(-time.Second))
	<-w.done
	_ = w.conn.SetReadDeadline(time.Time{})
	select {
	case <-w.broken:
		return false
	default:
		return true
	}
}

// transportError marks a failure of the connection itself, as distinct from a
// reply the agent actually sent. Only a transport failure is worth retrying on
// a fresh connection.
type transportError struct {
	err error
}

func (e *transportError) Error() string { return e.err.Error() }

func (e *transportError) Unwrap() error { return e.err }

// roundTripSign performs one sign request/response over conn.
func roundTripSign(
	conn net.Conn,
	kesVKey []byte,
	opCertStart uint64,
	period uint64,
	msg []byte,
	timeout time.Duration,
) ([]byte, error) {
	// Bound the round trip. Without this an agent that accepts a request and
	// never replies parks the pump forever: the caller's SignTimeout only
	// abandons its own wait, so every later request then fails to queue.
	if timeout > 0 {
		deadline := time.Now().Add(timeout)
		_ = conn.SetWriteDeadline(deadline)
		_ = conn.SetReadDeadline(deadline)
		defer func() {
			_ = conn.SetWriteDeadline(time.Time{})
			_ = conn.SetReadDeadline(time.Time{})
		}()
	}
	if err := writeFrame(conn, SignRequest{
		Type:    SignRequestType,
		Period:  period,
		Message: msg,
	}); err != nil {
		return nil, &transportError{err: err}
	}
	var resp SignResponse
	if err := readFrame(conn, &resp); err != nil {
		return nil, &transportError{err: err}
	}
	if resp.Error != "" {
		// An agent-level error (e.g. exhausted key) is not a transport
		// failure; surface it without dropping the connection.
		return nil, fmt.Errorf("%w: %s", ErrAgentSign, resp.Error)
	}
	// Validate the reply rather than trusting whatever came back. A
	// mislabelled frame, a reply for a different period, or an empty
	// signature would otherwise be reported as a successful signature and
	// only surface as an invalid block.
	if resp.Type != SignResponseType {
		return nil, fmt.Errorf(
			"kesagent: unexpected response type %q (want %q)",
			resp.Type, SignResponseType,
		)
	}
	if resp.Period != period {
		return nil, fmt.Errorf(
			"kesagent: response period %d does not match request period %d",
			resp.Period, period,
		)
	}
	if len(resp.Signature) == 0 {
		return nil, errors.New("kesagent: agent returned an empty signature")
	}
	relativePeriod, err := relativeKESPeriod(opCertStart, period)
	if err != nil {
		return nil, err
	}
	if !kes.VerifySignedKES(kesVKey, relativePeriod, msg, resp.Signature) {
		return nil, ErrInvalidSignature
	}
	return resp.Signature, nil
}

// KESSign signs message at the given absolute KES period.
func (c *Client) KESSign(period uint64, message []byte) ([]byte, error) {
	switch c.cfg.Mode {
	case ModeSign:
		return c.signRemote(period, message)
	default:
		return c.signLocal(period, message)
	}
}

func (c *Client) signRemote(period uint64, message []byte) ([]byte, error) {
	c.mu.Lock()
	started := c.started
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	if !started {
		return nil, ErrNotStarted
	}
	req := &signReq{period: period, msg: message, resp: make(chan signResp, 1)}
	timer := time.NewTimer(c.cfg.SignTimeout)
	defer timer.Stop()
	select {
	case c.reqCh <- req:
	case <-timer.C:
		return nil, errors.New("kesagent: timed out queuing sign request")
	}
	select {
	case r := <-req.resp:
		return r.sig, r.err
	case <-timer.C:
		return nil, errors.New("kesagent: timed out waiting for sign response")
	}
}

func (c *Client) signLocal(period uint64, message []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	rel, err := c.relativePeriod(period)
	if err != nil {
		return nil, err
	}
	if err := c.evolveLocked(rel); err != nil {
		return nil, err
	}
	return kes.Sign(c.kesSKey, rel, message)
}

// UpdateKESPeriod evolves the local KES key to the given absolute period
// (serve-key mode). In sign mode the agent evolves its own key, so this is a
// no-op.
func (c *Client) UpdateKESPeriod(period uint64) error {
	if c.cfg.Mode == ModeSign {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rel, err := c.relativePeriod(period)
	if err != nil {
		return err
	}
	return c.evolveLocked(rel)
}

// relativePeriod converts an absolute KES period to the period relative to the
// operational certificate's start period.
func (c *Client) relativePeriod(period uint64) (uint64, error) {
	return relativeKESPeriod(c.start, period)
}

func relativeKESPeriod(start, period uint64) (uint64, error) {
	if period < start {
		return 0, fmt.Errorf(
			"kesagent: absolute period %d is before opcert start %d",
			period,
			start,
		)
	}
	return period - start, nil
}

// evolveLocked evolves the local key forward to the given relative period.
// Caller holds c.mu.
func (c *Client) evolveLocked(rel uint64) error {
	if c.kesSKey == nil {
		return ErrNoKeyYet
	}
	if rel < c.kesSKey.Period {
		return fmt.Errorf(
			"%w: requested relative %d, key at %d",
			ErrPastPeriod, rel, c.kesSKey.Period,
		)
	}
	for c.kesSKey.Period < rel {
		next, err := kes.Update(c.kesSKey)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrExhausted, err.Error())
		}
		wipe(c.kesSKey.Data)
		c.kesSKey = next
	}
	return nil
}

// GetOpCert returns a copy of the operational certificate.
func (c *Client) GetOpCert() *forging.OpCert {
	if c.opCert == nil {
		return nil
	}
	return &forging.OpCert{
		KESVKey:     bytes.Clone(c.opCert.KESVKey),
		IssueNumber: c.opCert.IssueNumber,
		KESPeriod:   c.opCert.KESPeriod,
		Signature:   bytes.Clone(c.opCert.Signature),
		ColdVKey:    bytes.Clone(c.opCert.ColdVKey),
	}
}

// OpCertExpiryPeriod returns the absolute KES period at which the operational
// certificate expires.
func (c *Client) OpCertExpiryPeriod() uint64 {
	if c.opCert == nil {
		return 0
	}
	return c.opCert.KESPeriod + kes.MaxPeriod(kes.CardanoKesDepth)
}

// PeriodsRemaining returns how many KES periods remain before expiry.
func (c *Client) PeriodsRemaining(currentPeriod uint64) uint64 {
	expiry := c.OpCertExpiryPeriod()
	if currentPeriod >= expiry {
		return 0
	}
	return expiry - currentPeriod
}

// CurrentPeriod returns the absolute KES period the locally held key is
// evolved to (serve-key mode), or 0 when no key is held. It is always 0 in
// sign mode, where the agent owns the key.
func (c *Client) CurrentPeriod() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.kesSKey == nil {
		return 0
	}
	return c.start + c.kesSKey.Period
}

// HasKey reports whether a local KES key is currently held (serve-key mode).
// It is always false in sign mode. Useful for readiness checks.
func (c *Client) HasKey() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.kesSKey != nil
}

// WaitForReady blocks until the client can produce a signature, or until
// timeout elapses or ctx is cancelled.
//
// Ready means a validated key is held in serve-key mode, and a session with the
// agent is established in sign mode. Startup calls this so a wrong socket path,
// a permission problem, or a mode mismatch is reported while the operator is
// still watching the node start, rather than at the first slot win hours later.
// It does not keep waiting past the timeout: the agent is a separate process
// that may legitimately come up after the node, and a producer that logs loudly
// and keeps retrying is better than one that refuses to start because of a
// restarting agent.
func (c *Client) WaitForReady(
	ctx context.Context,
	timeout time.Duration,
) error {
	c.mu.Lock()
	started, closed := c.started, c.closed
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if !started {
		return ErrNotStarted
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("%w after %s", ErrNotReady, timeout)
	}
}

// Close wipes any locally held key material.
// Close stops the background loop and wipes any key material the client holds.
// It is safe to call more than once, and safe to call on a client that was
// never started.
//
// Stopping the loop is part of the contract, not a nicety: the node context can
// outlive a client (a live restore or truncate replaces the client while the
// node keeps running), and a loop left running would keep dialling the agent
// and repopulating key material in an object nothing wipes. Marking the client
// closed also stops a push already in flight from landing after the wipe.
func (c *Client) Close() {
	c.mu.Lock()
	c.closed = true
	stop := c.stop
	conn := c.conn
	c.conn = nil
	if c.kesSKey != nil {
		wipe(c.kesSKey.Data)
		c.kesSKey = nil
	}
	c.mu.Unlock()
	c.setKeyMetric(false, 0)
	c.setConnectedMetric(false)

	if stop != nil {
		stop()
	}
	if conn != nil {
		_ = conn.Close()
	}
}

// secretKeySize mirrors the KES secret key layout: a 32-byte seed plus, at each
// level, a seed and the two child public keys. The gouroboros helper is
// unexported, and the length has to be known before handing a pushed buffer to
// key derivation, which indexes into it.
func secretKeySize(depth uint64) int {
	const (
		ed25519KeySize = 32
		perLevel       = 96
	)
	if depth == 0 {
		return ed25519KeySize
	}
	return ed25519KeySize + int(
		depth,
	)*perLevel // #nosec G115 -- depth is bounded by the caller
}

func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		return max
	}
	return next
}
