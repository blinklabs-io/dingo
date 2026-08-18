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
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/gouroboros/kes"
)

// Errors returned by the client.
var (
	// ErrNoKeyYet is returned by KESSign in serve-key mode before the agent
	// has pushed a key (e.g. immediately after startup, or while the agent
	// is unreachable and no key was ever received).
	ErrNoKeyYet = errors.New("kesagent: no KES key received from agent yet")
	// ErrExhausted is returned when the requested period is beyond the
	// key's remaining evolutions.
	ErrExhausted = errors.New("kesagent: KES key exhausted for requested period")
	// ErrPastPeriod is returned when a period below the key's current period
	// is requested (KES keys only evolve forward).
	ErrPastPeriod = errors.New("kesagent: requested KES period is in the past")
	// ErrNotStarted is returned by sign-mode KESSign before Start.
	ErrNotStarted = errors.New("kesagent: client not started")
	// ErrClosed is returned when KESSign is called after Close.
	ErrClosed = errors.New("kesagent: client is closed")
	// ErrInvalidSignature is returned when the agent's signature fails KES
	// verification.
	ErrInvalidSignature = errors.New("kesagent: agent returned an invalid signature")
)

const (
	defaultMinReconnect = 500 * time.Millisecond
	defaultMaxReconnect = 5 * time.Second
	defaultSignTimeout  = 5 * time.Second
	// defaultHandshakeTimeout bounds the Hello exchange so an agent that
	// accepts the socket without speaking cannot wedge the client.
	defaultHandshakeTimeout = 5 * time.Second
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
		return nil, errors.New("kesagent: operational certificate has no KES vkey")
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
	return &Client{
		cfg:     cfg,
		logger:  logger.With("component", "kesagent"),
		opCert:  cfg.OpCert,
		kesVKey: bytes.Clone(cfg.OpCert.KESVKey),
		start:   cfg.OpCert.KESPeriod,
		reqCh:   make(chan *signReq),
	}, nil
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
	for {
		if ctx.Err() != nil {
			return
		}
		conn, err := c.dial(ctx)
		if err != nil {
			c.logger.Debug("serve-key connect failed; will retry", "error", err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, c.cfg.MaxReconnect)
			continue
		}
		c.logger.Info("connected to KES agent (serve-key)")
		backoff = c.cfg.MinReconnect
		// Recorded atomically with the lifecycle state so cancellation racing
		// this dial cannot leave the connection untracked.
		if err := c.registerConn(ctx, conn); err != nil {
			_ = conn.Close()
			return
		}
		for {
			var kp KeyPush
			if err := readFrame(conn, &kp); err != nil {
				_ = conn.Close()
				c.setConn(nil)
				if ctx.Err() == nil {
					c.logger.Warn("KES agent connection lost; reconnecting", "error", err)
				}
				break
			}
			c.applyKeyPush(kp)
		}
	}
}

// applyKeyPush installs a pushed key into local state after cross-checking it
// against the operational certificate.
func (c *Client) applyKeyPush(kp KeyPush) {
	// Refuse anything that is not a key push. Trusting the frame's contents
	// without checking what it claims to be would let a mislabelled or
	// out-of-band frame install key material.
	if kp.Type != "" && kp.Type != KeyPushType {
		c.logger.Error(
			"ignoring KES key push: unexpected frame type",
			"type", kp.Type,
		)
		wipe(kp.KESSignKey)
		return
	}
	// Compare unconditionally. Treating an absent vkey as "nothing to check"
	// let a push with no vkey skip the operational-certificate cross-check
	// entirely and be installed as trusted.
	if !bytes.Equal(kp.KESVKey, c.kesVKey) {
		c.logger.Error(
			"ignoring KES key push: pushed KES vkey missing or does not match operational certificate",
		)
		wipe(kp.KESSignKey)
		return
	}
	if kp.Period < c.start {
		c.logger.Warn(
			"ignoring KES key push: pushed period before opcert start",
			"pushed_period", kp.Period,
			"opcert_start", c.start,
		)
		wipe(kp.KESSignKey)
		return
	}
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
		c.logger.Error(
			"ignoring KES key push: unsupported key depth",
			"depth", depth,
			"want", kes.CardanoKesDepth,
		)
		wipe(kp.KESSignKey)
		return
	}
	// Validate the key's layout before it can reach kes.Sign. A short or
	// malformed buffer indexes out of range while deriving the public key, so
	// the size check has to come first.
	if want := secretKeySize(depth); len(kp.KESSignKey) != want {
		c.logger.Error(
			"ignoring KES key push: unexpected signing key size",
			"size", len(kp.KESSignKey),
			"want", want,
			"depth", depth,
		)
		wipe(kp.KESSignKey)
		return
	}
	internal := kp.Period - c.start
	maxPeriod := kes.MaxPeriod(depth)
	if internal >= maxPeriod {
		c.logger.Warn(
			"ignoring KES key push: pushed period is at or beyond key expiry",
			"relative_period", internal,
			"max_period", maxPeriod,
		)
		wipe(kp.KESSignKey)
		return
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
		c.logger.Error(
			"ignoring KES key push: signing key does not derive the operational certificate's KES vkey",
		)
		wipe(candidate.Data)
		wipe(kp.KESSignKey)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// A push racing shutdown must not repopulate key material after the wipe.
	if c.closed {
		wipe(candidate.Data)
		wipe(kp.KESSignKey)
		return
	}
	// Reject a push that would move the key backward (stale/duplicate).
	if c.kesSKey != nil && internal < c.kesSKey.Period {
		wipe(candidate.Data)
		wipe(kp.KESSignKey)
		return
	}
	if c.kesSKey != nil {
		wipe(c.kesSKey.Data)
	}
	c.kesSKey = candidate
	wipe(kp.KESSignKey)
	c.logger.Info("received KES key from agent", "absolute_period", kp.Period)
}

// runSign pumps sign requests over a persistent connection, reconnecting on
// error, until ctx is cancelled.
func (c *Client) runSign(ctx context.Context) {
	var conn net.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	backoff := c.cfg.MinReconnect
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.reqCh:
			if conn == nil {
				newConn, err := c.dial(ctx)
				if err != nil {
					c.logger.Debug("sign connect failed", "error", err)
					req.resp <- signResp{err: err}
					if !sleepCtx(ctx, backoff) {
						return
					}
					backoff = nextBackoff(backoff, c.cfg.MaxReconnect)
					continue
				}
				c.logger.Info("connected to KES agent (sign)")
				// Recorded so the client's cancellation watcher can close a
				// connection parked in readFrame; otherwise a shutdown while a
				// request is in flight leaks this goroutine and the socket
				// until the agent replies.
				if err := c.registerConn(ctx, newConn); err != nil {
					_ = newConn.Close()
					req.resp <- signResp{err: err}
					return
				}
				conn = newConn
				backoff = c.cfg.MinReconnect
			}
			sig, err := roundTripSign(
				conn,
				c.kesVKey,
				c.start,
				req.period,
				req.msg,
				c.cfg.SignTimeout,
			)
			if err != nil {
				// Transport error: drop the connection so the next request
				// reconnects.
				_ = conn.Close()
				c.setConn(nil)
				conn = nil
				c.logger.Warn("KES agent sign failed; will reconnect", "error", err)
			}
			req.resp <- signResp{sig: sig, err: err}
		}
	}
}

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
		return nil, err
	}
	var resp SignResponse
	if err := readFrame(conn, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		// An agent-level error (e.g. exhausted key) is not a transport
		// failure; surface it without dropping the connection.
		return nil, fmt.Errorf("kesagent: agent sign error: %s", resp.Error)
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
	return ed25519KeySize + int(depth)*perLevel // #nosec G115 -- depth is bounded by the caller
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
