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
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/blinklabs-io/bursa"
	"github.com/blinklabs-io/gouroboros/kes"
)

const (
	// DefaultHelloTimeout bounds the dial plus the initial Hello frame read.
	// A KES agent socket is attacker-reachable whenever the filesystem path
	// granting access to it is, so the handshake must never be allowed to
	// block indefinitely on a stalled or hostile peer (P1: "unbounded Hello
	// handshake").
	DefaultHelloTimeout = 5 * time.Second

	// DefaultSignTimeout bounds one sign-mode round trip. It must stay well
	// under a slot: block production calls Sign synchronously from the
	// slot-aligned forging loop, so a longer timeout parks forging for
	// several slots when the agent stops answering (P1: "sign round-trip
	// ignores SignTimeout").
	DefaultSignTimeout = 500 * time.Millisecond

	// DefaultFrameBodyTimeout bounds how long a frame body may take to
	// arrive once its length header has been read. It never applies to the
	// header read itself, where a serve-key subscriber legitimately blocks
	// between pushes, so a peer that announces a frame and then stops
	// sending cannot park the subscription loop indefinitely. This mirrors
	// the bound bursa's own agent puts on the same direction
	// (internal/kesagent frameBodyReadTimeout, also 10s).
	DefaultFrameBodyTimeout = 10 * time.Second

	minReconnectBackoff = 250 * time.Millisecond
	maxReconnectBackoff = 30 * time.Second

	// kesPushProbeMessage is signed and verified once, in-process, against
	// every pushed key before it is ever installed: proof that the pushed
	// secret key, verification key, and period are mutually consistent,
	// rather than trusting the agent's own report of them (P1: "pushed key
	// material accepted without validation").
	kesPushProbeMessage = "dingo-kesagent-push-probe"
)

var (
	// ErrClosed is returned by every Client method once Close has been
	// called.
	ErrClosed = errors.New("kesagent: client closed")
	// ErrWrongProtocol is returned when the agent's Hello does not report
	// ProtocolID.
	ErrWrongProtocol = errors.New("kesagent: unrecognized agent protocol")
	// ErrWrongMode is returned when the agent's Hello reports a mode other
	// than the one this client was configured for.
	ErrWrongMode = errors.New(
		"kesagent: agent mode does not match configured client mode",
	)
)

// Config configures a Client.
type Config struct {
	// SocketPath is the Unix-domain service socket of a running bursa KES
	// agent (--shelley-kes-agent-socket).
	SocketPath string
	// Mode selects ModeServeKey or ModeSign. Empty defaults to ModeServeKey.
	Mode string
	// HelloTimeout bounds the dial and handshake. Zero uses
	// DefaultHelloTimeout.
	HelloTimeout time.Duration
	// SignTimeout bounds one sign-mode round trip. Zero uses
	// DefaultSignTimeout. Ignored in serve-key mode.
	SignTimeout time.Duration
	// FrameBodyTimeout bounds the body of a frame whose length header has
	// already been read. Zero uses DefaultFrameBodyTimeout.
	FrameBodyTimeout time.Duration
	// KESVKey and OpCertStartPeriod are required for ModeSign only. The
	// local operational certificate already commits to the agent's KES
	// verification key and the KES period it was issued at, so Sign can
	// cryptographically verify every response without trusting anything the
	// agent reports about its own identity (P1: "sign response accepted
	// without validating its type or KES period" -- this validates the
	// signature itself, which subsumes both).
	KESVKey           []byte
	OpCertStartPeriod uint64

	Logger  *slog.Logger
	Metrics *Metrics
}

// Client is a bursa KES agent service-socket client. It is safe for
// concurrent use: every method holds the client's own mutex around the
// shared connection.
type Client struct {
	cfg    Config
	logger *slog.Logger

	mu   sync.Mutex
	conn net.Conn

	closed bool

	backoff       time.Duration
	nextDialAfter time.Time
}

// NewClient validates cfg and returns a Client. It does not dial the socket;
// the first call to Sign, AwaitPushedKey, or Run does.
func NewClient(cfg Config) (*Client, error) {
	if cfg.SocketPath == "" {
		return nil, errors.New("kesagent: socket path is required")
	}
	// Checked here rather than left to the first dial: an over-long path
	// fails with a bare "invalid argument" that names neither the length nor
	// the limit, and block-producer startup is where an operator can still
	// act on it.
	if err := validateSocketPath(cfg.SocketPath); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "":
		cfg.Mode = ModeServeKey
	case ModeServeKey:
	case ModeSign:
		if len(cfg.KESVKey) != ed25519.PublicKeySize {
			return nil, fmt.Errorf(
				"kesagent: sign mode requires a %d-byte KESVKey, got %d",
				ed25519.PublicKeySize,
				len(cfg.KESVKey),
			)
		}
	default:
		return nil, fmt.Errorf("kesagent: unknown mode %q", cfg.Mode)
	}
	if cfg.HelloTimeout <= 0 {
		cfg.HelloTimeout = DefaultHelloTimeout
	}
	if cfg.SignTimeout <= 0 {
		cfg.SignTimeout = DefaultSignTimeout
	}
	if cfg.FrameBodyTimeout <= 0 {
		cfg.FrameBodyTimeout = DefaultFrameBodyTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Client{cfg: cfg, logger: cfg.Logger}, nil
}

// Close closes the current connection, if any, and marks the client closed;
// every subsequent call fails with ErrClosed.
//
// Close never blocks on network I/O: it only ever holds the mutex long
// enough to grab and clear the current connection, then closes it outside
// the lock. That is what lets it interrupt AwaitPushedKey's unbounded
// idle-between-pushes read (the blocking call Run's background loop lives
// in almost all the time) -- that read holds no lock of its own while
// parked, and closing the net.Conn out from under it is what makes it
// return. Holding the mutex across that read instead would deadlock: Close
// could never acquire it to reach the conn.Close() that is the only thing
// able to unblock the read in the first place.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	c.cfg.Metrics.setConnected(false)
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (c *Client) closeLocked() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.cfg.Metrics.setConnected(false)
	return err
}

// invalidateConn clears c.conn if it is still exactly conn -- a concurrent
// Close or reconnect may already have replaced or cleared it -- records a
// dial failure so the next connect attempt backs off, and closes conn.
// Callers use this instead of closeLocked after a conn captured outside the
// lock (see AwaitPushedKey) turns out to be broken.
func (c *Client) invalidateConn(conn net.Conn) {
	c.mu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	c.recordDialFailureLocked()
	c.mu.Unlock()
	c.cfg.Metrics.setConnected(false)
	_ = conn.Close()
}

// connectLocked dials the socket (if not already connected) and performs the
// bounded Hello handshake. Caller holds c.mu.
//
// A completed handshake deliberately does not clear the reconnect backoff.
// Reaching Hello proves only that something is listening on the socket, and
// the failures the backoff exists to throttle -- an agent that pushes key
// material the node cannot install, or one that refuses or corrupts every
// sign -- all reconnect successfully first. Clearing here restarted every one
// of them at minReconnectBackoff, however long they had been failing. The
// reset belongs to the operation that actually succeeded: Run after an
// install, Sign after a verified signature.
func (c *Client) connectLocked(ctx context.Context) error {
	if c.closed {
		return ErrClosed
	}
	if c.conn != nil {
		return nil
	}
	if !c.nextDialAfter.IsZero() && time.Now().Before(c.nextDialAfter) {
		return fmt.Errorf(
			"kesagent: reconnect backoff active until %s",
			c.nextDialAfter.Format(time.RFC3339),
		)
	}

	dialCtx, cancel := context.WithTimeout(ctx, c.cfg.HelloTimeout)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(dialCtx, "unix", c.cfg.SocketPath)
	if err != nil {
		c.recordDialFailureLocked()
		return fmt.Errorf("kesagent: dial %s: %w", c.cfg.SocketPath, err)
	}

	// The Hello read is one-shot, not the idle-between-messages wait a
	// serve-key subscriber legitimately blocks on afterward, so it must be
	// bounded explicitly rather than left to block on a stalled or hostile
	// agent.
	if err := conn.SetDeadline(time.Now().Add(c.cfg.HelloTimeout)); err != nil {
		_ = conn.Close()
		c.recordDialFailureLocked()
		return fmt.Errorf("kesagent: set hello deadline: %w", err)
	}
	var hello Hello
	if err := readFrame(conn, MaxHelloFrameLen, &hello); err != nil {
		_ = conn.Close()
		c.recordDialFailureLocked()
		return fmt.Errorf("kesagent: read hello: %w", err)
	}
	if hello.Protocol != ProtocolID {
		_ = conn.Close()
		c.recordDialFailureLocked()
		return fmt.Errorf(
			"%w: got %q, want %q",
			ErrWrongProtocol,
			hello.Protocol,
			ProtocolID,
		)
	}
	if hello.Mode != c.cfg.Mode {
		_ = conn.Close()
		c.recordDialFailureLocked()
		return fmt.Errorf(
			"%w: agent serves %q, client configured for %q",
			ErrWrongMode,
			hello.Mode,
			c.cfg.Mode,
		)
	}
	// Per-operation deadlines are set by the caller (Sign) or left unset
	// (AwaitPushedKey's idle-between-pushes read); clear the handshake-only
	// one now that it succeeded.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		c.recordDialFailureLocked()
		return fmt.Errorf("kesagent: clear hello deadline: %w", err)
	}

	c.conn = conn
	c.cfg.Metrics.setConnected(true)
	return nil
}

func (c *Client) recordDialFailureLocked() {
	if c.backoff == 0 {
		c.backoff = minReconnectBackoff
	} else {
		c.backoff *= 2
		if c.backoff > maxReconnectBackoff {
			c.backoff = maxReconnectBackoff
		}
	}
	c.nextDialAfter = time.Now().Add(c.backoff)
	c.cfg.Metrics.incReconnectFailures()
}

func (c *Client) currentBackoff() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.backoff == 0 {
		return minReconnectBackoff
	}
	return c.backoff
}

func (c *Client) resetBackoff() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetBackoffLocked()
}

// resetBackoffLocked is resetBackoff for a caller that already holds c.mu,
// which Sign does for the whole of its request/response exchange.
func (c *Client) resetBackoffLocked() {
	c.backoff = 0
	c.nextDialAfter = time.Time{}
}

// PushedKey is validated serve-key material ready to install into
// ledger/forging.PoolCredentials via LoadFromAgentServeKey.
type PushedKey struct {
	// AbsolutePeriod is the KES period the pushed secret key is already
	// evolved to.
	AbsolutePeriod uint64
	// KESSKeyData is the raw KES secret key bytes at AbsolutePeriod.
	KESSKeyData []byte
	// KESVKey is the 32-byte KES verification key matching KESSKeyData.
	KESVKey []byte
	// OpCert is the decoded operational certificate served alongside the
	// key.
	OpCert *bursa.DecodedOpCert
}

// Wipe zeroes the KES signing key bytes this PushedKey carries. Callers own
// the material AwaitPushedKey hands them and call this once they have
// installed it: ledger/forging copies the bytes it keeps, so the copy here is
// dead the moment the install returns and should not be left lying in the
// heap. It is safe to call more than once and on a zero PushedKey.
func (pk *PushedKey) Wipe() {
	wipeBytes(pk.KESSKeyData)
}

// AwaitPushedKey blocks until the agent delivers the next serve-key push (or
// ctx is done), validating it before returning. It performs no reconnect
// looping itself; use Run to survive a mid-stream disconnect.
//
// Unlike Sign, AwaitPushedKey does not hold c.mu across its network read:
// the read is unbounded whenever ctx carries no deadline (the idle
// wait-for-the-next-push case Run's loop lives in almost all the time), and
// holding the lock across an unbounded read would make Close unable to ever
// acquire it to interrupt that same read -- see Close's doc comment. Only
// one goroutine should call AwaitPushedKey on a given Client at a time (Run
// already guarantees this); concurrent callers would race on the same
// underlying connection.
func (c *Client) AwaitPushedKey(ctx context.Context) (PushedKey, error) {
	c.mu.Lock()
	if c.cfg.Mode != ModeServeKey {
		c.mu.Unlock()
		return PushedKey{}, fmt.Errorf(
			"kesagent: AwaitPushedKey called on a client configured for %q",
			c.cfg.Mode,
		)
	}
	if err := c.connectLocked(ctx); err != nil {
		c.mu.Unlock()
		return PushedKey{}, err
	}
	conn := c.conn
	c.mu.Unlock()

	// The watcher exists to unblock a read that ctx cancelled, so its
	// invalidation and this call's own settlement have to be mutually
	// exclusive. Signalling completion by closing readDone alone is not
	// enough: a select whose cases are both ready picks uniformly at random,
	// and the goroutine below is routinely still unscheduled when the caller
	// cancels a startup context it has already bounded successfully. The
	// watcher then tears down a healthy connection roughly half the time,
	// and the caller's next AwaitPushedKey has to redial to find the push
	// the agent had already queued on the connection it lost.
	//
	// settle names the winner once. After the read has returned a value the
	// connection belongs to the Client and not to the call that dialed it,
	// so a later cancellation of that call's context must leave it alone.
	var (
		settleMu sync.Mutex
		settled  bool
	)
	settle := func() bool {
		settleMu.Lock()
		defer settleMu.Unlock()
		if settled {
			return false
		}
		settled = true
		return true
	}
	readDone := make(chan struct{})
	defer func() {
		settle()
		close(readDone)
	}()
	go func() {
		select {
		case <-ctx.Done():
			if settle() {
				c.invalidateConn(conn)
			}
		case <-readDone:
		}
	}()

	// The header wait is deliberately left unbounded unless ctx bounds it:
	// idling between pushes with nothing to read is the normal state of a
	// serve-key subscriber, not a fault.
	var ctxDeadline time.Time
	if deadline, ok := ctx.Deadline(); ok {
		ctxDeadline = deadline
		if err := conn.SetReadDeadline(deadline); err != nil {
			c.invalidateConn(conn)
			return PushedKey{}, fmt.Errorf(
				"kesagent: set read deadline: %w",
				err,
			)
		}
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	// Once the peer has declared a length, though, the body has to arrive.
	// Never past a deadline ctx already imposed: this bounds the body read,
	// it does not extend the caller's own bound.
	bodyGuard := func() error {
		deadline := time.Now().Add(c.cfg.FrameBodyTimeout)
		if !ctxDeadline.IsZero() && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
		return conn.SetReadDeadline(deadline)
	}

	var push KeyPush
	if err := readFrameGuarded(
		conn,
		MaxKeyPushFrameLen,
		bodyGuard,
		&push,
	); err != nil {
		c.invalidateConn(conn)
		return PushedKey{}, fmt.Errorf("kesagent: read key push: %w", err)
	}
	pk, err := validateKeyPush(push)
	if err != nil {
		c.invalidateConn(conn)
		return PushedKey{}, err
	}
	// Settled here rather than left to the deferred settle: the value is in
	// hand, so the connection stops being this call's to lose from this
	// point, not from wherever the deferred call happens to run.
	settle()
	return pk, nil
}

// validateKeyPush validates every field of a KeyPush before it becomes a
// PushedKey, including a self-sign probe proving the secret key, its
// verification key, its declared period, and the operational certificate are
// all mutually consistent -- not merely present (P1: "pushed key material
// accepted without validation").
func validateKeyPush(push KeyPush) (PushedKey, error) {
	// push is decoded for this call and discarded by the caller, and the
	// probe below needs its own evolvable copy. Both hold the raw KES
	// signing key, so neither outlives the validation.
	defer wipeBytes(push.KESSignKey)
	if push.Type != "key_push" {
		return PushedKey{}, fmt.Errorf(
			"kesagent: expected key_push frame, got type %q",
			push.Type,
		)
	}
	if push.Depth != kes.CardanoKesDepth {
		return PushedKey{}, fmt.Errorf(
			"kesagent: unsupported KES depth %d (want %d)",
			push.Depth,
			kes.CardanoKesDepth,
		)
	}
	if len(push.KESSignKey) != kes.CardanoKesSecretKeySize {
		return PushedKey{}, fmt.Errorf(
			"kesagent: pushed KES secret key is %d bytes, want %d",
			len(push.KESSignKey),
			kes.CardanoKesSecretKeySize,
		)
	}
	if len(push.KESVKey) != ed25519.PublicKeySize {
		return PushedKey{}, fmt.Errorf(
			"kesagent: pushed KES verification key is %d bytes, want %d",
			len(push.KESVKey),
			ed25519.PublicKeySize,
		)
	}
	if len(push.OpCert) == 0 {
		return PushedKey{}, errors.New(
			"kesagent: key push carried no operational certificate",
		)
	}
	opCert, err := bursa.DecodeOpCert(push.OpCert)
	if err != nil {
		return PushedKey{}, fmt.Errorf(
			"kesagent: decode pushed opcert: %w",
			err,
		)
	}
	if !bytes.Equal(opCert.KESVKey, push.KESVKey) {
		return PushedKey{}, errors.New(
			"kesagent: pushed opcert KES verification key does not match pushed KES verification key",
		)
	}
	if push.Period < opCert.KESPeriod {
		return PushedKey{}, fmt.Errorf(
			"kesagent: pushed key period %d precedes opcert start period %d",
			push.Period,
			opCert.KESPeriod,
		)
	}

	// Self-sign probe: prove the pushed secret key actually produces
	// signatures the pushed verification key accepts, at the pushed period,
	// rather than trusting the agent's report that they correspond. A wrong
	// or corrupted push must never be installed as if it were valid.
	relativePeriod := push.Period - opCert.KESPeriod
	probe := &kes.SecretKey{
		Depth:  kes.CardanoKesDepth,
		Period: relativePeriod,
		Data:   append([]byte(nil), push.KESSignKey...),
	}
	defer wipeBytes(probe.Data)
	sig, err := kes.Sign(probe, relativePeriod, []byte(kesPushProbeMessage))
	if err != nil {
		return PushedKey{}, fmt.Errorf(
			"kesagent: pushed key failed self-sign probe: %w",
			err,
		)
	}
	if !kes.VerifySignedKES(
		push.KESVKey,
		relativePeriod,
		[]byte(kesPushProbeMessage),
		sig,
	) {
		return PushedKey{}, errors.New(
			"kesagent: pushed KES secret key does not match its own pushed verification key",
		)
	}

	return PushedKey{
		AbsolutePeriod: push.Period,
		KESSKeyData:    append([]byte(nil), push.KESSignKey...),
		KESVKey:        append([]byte(nil), push.KESVKey...),
		OpCert:         opCert,
	}, nil
}

// Run runs the serve-key subscription loop until ctx is done, calling install
// for every validated key push and reconnecting with backoff on any
// connection error. install's error, if any, is logged and does not stop the
// loop -- a transient downstream failure should not tear down the agent
// connection -- but it also does not reset the reconnect backoff: only an
// install that actually succeeds does, so a connection that completes Hello
// but never yields an installable key is still treated as unhealthy rather
// than retried at full speed.
func (c *Client) Run(
	ctx context.Context,
	install func(PushedKey) error,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pk, err := c.AwaitPushedKey(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			c.logger.Warn(
				"kes agent connection error, retrying",
				"error", err,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.currentBackoff()):
			}
			continue
		}
		err = install(pk)
		pk.Wipe()
		if err != nil {
			c.logger.Error(
				"failed to install agent-pushed KES key",
				"error", err,
			)
			c.invalidateCurrentConn()
			continue
		}
		c.resetBackoff()
	}
}

func (c *Client) invalidateCurrentConn() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		c.invalidateConn(conn)
	}
}

// Sign implements ledger/forging.RemoteKESSigner: it sends a sign-mode
// request for message at the given ABSOLUTE KES period (matching the bursa
// wire protocol, which also takes an absolute period and translates
// internally) and returns the agent's signature only after validating the
// response's type, its echoed period, and the signature itself against
// Config.KESVKey (P1: "sign response accepted without validating its type or
// KES period" -- a cryptographic verification subsumes both).
func (c *Client) Sign(period uint64, message []byte) ([]byte, error) {
	if len(message) > MaxSignFrameLen/2 {
		return nil, fmt.Errorf(
			"kesagent: sign message is %d bytes, too large for a sign request",
			len(message),
		)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cfg.Mode != ModeSign {
		return nil, fmt.Errorf(
			"kesagent: Sign called on a client configured for %q",
			c.cfg.Mode,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.SignTimeout)
	defer cancel()
	if err := c.connectLocked(ctx); err != nil {
		return nil, err
	}
	if err := c.conn.SetDeadline(time.Now().Add(c.cfg.SignTimeout)); err != nil {
		_ = c.closeLocked()
		c.recordDialFailureLocked()
		return nil, fmt.Errorf("kesagent: set sign deadline: %w", err)
	}
	defer func() {
		if c.conn != nil {
			_ = c.conn.SetDeadline(time.Time{})
		}
	}()

	start := time.Now()
	req := SignRequest{Type: "sign_request", Period: period, Message: message}
	if err := writeFrame(c.conn, MaxSignFrameLen, req); err != nil {
		_ = c.closeLocked()
		c.recordDialFailureLocked()
		c.cfg.Metrics.incSignFailure()
		return nil, fmt.Errorf("kesagent: send sign request: %w", err)
	}
	var resp SignResponse
	if err := readFrame(c.conn, MaxSignFrameLen, &resp); err != nil {
		_ = c.closeLocked()
		c.recordDialFailureLocked()
		c.cfg.Metrics.incSignFailure()
		return nil, fmt.Errorf("kesagent: read sign response: %w", err)
	}
	if resp.Type != "sign_response" {
		_ = c.closeLocked()
		c.recordDialFailureLocked()
		c.cfg.Metrics.incSignFailure()
		return nil, fmt.Errorf(
			"kesagent: expected sign_response frame, got type %q",
			resp.Type,
		)
	}
	if resp.Period != period {
		_ = c.closeLocked()
		c.recordDialFailureLocked()
		c.cfg.Metrics.incSignFailure()
		return nil, fmt.Errorf(
			"kesagent: sign response period %d does not match requested period %d",
			resp.Period,
			period,
		)
	}
	if resp.Error != "" {
		c.cfg.Metrics.incSignFailure()
		return nil, fmt.Errorf(
			"kesagent: agent refused to sign: %s",
			resp.Error,
		)
	}
	if len(resp.Signature) != kes.CardanoKesSignatureSize {
		_ = c.closeLocked()
		c.recordDialFailureLocked()
		c.cfg.Metrics.incSignFailure()
		return nil, fmt.Errorf(
			"kesagent: sign response signature is %d bytes, want %d",
			len(resp.Signature),
			kes.CardanoKesSignatureSize,
		)
	}
	if period < c.cfg.OpCertStartPeriod {
		_ = c.closeLocked()
		c.recordDialFailureLocked()
		c.cfg.Metrics.incSignFailure()
		return nil, fmt.Errorf(
			"kesagent: sign period %d precedes configured opcert start period %d",
			period,
			c.cfg.OpCertStartPeriod,
		)
	}
	relativePeriod := period - c.cfg.OpCertStartPeriod
	if !kes.VerifySignedKES(
		c.cfg.KESVKey,
		relativePeriod,
		message,
		resp.Signature,
	) {
		_ = c.closeLocked()
		c.recordDialFailureLocked()
		c.cfg.Metrics.incSignFailure()
		return nil, errors.New(
			"kesagent: sign response failed KES signature verification",
		)
	}

	// A verified signature is the sign-mode equivalent of Run's completed
	// install: the only outcome that proves the agent is actually usable, and
	// therefore the only one that clears the backoff its failures accumulated.
	c.resetBackoffLocked()
	c.cfg.Metrics.observeSignLatency(time.Since(start))
	c.cfg.Metrics.incSignSuccess()
	return resp.Signature, nil
}
