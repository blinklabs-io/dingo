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
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/prometheus/client_golang/prometheus"
	prometheustest "github.com/prometheus/client_golang/prometheus/testutil"
)

// clampedReadDeadlineConn shortens long read deadlines so the serve-key body
// timeout can be exercised without making the test wait for the production
// ten-second bound. A caller deadline already tighter than max is preserved.
type clampedReadDeadlineConn struct {
	net.Conn
	max       time.Duration
	readBytes atomic.Int64
	events    chan<- readDeadlineEvent
}

type readDeadlineEvent struct {
	deadline  time.Time
	readBytes int64
}

func (c *clampedReadDeadlineConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.readBytes.Add(int64(n))
	return n, err
}

func (c *clampedReadDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if c.events != nil {
		c.events <- readDeadlineEvent{
			deadline:  deadline,
			readBytes: c.readBytes.Load(),
		}
	}
	if !deadline.IsZero() {
		maxDeadline := time.Now().Add(c.max)
		if deadline.After(maxDeadline) {
			deadline = maxDeadline
		}
	}
	return c.Conn.SetReadDeadline(deadline)
}

// --- helpers ------------------------------------------------------------

// capturedLog records what an operator would actually see: records at or above
// the handler's level, and nothing below it.
type capturedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func newCapturedLog(level slog.Level) (*slog.Logger, *capturedLog) {
	c := &capturedLog{}
	handler := slog.NewTextHandler(
		&syncWriter{mu: &c.mu, w: &c.buf},
		&slog.HandlerOptions{Level: level},
	)
	return slog.New(handler), c
}

func (l *capturedLog) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *capturedLog) count(substr string) int {
	return strings.Count(l.text(), substr)
}

// startRawAgent accepts connections and hands them to handler with no
// handshake, so a test can withhold or corrupt the Hello frame.
func startRawAgent(t *testing.T, handler func(conn net.Conn)) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "raw-agent.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer func() { _ = conn.Close() }()
				handler(conn)
			})
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return sock
}

// missingSocket returns a path inside a temp dir where nothing is listening —
// the shape of a typo in --shelley-kes-agent-socket.
func missingSocket(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "not-a-real-agent.sock")
}

// --- connect-failure visibility -----------------------------------------

// TestConnectFailureIsLoggedAtWarn pins that a socket path nothing is listening
// on is visible to an operator.
//
// At Debug it was not: 1.5 seconds of failed dials produced zero records at
// Info or above, while the node logged "KES signing key sourced from agent" and
// "block forger started in production mode". The node reported a healthy
// producer, forged nothing, and the first symptom was a lost slot — hours away
// on a real pool. The handler here is levelled at Info precisely so a Debug
// record cannot satisfy it.
func TestConnectFailureIsLoggedAtWarn(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	sock := missingSocket(t)
	logger, log := newCapturedLog(slog.LevelInfo)

	client, err := New(Config{
		SocketPath:   sock,
		Mode:         ModeServeKey,
		OpCert:       opcert,
		Logger:       logger,
		MinReconnect: 10 * time.Millisecond,
		MaxReconnect: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	waitFor(t, 2*time.Second, func() bool {
		return log.count("could not connect to KES agent") >= 1
	})
	// The record has to name the path, or the operator cannot tell which of
	// the configured paths is wrong.
	if !strings.Contains(log.text(), sock) {
		t.Fatalf(
			"connect failure did not name the socket path; log was:\n%s",
			log.text(),
		)
	}
}

// TestConnectFailureLoggingIsThrottled pins that making the failure visible did
// not make it a flood. A node left running against a stopped agent would
// otherwise emit a Warn per reconnect for as long as the outage lasts.
func TestConnectFailureLoggingIsThrottled(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	logger, log := newCapturedLog(slog.LevelInfo)
	reg := prometheus.NewRegistry()

	client, err := New(Config{
		SocketPath:   missingSocket(t),
		Mode:         ModeServeKey,
		OpCert:       opcert,
		Logger:       logger,
		MinReconnect: time.Millisecond,
		MaxReconnect: time.Millisecond,
		PromRegistry: reg,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	const minAttempts = 60
	waitFor(t, 5*time.Second, func() bool {
		return prometheustest.ToFloat64(
			client.metrics.connectFailures,
		) >= minAttempts
	})
	// Read the log first, then the attempt count: the bound then compares a
	// count taken at t1 against attempts at t2 >= t1, which can only make the
	// bound looser, never spuriously tight.
	warns := log.count("could not connect to KES agent")
	attempts := prometheustest.ToFloat64(client.metrics.connectFailures)
	limit := int(attempts)/connectWarnRepeat + 2
	if warns > limit {
		t.Fatalf(
			"%d Warn records for %.0f failed attempts; at most %d expected with a repeat of %d",
			warns,
			attempts,
			limit,
			connectWarnRepeat,
		)
	}
	if warns == 0 {
		t.Fatal("no connect failure was reported at Warn")
	}
}

func TestServeKeyReconnectsWhenFrameBodyStalls(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	var (
		attempts   atomic.Int32
		servers    sync.WaitGroup
		secondOnce sync.Once
	)
	partialSent := make(chan struct{})
	secondDial := make(chan struct{})
	release := make(chan struct{})
	deadlineEvents := make(chan readDeadlineEvent, 4)

	dial := func(context.Context) (net.Conn, error) {
		clientConn, serverConn := net.Pipe()
		attempt := attempts.Add(1)
		servers.Go(func() {
			defer func() { _ = serverConn.Close() }()
			if err := writeFrame(serverConn, Hello{
				Protocol: ProtocolID,
				Mode:     ModeServeKey,
			}); err != nil {
				return
			}
			if attempt == 1 {
				// Declare a two-byte body but send only its first byte. The peer
				// remains connected, so only the body deadline can release the
				// client's registered read and allow another dial.
				partial := frameBytes([]byte("{}"))[:5]
				if _, err := serverConn.Write(partial); err != nil {
					return
				}
				close(partialSent)
			} else {
				secondOnce.Do(func() { close(secondDial) })
			}
			<-release
		})
		wrapped := &clampedReadDeadlineConn{
			Conn: clientConn,
			max:  25 * time.Millisecond,
		}
		if attempt == 1 {
			wrapped.events = deadlineEvents
		}
		return wrapped, nil
	}

	client, err := New(Config{
		Mode:         ModeServeKey,
		OpCert:       opcert,
		Dial:         dial,
		MinReconnect: time.Millisecond,
		MaxReconnect: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
		close(release)
		servers.Wait()
	})
	client.Start(t.Context())

	select {
	case <-partialSent:
	case <-time.After(time.Second):
		t.Fatal("fake agent did not send the partial serve-key frame")
	}
	select {
	case <-secondDial:
	case <-time.After(500 * time.Millisecond):
		t.Fatal(
			"serve-key client did not reconnect after a declared frame body stalled",
		)
	}

	events := make([]readDeadlineEvent, 0, cap(deadlineEvents))
	for range cap(deadlineEvents) {
		select {
		case event := <-deadlineEvents:
			events = append(events, event)
		case <-time.After(time.Second):
			t.Fatalf(
				"first connection recorded %d read-deadline changes, want 4: %+v",
				len(events),
				events,
			)
		}
	}
	if events[0].deadline.IsZero() || events[0].readBytes != 0 {
		t.Fatalf(
			"handshake deadline was not installed before reading: %+v",
			events,
		)
	}
	if !events[1].deadline.IsZero() || events[1].readBytes <= 4 {
		t.Fatalf("handshake deadline was not cleared after Hello: %+v", events)
	}
	if events[2].deadline.IsZero() ||
		events[2].readBytes != events[1].readBytes+4 {
		t.Fatalf(
			"serve-key body deadline did not start after its four-byte header: %+v",
			events,
		)
	}
	if !events[3].deadline.IsZero() ||
		events[3].readBytes != events[2].readBytes+1 {
		t.Fatalf(
			"serve-key body deadline was not cleared after the partial-body timeout: %+v",
			events,
		)
	}
}

// TestServeKeyMetricsTrackConnectionAndKey pins the gauges an operator alerts
// on. Client.HasKey and CurrentPeriod existed but nothing exported them, so a
// node connected to no agent and holding no key was indistinguishable from a
// healthy one in metrics.
func TestServeKeyMetricsTrackConnectionAndKey(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)
	const pushPeriod = uint64(3)

	dropSession := make(chan struct{})
	agent := startFakeAgent(t, ModeServeKey, func(index int, conn net.Conn) {
		if index > 0 {
			// Later sessions hold without pushing, so the assertions below
			// cannot be satisfied by a reconnect.
			<-dropSession
			return
		}
		evolved, err := evolveClone(master, pushPeriod-start)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     pushPeriod,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: evolved.Data,
			KESVKey:    vkey,
		})
		<-dropSession
	})

	reg := prometheus.NewRegistry()
	client, err := New(Config{
		SocketPath: agent.socket(),
		Mode:       ModeServeKey,
		OpCert:     opcert,
		// Long enough that the gauge can be observed at 0 after the drop,
		// before the client reconnects.
		MinReconnect: 5 * time.Second,
		MaxReconnect: 5 * time.Second,
		PromRegistry: reg,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	waitFor(t, 2*time.Second, func() bool {
		return prometheustest.ToFloat64(client.metrics.keyPresent) == 1
	})
	if got := prometheustest.ToFloat64(client.metrics.connected); got != 1 {
		t.Fatalf("connected gauge is %v while a session is up", got)
	}
	if got := prometheustest.ToFloat64(client.metrics.keyPeriod); got != float64(
		pushPeriod,
	) {
		t.Fatalf("key period gauge is %v, want %d", got, pushPeriod)
	}

	// Dropping the session must clear the connection gauge while leaving the
	// key gauge set: the client keeps forging on the last received key, which
	// is the documented behaviour, so conflating the two would hide an agent
	// outage or invent a key outage.
	close(dropSession)
	waitFor(t, 3*time.Second, func() bool {
		return prometheustest.ToFloat64(client.metrics.connected) == 0
	})
	if got := prometheustest.ToFloat64(client.metrics.keyPresent); got != 1 {
		t.Fatalf("key gauge cleared on a dropped session: %v", got)
	}

	client.Close()
	if got := prometheustest.ToFloat64(client.metrics.keyPresent); got != 0 {
		t.Fatalf("key gauge still set after Close: %v", got)
	}
}

// --- sign-mode connection lifecycle -------------------------------------

// TestSignModeReconnectsWhileIdle pins that an idle-closed connection is
// noticed with nothing waiting on it.
//
// runSign used to park in a select on ctx.Done and reqCh, never reading the
// socket, so a FIN from the agent went unseen until the next request's write
// failed. Slot wins are hours apart on a real pool, so any agent idle timeout,
// restart, or socket recycle in between meant the next won slot was spent
// discovering a dead connection. No KESSign is issued here: the reconnects have
// to come from the pump itself.
func TestSignModeReconnectsWhileIdle(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)

	const minReconnect = 50 * time.Millisecond
	var sessions sessionLog
	agent := startFakeAgent(t, ModeSign, func(_ int, conn net.Conn) {
		sessions.mark()
		// The handshake completes in startFakeAgent; close straight after, the
		// way an idle timeout or a restarting agent looks from here.
		_ = conn.Close()
	})

	client, err := New(Config{
		SocketPath:   agent.socket(),
		Mode:         ModeSign,
		OpCert:       opcert,
		MinReconnect: minReconnect,
		MaxReconnect: time.Second,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	waitFor(t, 5*time.Second, func() bool { return sessions.count() >= 4 })
	if got := agent.conns.Load(); got < 4 {
		t.Fatalf("expected at least 4 sessions, got %d", got)
	}
	// And the reconnects are paced. A session that served no request is
	// unproductive, so it has to back off exactly like a failed dial;
	// reconnecting at full speed is what the serve-key path was fixed for.
	for i, gap := range sessions.intervals() {
		if gap < minReconnect {
			t.Fatalf(
				"reconnect %d came %v after the previous session; the floor is %v (all intervals: %v)",
				i+1,
				gap,
				minReconnect,
				sessions.intervals(),
			)
		}
	}
}

// TestSignModeSignsAfterAgentClosesBetweenRequests is the end-to-end guarantee
// for the lost block: an agent that answers one request and closes must not cost
// the next one.
//
// Reproduced before the fix as: first sign succeeds, second fails with
// "write: broken pipe", third succeeds. On a real pool those are three
// different slot wins, so the middle one is a forfeited block.
func TestSignModeSignsAfterAgentClosesBetweenRequests(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	agent := startFakeAgent(t, ModeSign, func(_ int, conn net.Conn) {
		// Answer exactly one request per connection, then hang up.
		var req SignRequest
		if err := readFrame(conn, &req); err != nil {
			return
		}
		rel := req.Period - start
		sk, err := evolveClone(master, rel)
		if err != nil {
			return
		}
		sig, err := kes.Sign(sk, rel, req.Message)
		if err != nil {
			return
		}
		_ = writeFrame(conn, SignResponse{
			Type:      SignResponseType,
			Period:    req.Period,
			Signature: sig,
		})
		_ = conn.Close()
	})

	client, err := New(Config{
		SocketPath:   agent.socket(),
		Mode:         ModeSign,
		OpCert:       opcert,
		MinReconnect: 10 * time.Millisecond,
		SignTimeout:  900 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	const period = uint64(4)
	for i := range 3 {
		msg := []byte("header body for slot win")
		sig, err := client.KESSign(period, msg)
		if err != nil {
			t.Fatalf("sign %d after an agent-closed connection: %v", i+1, err)
		}
		if !kes.VerifySignedKES(vkey, period-start, msg, sig) {
			t.Fatalf("signature %d did not verify", i+1)
		}
	}
}

// TestSignModeDoesNotRetryAgentError pins the other half of the retry rule. An
// error the agent replied with — an exhausted key, a refused period — is not a
// transport fault, so asking again gets the same answer and burns the slot's
// remaining time.
func TestSignModeDoesNotRetryAgentError(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)

	var requests atomic.Int32
	agent := startFakeAgent(t, ModeSign, func(_ int, conn net.Conn) {
		for {
			var req SignRequest
			if err := readFrame(conn, &req); err != nil {
				return
			}
			requests.Add(1)
			_ = writeFrame(conn, SignResponse{
				Type:   SignResponseType,
				Period: req.Period,
				Error:  "KES key exhausted",
			})
		}
	})

	client, err := New(Config{
		SocketPath:  agent.socket(),
		Mode:        ModeSign,
		OpCert:      opcert,
		SignTimeout: 900 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	_, err = client.KESSign(2, []byte("message"))
	if !errors.Is(err, ErrAgentSign) {
		t.Fatalf("expected an agent sign error, got %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf(
			"agent-level error was retried: %d requests reached the agent",
			got,
		)
	}
}

// TestDefaultSignTimeoutIsBelowASlot pins the value. A mainnet slot is one
// second, and checkAndForgeProduction calls the signer synchronously on the
// slot-aligned loop while ignoring its context, so the previous 5s default
// parked block production for about five slots whenever the agent stopped
// answering.
func TestDefaultSignTimeoutIsBelowASlot(t *testing.T) {
	const mainnetSlot = time.Second
	if defaultSignTimeout >= mainnetSlot {
		t.Fatalf(
			"default sign timeout %v is not below a mainnet slot (%v)",
			defaultSignTimeout, mainnetSlot,
		)
	}
}

func TestNewValidatesSignTimeoutBounds(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	const mainnetSlot = time.Second
	for _, timeout := range []time.Duration{
		-time.Nanosecond,
		mainnetSlot,
		mainnetSlot + time.Nanosecond,
	} {
		t.Run(timeout.String(), func(t *testing.T) {
			_, err := New(Config{
				SocketPath:  "/run/kes-agent.sock",
				Mode:        ModeSign,
				OpCert:      opcert,
				SignTimeout: timeout,
			})
			if err == nil {
				t.Fatalf("New accepted out-of-range sign timeout %s", timeout)
			}
			if !strings.Contains(err.Error(), "sign timeout") {
				t.Fatalf(
					"unexpected error for sign timeout %s: %v",
					timeout,
					err,
				)
			}
		})
	}

	for _, timeout := range []time.Duration{
		0,
		time.Nanosecond,
		mainnetSlot - time.Nanosecond,
	} {
		t.Run("valid_"+timeout.String(), func(t *testing.T) {
			client, err := New(Config{
				SocketPath:  "/run/kes-agent.sock",
				Mode:        ModeSign,
				OpCert:      opcert,
				SignTimeout: timeout,
			})
			if err != nil {
				t.Fatalf("New rejected valid sign timeout %s: %v", timeout, err)
			}
			if timeout == 0 && client.cfg.SignTimeout != defaultSignTimeout {
				t.Fatalf(
					"zero sign timeout resolved to %s, want default %s",
					client.cfg.SignTimeout,
					defaultSignTimeout,
				)
			}
		})
	}
}

// TestSignTimeoutIsConfigurable pins that Config.SignTimeout actually bounds a
// round trip, against an agent that accepts a request and never replies.
func TestSignTimeoutIsConfigurable(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)

	stalled := make(chan struct{})
	agent := startFakeAgent(t, ModeSign, func(_ int, conn net.Conn) {
		var req SignRequest
		if err := readFrame(conn, &req); err != nil {
			return
		}
		<-stalled
	})
	// Registered after the agent so it runs before the agent's cleanup waits
	// for its handlers; the reverse order deadlocks.
	t.Cleanup(func() { close(stalled) })

	const signTimeout = 100 * time.Millisecond
	client, err := New(Config{
		SocketPath:  agent.socket(),
		Mode:        ModeSign,
		OpCert:      opcert,
		SignTimeout: signTimeout,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	began := time.Now()
	if _, err := client.KESSign(1, []byte("message")); err == nil {
		t.Fatal("expected a timeout error from a stalled agent")
	}
	// Generous against scheduling noise, but far below both the previous 5s
	// default and the 500ms default this configuration overrides.
	if elapsed := time.Since(began); elapsed > 400*time.Millisecond {
		t.Fatalf(
			"configured sign timeout of %v was not honoured: the call took %v",
			signTimeout, elapsed,
		)
	}
}

// --- rejection logging --------------------------------------------------

// TestRejectedPushLoggingIsBoundedWithinASession pins the intra-session bound.
// Each rejection path emitted a record with no limit: 5000 Error lines from one
// session in about two seconds, which buries every other producer log.
func TestRejectedPushLoggingIsBoundedWithinASession(t *testing.T) {
	const start = uint64(0)
	vkey, _, opcert := newTestKES(t, start)
	wrongVKey := bytes.Clone(vkey)
	wrongVKey[0] ^= 0xff

	const pushes = 200
	logger, log := newCapturedLog(slog.LevelInfo)
	reg := prometheus.NewRegistry()
	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		for range pushes {
			if err := writeFrame(conn, KeyPush{
				Type:       KeyPushType,
				Period:     start,
				Depth:      kes.CardanoKesDepth,
				KESSignKey: make([]byte, secretKeySize(kes.CardanoKesDepth)),
				KESVKey:    wrongVKey,
			}); err != nil {
				return
			}
		}
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(Config{
		SocketPath:   agent.socket(),
		Mode:         ModeServeKey,
		OpCert:       opcert,
		Logger:       logger,
		PromRegistry: reg,
		MinReconnect: time.Hour, // one session only
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// The counter is what proves the pushes were processed rather than the log
	// merely being empty because nothing arrived.
	waitFor(t, 5*time.Second, func() bool {
		return prometheustest.ToFloat64(client.metrics.rejectedPushes) >= pushes
	})
	records := log.count("does not match operational certificate")
	if records > 2 {
		t.Fatalf(
			"%d records logged for %d rejected pushes in one session",
			records, pushes,
		)
	}
	if records == 0 {
		t.Fatal("a rejected push was never reported")
	}
}

// TestRejectedPushReportsSuppressedCount pins that throttling hides the
// repetition and not the volume: the next record for a reason carries how many
// were suppressed behind it.
func TestRejectedPushReportsSuppressedCount(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	logger, log := newCapturedLog(slog.LevelInfo)
	client, err := New(Config{
		SocketPath: "/unused",
		Mode:       ModeServeKey,
		OpCert:     opcert,
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	const reason = "test rejection reason"
	client.rejectPush(t.Context(), slog.LevelError, reason)
	client.rejectPush(t.Context(), slog.LevelError, reason)
	client.rejectPush(t.Context(), slog.LevelError, reason)
	if got := log.count(reason); got != 1 {
		t.Fatalf("expected 1 record before the interval elapses, got %d", got)
	}

	// Age the last record past the interval rather than waiting it out.
	client.rejectMu.Lock()
	client.rejectLast[reason] = time.Now().Add(-2 * rejectLogInterval)
	client.rejectMu.Unlock()

	client.rejectPush(t.Context(), slog.LevelError, reason)
	if got := log.count(reason); got != 2 {
		t.Fatalf("expected a second record after the interval, got %d", got)
	}
	if !strings.Contains(log.text(), "suppressed_since_last_record=2") {
		t.Fatalf(
			"the suppressed count was not reported; log was:\n%s",
			log.text(),
		)
	}
}

// --- push validation ----------------------------------------------------

// TestServeKeyRejectsUntypedFrame covers a frame that declares no type at all.
// protocol.go documents every serve-key frame as carrying "type":"key_push",
// and a frame that omits it is not speaking this protocol, so nothing else in
// it should be trusted either.
//
// The rejected push claims the later period, so had it been installed the
// legitimate push behind it would have been refused as moving backward.
func TestServeKeyRejectsUntypedFrame(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	logger, log := newCapturedLog(slog.LevelInfo)
	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		// The real key, so only the missing type can reject it.
		untyped, err := evolveClone(master, 3)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Period:     3,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: untyped.Data,
			KESVKey:    vkey,
		})
		good, err := evolveClone(master, 1)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     1,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: good.Data,
			KESVKey:    vkey,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(Config{
		SocketPath: agent.socket(),
		Mode:       ModeServeKey,
		OpCert:     opcert,
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 1 },
	)
	// Assert the rejection reason, not just the outcome: this push carries a
	// valid key for this opcert, so nothing else in applyKeyPush would refuse
	// it and a weaker assertion would pass with the type rule removed.
	if !strings.Contains(log.text(), "unexpected frame type") {
		t.Fatalf(
			"an untyped frame was not refused on the type rule; log was:\n%s",
			log.text(),
		)
	}
}

// TestServeKeyLogsStalePush pins that a push moving the key backward is
// reported. It was silent while still counting the session as unproductive, so
// the backoff escalated to MaxReconnect with no explanation anywhere — an agent
// stuck on a period the node has evolved past looked like an agent that was
// simply not answering.
func TestServeKeyLogsStalePush(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	logger, log := newCapturedLog(slog.LevelInfo)
	releaseStale := make(chan struct{})
	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		for _, period := range []uint64{5, 2} {
			if period == 2 {
				<-releaseStale
			}
			key, err := evolveClone(master, period-start)
			if err != nil {
				return
			}
			if err := writeFrame(conn, KeyPush{
				Type:       KeyPushType,
				Period:     period,
				Depth:      kes.CardanoKesDepth,
				KESSignKey: key.Data,
				KESVKey:    vkey,
			}); err != nil {
				return
			}
		}
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(Config{
		SocketPath: agent.socket(),
		Mode:       ModeServeKey,
		OpCert:     opcert,
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	// Hand off rather than race: the stale push is only stale once period 5 is
	// installed.
	waitFor(
		t,
		2*time.Second,
		func() bool { return client.CurrentPeriod() == 5 },
	)
	close(releaseStale)

	waitFor(t, 2*time.Second, func() bool {
		return strings.Contains(log.text(), "behind the key already held")
	})
	if client.CurrentPeriod() != 5 {
		t.Fatalf(
			"stale push moved the held key to %d",
			client.CurrentPeriod(),
		)
	}
}

// TestCloseRacingInFlightPush covers the shutdown window: a push already
// decoded when Close wipes the key must not repopulate key material behind it.
func TestCloseRacingInFlightPush(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)
	client, err := New(Config{
		SocketPath: "/unused",
		Mode:       ModeServeKey,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	push := func() KeyPush {
		key, err := evolveClone(master, 1)
		if err != nil {
			t.Fatalf("evolve: %v", err)
		}
		return KeyPush{
			Type:       KeyPushType,
			Period:     1,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: key.Data,
			KESVKey:    vkey,
		}
	}

	// Deterministic half: after Close, a valid push must be refused.
	client.Close()
	if client.applyKeyPush(t.Context(), push()) {
		t.Fatal("a key push was installed after Close")
	}
	if client.HasKey() {
		t.Fatal("key material was repopulated after the shutdown wipe")
	}

	// Racing half: Close concurrent with pushes. Whatever the interleaving,
	// no key may be held once both have returned.
	racer, err := New(Config{
		SocketPath: "/unused",
		Mode:       ModeServeKey,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 50 {
			racer.applyKeyPush(t.Context(), push())
		}
	})
	wg.Go(racer.Close)
	wg.Wait()
	if racer.HasKey() {
		t.Fatal("key material survived a Close racing in-flight pushes")
	}
}

// --- handshake ----------------------------------------------------------

func TestDialTimesOutOnSilentAgent(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	blocked := make(chan struct{})
	// Accepts the connection and never sends Hello.
	sock := startRawAgent(t, func(net.Conn) { <-blocked })
	// Registered after the agent so it runs before the agent's cleanup waits
	// for its handlers; the reverse order deadlocks.
	t.Cleanup(func() { close(blocked) })

	client, err := New(Config{
		SocketPath:       sock,
		Mode:             ModeServeKey,
		OpCert:           opcert,
		HandshakeTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	began := time.Now()
	if _, err := client.dial(t.Context()); err == nil {
		t.Fatal("expected the handshake to time out")
	} else if !strings.Contains(err.Error(), "read hello") {
		t.Fatalf("expected a hello read error, got %v", err)
	}
	if elapsed := time.Since(began); elapsed > time.Second {
		t.Fatalf("handshake timeout was not honoured: dial took %v", elapsed)
	}
}

func TestDialRejectsWrongProtocol(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	sock := startRawAgent(t, func(conn net.Conn) {
		_ = writeFrame(conn, Hello{
			Protocol: "bursa-kes-agent/99",
			Mode:     ModeServeKey,
		})
	})

	client, err := New(Config{
		SocketPath: sock,
		Mode:       ModeServeKey,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = client.dial(t.Context())
	if err == nil || !strings.Contains(err.Error(), "unexpected protocol") {
		t.Fatalf("expected a protocol mismatch error, got %v", err)
	}
}

func TestDialRejectsModeMismatch(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	// The agent's socket serves sign mode; the node is configured for
	// serve-key. Proceeding would leave the node waiting for pushes that never
	// come from a socket that only answers requests.
	sock := startRawAgent(t, func(conn net.Conn) {
		_ = writeFrame(conn, Hello{Protocol: ProtocolID, Mode: ModeSign})
	})

	client, err := New(Config{
		SocketPath: sock,
		Mode:       ModeServeKey,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = client.dial(t.Context())
	if err == nil ||
		!strings.Contains(err.Error(), "does not match configured mode") {
		t.Fatalf("expected a mode mismatch error, got %v", err)
	}
}

// --- sign response validation -------------------------------------------

func TestSignRejectsMalformedResponses(t *testing.T) {
	tests := []struct {
		name    string
		reply   func(req SignRequest) SignResponse
		wantErr string
	}{
		{
			name: "wrong type",
			reply: func(req SignRequest) SignResponse {
				return SignResponse{
					Type:      "not_a_sign_response",
					Period:    req.Period,
					Signature: bytes.Repeat([]byte{0x01}, 448),
				}
			},
			wantErr: "unexpected response type",
		},
		{
			name: "wrong period",
			reply: func(req SignRequest) SignResponse {
				return SignResponse{
					Type:      SignResponseType,
					Period:    req.Period + 1,
					Signature: bytes.Repeat([]byte{0x01}, 448),
				}
			},
			wantErr: "does not match request period",
		},
		{
			name: "empty signature",
			reply: func(req SignRequest) SignResponse {
				return SignResponse{
					Type:   SignResponseType,
					Period: req.Period,
				}
			},
			wantErr: "empty signature",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, opcert := newTestKES(t, 0)
			agent := startFakeAgent(
				t,
				ModeSign,
				func(_ int, conn net.Conn) {
					for {
						var req SignRequest
						if err := readFrame(conn, &req); err != nil {
							return
						}
						if err := writeFrame(conn, tc.reply(req)); err != nil {
							return
						}
					}
				},
			)

			client, err := New(Config{
				SocketPath:   agent.socket(),
				Mode:         ModeSign,
				OpCert:       opcert,
				SignTimeout:  900 * time.Millisecond,
				MinReconnect: 10 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("new: %v", err)
			}
			client.Start(t.Context())

			_, err = client.KESSign(1, []byte("message"))
			if err == nil {
				t.Fatal("a malformed sign response was accepted")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf(
					"expected an error containing %q, got %v",
					tc.wantErr,
					err,
				)
			}
		})
	}
}

// --- startup readiness --------------------------------------------------

func TestWaitForReadyServeKeyWaitsForAKey(t *testing.T) {
	const start = uint64(0)
	vkey, master, opcert := newTestKES(t, start)

	release := make(chan struct{})
	agent := startFakeAgent(t, ModeServeKey, func(_ int, conn net.Conn) {
		// Connected but silent until released: a session alone is not
		// readiness in serve-key mode, only a validated key is.
		<-release
		key, err := evolveClone(master, 2)
		if err != nil {
			return
		}
		_ = writeFrame(conn, KeyPush{
			Type:       KeyPushType,
			Period:     2,
			Depth:      kes.CardanoKesDepth,
			KESSignKey: key.Data,
			KESVKey:    vkey,
		})
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	})

	client, err := New(Config{
		SocketPath: agent.socket(),
		Mode:       ModeServeKey,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	if err := client.WaitForReady(
		t.Context(),
		100*time.Millisecond,
	); !errors.Is(err, ErrNotReady) {
		t.Fatalf("expected ErrNotReady while no key has arrived, got %v", err)
	}
	close(release)
	if err := client.WaitForReady(t.Context(), 3*time.Second); err != nil {
		t.Fatalf("WaitForReady after a key push: %v", err)
	}
}

// TestWaitForReadySignConnectsWithoutARequest pins that sign mode reaches
// readiness on its own. The pump used to dial only when a request arrived, so
// nothing at startup could tell a good socket path from a typo.
func TestWaitForReadySignConnectsWithoutARequest(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	blocked := make(chan struct{})
	agent := startFakeAgent(t, ModeSign, func(_ int, conn net.Conn) {
		<-blocked
	})
	// Registered after the agent so it runs before the agent's cleanup waits
	// for its handlers; the reverse order deadlocks.
	t.Cleanup(func() { close(blocked) })

	client, err := New(Config{
		SocketPath: agent.socket(),
		Mode:       ModeSign,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())

	if err := client.WaitForReady(t.Context(), 3*time.Second); err != nil {
		t.Fatalf("WaitForReady with no sign request issued: %v", err)
	}
}

func TestWaitForReadyReportsAMissingSocket(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	client, err := New(Config{
		SocketPath:   missingSocket(t),
		Mode:         ModeServeKey,
		OpCert:       opcert,
		MinReconnect: 10 * time.Millisecond,
		MaxReconnect: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	client.Start(t.Context())
	if err := client.WaitForReady(
		t.Context(),
		200*time.Millisecond,
	); !errors.Is(err, ErrNotReady) {
		t.Fatalf("expected ErrNotReady for a socket with no agent, got %v", err)
	}
}

func TestWaitForReadyRejectsUnstartedAndClosedClients(t *testing.T) {
	_, _, opcert := newTestKES(t, 0)
	client, err := New(Config{
		SocketPath: "/unused",
		Mode:       ModeSign,
		OpCert:     opcert,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := client.WaitForReady(
		t.Context(),
		time.Millisecond,
	); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("expected ErrNotStarted, got %v", err)
	}
	client.Close()
	if err := client.WaitForReady(
		t.Context(),
		time.Millisecond,
	); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}
