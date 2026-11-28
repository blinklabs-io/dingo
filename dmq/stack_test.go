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

package dmq

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagenotification"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagesubmission"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const testDMQMagic = 2147483650

// soonExpiry is within the stack's default 30 minute message TTL.
func soonExpiry() uint32 {
	// #nosec G115 -- test fixture timestamp
	return uint32(time.Now().Add(10 * time.Minute).Unix())
}

func TestNewStackRejectsNonPermissionSocketModeBits(t *testing.T) {
	t.Parallel()

	_, err := NewStack(StackConfig{
		SocketPath:    "dmq.socket",
		SocketMode:    os.FileMode(0o1000),
		Authenticator: ocommon.NewNoOpAuthenticator(nil),
	})
	require.ErrorContains(t, err, "only Unix permission bits")
}

type noStake struct{}

func (noStake) PoolActiveStake(ocommon.PoolKeyHash) (uint64, error) {
	return 0, nil
}

func newTestStack(
	t *testing.T,
	cfg StackConfig,
) (*Stack, *prometheus.Registry) {
	t.Helper()
	dir, err := os.MkdirTemp("", "dmq")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	reg := prometheus.NewRegistry()
	cfg.PromRegistry = reg
	// The fixtures sign every message with one key; the pacing has tests of
	// its own.
	if cfg.MinPoolMessageInterval == 0 {
		cfg.MinPoolMessageInterval = -1
	}
	// The fixtures use short bodies; the size limits have tests of their own.
	cfg.SocketPath = filepath.Join(dir, "dmq.sock")
	cfg.NetworkMagic = testDMQMagic
	if cfg.Authenticator == nil {
		cfg.Authenticator = ocommon.NewNoOpAuthenticator(nil)
	}
	s, err := NewStack(cfg)
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		require.NoError(t, s.Stop(ctx))
	})
	return s, reg
}

// dialClient connects a local client that does no validation of its own, so
// the server's behaviour is what each test observes.
func dialClient(
	t *testing.T,
	s *Stack,
	sub localmessagesubmission.Config,
	notif localmessagenotification.Config,
) *ouroboros.Connection {
	t.Helper()
	sub.Authenticator = ocommon.NewNoOpAuthenticator(nil)
	sub.TTLValidator = ocommon.NewNoOpTTLValidator(nil)
	notif.Authenticator = ocommon.NewNoOpAuthenticator(nil)
	notif.TTLValidator = ocommon.NewNoOpTTLValidator(nil)
	conn, err := ouroboros.NewConnection(
		ouroboros.WithDMQ(true),
		ouroboros.WithNetworkMagic(testDMQMagic),
		ouroboros.WithLocalMessageSubmissionConfig(sub),
		ouroboros.WithLocalMessageNotificationConfig(notif),
	)
	require.NoError(t, err)
	require.NoError(t, conn.Dial("unix", s.cfg.SocketPath))
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// submitter returns a function that submits msg and returns the server's
// verdict: nil for accept, the reject reason otherwise.
func submitter(
	t *testing.T,
	s *Stack,
) func(ocommon.DmqMessage) ocommon.RejectReason {
	t.Helper()
	conn := dialClient(
		t,
		s,
		localmessagesubmission.NewConfig(),
		localmessagenotification.NewConfig(),
	)
	return func(msg ocommon.DmqMessage) ocommon.RejectReason {
		return submissionVerdict(
			t,
			conn.LocalMessageSubmission().Client.SubmitMessage(&msg),
		)
	}
}

// submissionVerdict turns SubmitMessage's result into the server's verdict:
// nil for accept, the reject reason otherwise.
func submissionVerdict(t *testing.T, err error) ocommon.RejectReason {
	t.Helper()
	if err == nil {
		return nil
	}
	var rejected localmessagesubmission.MessageRejectedError
	require.ErrorAs(t, err, &rejected)
	return rejected.Reason
}

// consumer returns blocking and non-blocking request functions that each
// return the bodies of the messages in the reply.
type consumer struct {
	conn    *ouroboros.Connection
	replies chan []ocommon.DmqMessage
}

func newConsumer(t *testing.T, s *Stack) *consumer {
	t.Helper()
	c := &consumer{replies: make(chan []ocommon.DmqMessage, 16)}
	c.conn = dialClient(
		t,
		s,
		localmessagesubmission.NewConfig(),
		localmessagenotification.NewConfig(
			localmessagenotification.WithReplyMessagesFunc(
				func(
					_ localmessagenotification.CallbackContext,
					msgs []ocommon.DmqMessage,
					_ bool,
				) {
					c.replies <- msgs
				},
			),
		),
	)
	return c
}

func (c *consumer) client() *localmessagenotification.Client {
	return c.conn.LocalMessageNotification().Client
}

func (c *consumer) reply(t *testing.T) []ocommon.DmqMessage {
	t.Helper()
	select {
	case msgs := <-c.replies:
		return msgs
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no reply from server")
		return nil
	}
}

func bodies(msgs []ocommon.DmqMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, strings.TrimRight(string(m.Payload.MessageBody), "."))
	}
	return out
}

// metricValue returns the value of the named counter or gauge, or of the one
// series carrying the given label value when label is non-empty.
func metricValue(
	t *testing.T,
	reg *prometheus.Registry,
	name, label string,
) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if label != "" &&
				(len(m.GetLabel()) != 1 || m.GetLabel()[0].GetValue() != label) {
				continue
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
			return m.GetGauge().GetValue()
		}
	}
	return 0
}

// unavailableStake answers like a node whose ledger cannot read the electing
// stake distribution.
type unavailableStake struct{}

func (unavailableStake) PoolActiveStake(ocommon.PoolKeyHash) (uint64, error) {
	return 0, fmt.Errorf("%w: snapshot not captured", ErrChainStateUnavailable)
}

func newNoStakeAuthenticator(t *testing.T) *ocommon.MessageAuthenticator {
	t.Helper()
	return newStakeAuthenticator(t, noStake{})
}

func newStakeAuthenticator(
	t *testing.T,
	stake ocommon.StakeAuthority,
) *ocommon.MessageAuthenticator {
	t.Helper()
	auth, err := ocommon.NewMessageAuthenticator(
		ocommon.MessageAuthenticatorConfig{
			// Tests send one pool's messages back to back.
			MinSigDelay:    -1,
			StakeAuthority: stake,
			CurrentSlot: func() (uint64, error) {
				return 0, nil
			},
			PoolOpCertIssueNumber: func(ocommon.PoolKeyHash) (uint64, bool, error) {
				return 0, false, nil
			},
		},
	)
	require.NoError(t, err)
	return auth
}

func TestStackSubmissionAdmitsToMempool(t *testing.T) {
	t.Parallel()
	s, reg := newTestStack(t, StackConfig{})
	submit := submitter(t, s)

	require.Nil(t, submit(newTestMessage(testBody("one"), soonExpiry())))
	require.Equal(t, 1, s.Mempool().Len())
	got, ok := s.Mempool().NextForPeer("probe")
	require.True(t, ok)
	require.Equal(t, testBody("one"), got.Payload.MessageBody)
	require.Equal(
		t,
		1.0,
		metricValue(t, reg, "dingo_dmq_messages_received_total", ""),
	)
	require.Equal(
		t,
		1.0,
		metricValue(t, reg, "dingo_dmq_mempool_messages", ""),
	)
}

func TestStackSubmissionRejectReasons(t *testing.T) {
	t.Parallel()
	// #nosec G115 -- test fixture timestamp
	soon := soonExpiry()
	tooFar := uint32(time.Now().Add(10 * time.Hour).Unix())
	cases := []struct {
		name  string
		cfg   StackConfig
		first *ocommon.DmqMessage // submitted and accepted beforehand
		// pooled is admitted to the pool directly, so the submission reaches
		// submit and the pool rather than the server's replay cache refuses it.
		pooled *ocommon.DmqMessage
		msg    ocommon.DmqMessage
		want   ocommon.RejectReason
		label  string
	}{
		{
			name:  "expired",
			msg:   newTestMessage(testBody("old"), 1),
			want:  ocommon.ExpiredReason{},
			label: reasonExpired,
		},
		{
			name: "duplicate",
			first: func() *ocommon.DmqMessage {
				m := newTestMessage(testBody("dup"), soon)
				return &m
			}(),
			msg:   newTestMessage(testBody("dup"), soon),
			want:  ocommon.AlreadyReceivedReason{},
			label: reasonAlreadyReceived,
		},
		{
			name: "duplicate in pool",
			pooled: func() *ocommon.DmqMessage {
				m := newTestMessage(testBody("pooled"), soon)
				return &m
			}(),
			msg:   newTestMessage(testBody("pooled"), soon),
			want:  ocommon.AlreadyReceivedReason{},
			label: reasonAlreadyReceived,
		},
		{
			name:  "unauthorized pool",
			cfg:   StackConfig{Authenticator: newNoStakeAuthenticator(t)},
			msg:   newTestMessage(testBody("anon"), soon),
			want:  ocommon.InvalidReason{},
			label: reasonInvalid,
		},
		{
			name: "stake distribution unavailable",
			cfg: StackConfig{
				Authenticator: newStakeAuthenticator(t, unavailableStake{}),
			},
			msg:   newTestMessage(testBody("unknown"), soon),
			want:  ocommon.OtherReason{},
			label: reasonOther,
		},
		{
			name:  "expiry beyond message ttl",
			msg:   newTestMessage(testBody("far"), tooFar),
			want:  ocommon.InvalidReason{},
			label: reasonInvalid,
		},
		{
			name: "future KES period",
			cfg: StackConfig{CurrentKESPeriod: func() (uint64, error) {
				return 0, nil
			}},
			msg:   newTestMessage(testBody("future"), soon),
			want:  ocommon.InvalidReason{},
			label: reasonInvalid,
		},
		{
			name: "operational certificate past its KES window",
			cfg: StackConfig{CurrentKESPeriod: func() (uint64, error) {
				return 1 + ocommon.DefaultMaxKESEvolutions, nil
			}},
			msg:   newTestMessage(testBody("stale"), soon),
			want:  ocommon.InvalidReason{},
			label: reasonInvalid,
		},
		{
			name:  "pool full",
			cfg:   StackConfig{MaxMempoolBytes: 1},
			msg:   newTestMessage(testBody("big"), soon),
			want:  ocommon.OtherReason{},
			label: reasonOther,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, reg := newTestStack(t, tc.cfg)
			submit := submitter(t, s)
			if tc.first != nil {
				require.Nil(t, submit(*tc.first))
			}
			if tc.pooled != nil {
				added, err := s.pool.Add(*tc.pooled)
				require.NoError(t, err)
				require.True(t, added)
			}
			reason := submit(tc.msg)
			require.IsType(t, tc.want, reason)
			if _, ok := reason.(ocommon.InvalidReason); ok {
				require.NotEmpty(t, reason.(ocommon.InvalidReason).Message)
			}
			require.Equal(
				t,
				1.0,
				metricValue(
					t,
					reg,
					"dingo_dmq_validation_failures_total",
					tc.label,
				),
			)
		})
	}
}

func TestStackNotificationNonBlocking(t *testing.T) {
	t.Parallel()
	s, reg := newTestStack(t, StackConfig{})
	submit := submitter(t, s)
	require.Nil(t, submit(newTestMessage(testBody("a"), soonExpiry())))
	require.Nil(t, submit(newTestMessage(testBody("b"), soonExpiry())))

	first := newConsumer(t, s)
	second := newConsumer(t, s)
	// The feeder runs asynchronously: poll until the first consumer's queue
	// holds both messages.
	var got []string
	require.Eventually(t, func() bool {
		require.NoError(t, first.client().RequestMessagesNonBlocking())
		got = append(got, bodies(first.reply(t))...)
		return len(got) == 2
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"a", "b"}, got)

	// Every message is delivered once per consumer, and only once.
	require.NoError(t, first.client().RequestMessagesNonBlocking())
	require.Empty(t, first.reply(t))
	var other []string
	require.Eventually(t, func() bool {
		require.NoError(t, second.client().RequestMessagesNonBlocking())
		other = append(other, bodies(second.reply(t))...)
		return len(other) == 2
	}, 10*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"a", "b"}, other)
	// Two messages queued for each of the three open connections.
	require.Eventually(t, func() bool {
		return metricValue(t, reg, "dingo_dmq_messages_sent_total", "") == 6
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return metricValue(t, reg, "dingo_dmq_connections", "") == 3
	}, 10*time.Second, 10*time.Millisecond)
}

func TestStackNotificationBlockingWaitsForMessage(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	c := newConsumer(t, s)
	require.NoError(t, c.client().RequestMessagesBlocking())
	select {
	case msgs := <-c.replies:
		require.FailNow(
			t,
			"blocking request answered with no message",
			"%v",
			msgs,
		)
	case <-time.After(200 * time.Millisecond):
	}

	submit := submitter(t, s)
	require.Nil(t, submit(newTestMessage(testBody("late"), soonExpiry())))
	require.Equal(t, []string{"late"}, bodies(c.reply(t)))
}

func TestStackNotificationDeliversEveryMessageOnce(t *testing.T) {
	t.Parallel()
	// More messages than a consumer's queue holds, so the feeder must hold
	// the refused ones back rather than drop them.
	total := localmessagenotification.NewConfig().MaxQueueSize + 20
	s, _ := newTestStack(t, StackConfig{})
	submit := submitter(t, s)
	for i := range total {
		require.Nil(
			t,
			submit(
				newTestMessage(
					[]byte(fmt.Sprintf("msg-%03d", i)),
					soonExpiry(),
				),
			),
		)
	}
	c := newConsumer(t, s)
	var got []string
	for len(got) < total {
		require.NoError(t, c.client().RequestMessagesBlocking())
		got = append(got, bodies(c.reply(t))...)
	}
	want := make([]string, 0, total)
	for i := range total {
		want = append(want, fmt.Sprintf("msg-%03d", i))
	}
	require.Equal(t, want, got)
}

// Local connections are node-to-client, which the connection manager reports
// only through ConnClosedFunc, never the bus's closed event. A disconnect must
// still stop the connection's feeder and release its pool cursor.
func TestStackReleasesClosedConnection(t *testing.T) {
	t.Parallel()
	s, reg := newTestStack(t, StackConfig{})
	c := newConsumer(t, s)
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.feeders) == 1
	}, 10*time.Second, 10*time.Millisecond)

	require.NoError(t, c.conn.Close(context.Background()))
	require.Eventually(t, func() bool {
		s.mu.Lock()
		feeders := len(s.feeders)
		s.mu.Unlock()
		s.pool.peersMu.Lock()
		cursors := len(s.pool.peers)
		s.pool.peersMu.Unlock()
		return feeders == 0 && cursors == 0 &&
			metricValue(t, reg, "dingo_dmq_connections", "") == 0
	}, 10*time.Second, 10*time.Millisecond)
}

func TestStackStartFailureStopsPool(t *testing.T) {
	t.Parallel()
	s, err := NewStack(StackConfig{
		SocketPath:    filepath.Join(t.TempDir(), "missing", "dmq.sock"),
		Authenticator: ocommon.NewNoOpAuthenticator(nil),
	})
	require.NoError(t, err)
	require.Error(t, s.Start(t.Context()))
	select {
	case <-s.pool.done:
	default:
		require.FailNow(t, "message pool expiry loop left running")
	}
}

func TestStackShutdownBoundsBlockedEventHandler(t *testing.T) {
	t.Parallel()

	s, err := NewStack(StackConfig{
		SocketPath:    shortSocketPath(t),
		NetworkMagic:  testDMQMagic,
		Authenticator: ocommon.NewNoOpAuthenticator(nil),
	})
	require.NoError(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	released := sync.OnceFunc(func() { close(release) })
	defer released()
	s.inboundSub = s.bus.SubscribeFuncWithBufferPolicy(
		connmanager.InboundConnectionEventType,
		1,
		event.SubscriberBackpressureBlock,
		func(event.Event) {
			close(entered)
			<-release
		},
	)
	s.bus.Publish(
		connmanager.InboundConnectionEventType,
		event.NewEvent(connmanager.InboundConnectionEventType, struct{}{}),
	)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("event handler did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- s.shutdownLocal(ctx) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(
			t,
			err,
			string(connmanager.InboundConnectionEventType),
		)
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown exceeded its bounded EventBus close")
	}

	released()
	require.NoError(t, s.bus.CloseContext(context.Background()))
}

// A full inbound queue must drain without losing the subscription, or no
// later connection would ever get a notification feeder.
func TestStackServesConnectionsAfterInboundStall(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	// Hold the feeder registry so the inbound handler stalls on the first
	// real connection.
	require.NoError(
		t,
		s.bus.UnsubscribeAndWaitContext(
			t.Context(),
			connmanager.InboundConnectionEventType,
			s.inboundSub,
		),
	)
	entered := make(chan struct{})
	var enteredOnce sync.Once
	s.inboundSub = subscribeInbound(s.bus, func(evt event.Event) {
		enteredOnce.Do(func() { close(entered) })
		s.handleInbound(evt)
	})
	s.mu.Lock()
	newConsumer(t, s)
	locked := true
	defer func() {
		if locked {
			s.mu.Unlock()
		}
	}()
	select {
	case <-entered:
	case <-t.Context().Done():
		t.Fatal("inbound handler did not enter")
	}

	flooded := make(chan struct{})
	var attempts atomic.Int64
	go func() {
		defer close(flooded)
		// Unknown connection IDs: the handler returns early for each, once
		// it is running again. One more than the subscriber buffer parks
		// the publisher on a full queue.
		for range event.DefaultSubscriberBuffer + 1 {
			attempts.Add(1)
			s.bus.Publish(
				connmanager.InboundConnectionEventType,
				event.NewEvent(
					connmanager.InboundConnectionEventType,
					connmanager.InboundConnectionEvent{},
				),
			)
		}
	}()
	require.Eventually(t, func() bool {
		return attempts.Load() == event.DefaultSubscriberBuffer+1
	}, 5*time.Second, time.Millisecond)
	select {
	case <-flooded:
		t.Fatal("inbound publisher did not backpressure on a full queue")
	default:
	}
	s.mu.Unlock()
	locked = false
	select {
	case <-flooded:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "inbound events never drained")
	}

	c := newConsumer(t, s)
	require.NoError(t, c.client().RequestMessagesBlocking())
	submit := submitter(t, s)
	require.Nil(t, submit(newTestMessage(testBody("after"), soonExpiry())))
	require.Equal(t, []string{"after"}, bodies(c.reply(t)))
}

// A consumer that ends both local protocols with MsgDone and MsgClientDone
// starts a new run of each server on its connection, as dmq-node's do, and
// the stack keeps serving other connections.
func TestStackHandlesClientDone(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	done := newConsumer(t, s)
	var id ouroboros.ConnectionId
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for k := range s.feeders {
			id = k
		}
		return len(s.feeders) == 1
	}, 10*time.Second, 10*time.Millisecond)
	server := s.connMgr.GetConnectionById(id)
	require.NotNil(t, server)
	submissionRun := server.LocalMessageSubmission().Server.ProtocolInstance()
	notificationRun := server.LocalMessageNotification().Server.ProtocolInstance()
	require.NoError(t, done.conn.LocalMessageSubmission().Client.Stop(context.Background()))
	require.NoError(t, done.client().Stop(context.Background()))
	// Each server passes through Done before its restart installs a new run,
	// so wait for a live run other than the one the client ended.
	require.Eventually(t, func() bool {
		submission := server.LocalMessageSubmission().Server.ProtocolInstance()
		notification := server.LocalMessageNotification().Server.ProtocolInstance()
		return submission != submissionRun && !submission.IsDone() &&
			notification != notificationRun && !notification.IsDone()
	}, 10*time.Second, 10*time.Millisecond)

	c := newConsumer(t, s)
	require.NoError(t, c.client().RequestMessagesBlocking())
	submit := submitter(t, s)
	require.Nil(t, submit(newTestMessage(testBody("next"), soonExpiry())))
	require.Equal(t, []string{"next"}, bodies(c.reply(t)))
}

// MsgClientDone ends one notification run; the server restarts for the next
// on the same connection and keeps its queue, so the feeder keeps the cursor
// until the connection closes, and then releases it.
func TestStackStopsFeederOnClientDone(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	feederIDs := func() []ouroboros.ConnectionId {
		s.mu.Lock()
		defer s.mu.Unlock()
		ids := make([]ouroboros.ConnectionId, 0, len(s.feeders))
		for id := range s.feeders {
			ids = append(ids, id)
		}
		return ids
	}
	hasCursor := func(id ouroboros.ConnectionId) bool {
		s.pool.peersMu.Lock()
		defer s.pool.peersMu.Unlock()
		_, ok := s.pool.peers[id.String()]
		return ok
	}
	submit := submitter(t, s)
	require.Eventually(t, func() bool {
		return len(feederIDs()) == 1
	}, 10*time.Second, 10*time.Millisecond)
	submitterID := feederIDs()[0]
	c := newConsumer(t, s)
	var consumerID ouroboros.ConnectionId
	require.Eventually(t, func() bool {
		for _, id := range feederIDs() {
			if id != submitterID {
				consumerID = id
				return hasCursor(id)
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond)

	require.NoError(t, c.client().Stop(context.Background()))
	require.Nil(t, submit(newTestMessage(testBody("after"), soonExpiry())))
	require.True(t, hasCursor(consumerID))
	require.NoError(t, c.conn.Close(context.Background()))
	require.Eventually(t, func() bool {
		return !hasCursor(consumerID)
	}, 10*time.Second, 10*time.Millisecond)
}

// The test message's certificate starts at KES period 1, so its last period
// is the default maximum evolutions later, less one.
func TestStackAdmitsCertificateLastKESPeriod(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{
		CurrentKESPeriod: func() (uint64, error) {
			return ocommon.DefaultMaxKESEvolutions, nil
		},
	})
	submit := submitter(t, s)
	require.Nil(t, submit(newTestMessage(testBody("last"), soonExpiry())))
}

// A consumer whose connection closes while the pool is idle must still have
// its feeder stop and its cursor released, with no admission to wake on.
// MsgClientDone alone does not release it: the server restarts.
func TestStackReleasesCursorOnIdleClientDone(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	c := newConsumer(t, s)
	var id ouroboros.ConnectionId
	hasCursor := func() bool {
		s.pool.peersMu.Lock()
		defer s.pool.peersMu.Unlock()
		_, ok := s.pool.peers[id.String()]
		return ok
	}
	require.Eventually(t, func() bool {
		s.mu.Lock()
		for k := range s.feeders {
			id = k
		}
		s.mu.Unlock()
		return hasCursor()
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, c.client().Stop(context.Background()))
	require.True(t, hasCursor())
	require.NoError(t, c.conn.Close(context.Background()))
	require.Eventually(t, func() bool {
		return !hasCursor()
	}, 10*time.Second, 10*time.Millisecond)
}

// refusalCounter counts the stack's log records for a notification queue
// refusing a message.
type refusalCounter struct{ n atomic.Int64 }

func (r *refusalCounter) Enabled(context.Context, slog.Level) bool {
	return true
}

func (r *refusalCounter) Handle(_ context.Context, rec slog.Record) error {
	if rec.Message == "dmq notification queue refused message" {
		r.n.Add(1)
	}
	return nil
}

func (r *refusalCounter) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *refusalCounter) WithGroup(string) slog.Handler      { return r }

type manualFeedTimer struct {
	wait    time.Duration
	wake    chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func (t *manualFeedTimer) timer() feedTimer {
	return feedTimer{
		wake: t.wake,
		stop: func() { t.once.Do(func() { close(t.stopped) }) },
	}
}

// A connection that never requests notifications, such as a submit-only
// client, fills its notification queue. Its feeder must back off rather than
// re-offer the held message at a fixed short interval for the life of the
// connection.
func TestStackBacksOffFullNotificationQueue(t *testing.T) {
	t.Parallel()
	refusals := &refusalCounter{}
	s, _ := newTestStack(t, StackConfig{Logger: slog.New(refusals)})
	timers := make(
		chan *manualFeedTimer,
		2*localmessagenotification.NewConfig().MaxQueueSize,
	)
	s.feedTimer = func(wait time.Duration) feedTimer {
		timer := &manualFeedTimer{
			wait:    wait,
			wake:    make(chan time.Time, 1),
			stopped: make(chan struct{}),
		}
		timers <- timer
		return timer.timer()
	}
	submit := submitter(t, s)
	// One more than the notification server's default queue size.
	for i := range localmessagenotification.NewConfig().MaxQueueSize + 1 {
		require.Nil(t, submit(newTestMessage(
			fmt.Appendf(nil, "fill-%d", i),
			soonExpiry(),
		)))
	}
	require.Eventually(t, func() bool {
		return refusals.n.Load() > 0
	}, 10*time.Second, 10*time.Millisecond)
	nextActiveTimer := func() *manualFeedTimer {
		t.Helper()
		for {
			select {
			case timer := <-timers:
				select {
				case <-timer.stopped:
					continue
				default:
					return timer
				}
			case <-time.After(5 * time.Second):
				t.Fatal("feeder did not create its retry timer")
				return nil
			}
		}
	}

	for i, want := range []time.Duration{
		feedRetryInterval,
		2 * feedRetryInterval,
		4 * feedRetryInterval,
	} {
		timer := nextActiveTimer()
		require.Equal(t, want, timer.wait)
		before := refusals.n.Load()
		timer.wake <- time.Now()
		require.Eventually(t, func() bool {
			return refusals.n.Load() > before
		}, 5*time.Second, time.Millisecond, "retry %d did not run", i+1)
	}
}

func TestStackAdmissionUsesPoolClock(t *testing.T) {
	t.Parallel()
	now := time.Now().Add(-time.Minute)
	s, _ := newTestStack(t, StackConfig{Now: func() time.Time { return now }})
	msg := newTestMessage(
		[]byte("clock"),
		uint32(now.Add(30*time.Second).Unix()),
	)
	require.False(t, msg.IsValid())
	require.Nil(t, s.admit(&msg))
}

// A message whose body breaks CIP-0137's sizes is one dmq-node refuses to
// decode, and gouroboros refuses it the same way: the local connection ends
// instead of answering. In-range bodies are admitted, and a key or signature
// of the wrong length is refused by the stack with a reason.
func TestStackRefusesMessagesOutsideTheSizeLimits(t *testing.T) {
	t.Parallel()
	s, reg := newTestStack(t, StackConfig{})
	exp := soonExpiry()
	body := func(n int) []byte { return bytes.Repeat([]byte{'x'}, n) }

	for _, n := range []int{0, 1, 89, 2001, 5000} {
		conn := dialClient(
			t,
			s,
			localmessagesubmission.NewConfig(),
			localmessagenotification.NewConfig(),
		)
		msg := newSizedTestMessage(body(n), exp)
		err := conn.LocalMessageSubmission().Client.SubmitMessage(&msg)
		require.ErrorIs(
			t, err, protocol.ErrProtocolShuttingDown, "body of %d bytes", n,
		)
	}
	require.Zero(t, s.Mempool().Len())
	submit := submitter(t, s)
	for _, n := range []int{90, 1000, 2000} {
		require.Nil(t, submit(newSizedTestMessage(body(n), exp)), "%d bytes", n)
	}
	require.Equal(t, 3, s.Mempool().Len())

	// A signature or key of the wrong length is refused as well.
	short := newSizedTestMessage(body(400), exp)
	short.KESSignature = make([]byte, 447)
	require.IsType(t, ocommon.InvalidReason{}, submit(short))
	require.Equal(
		t,
		1.0,
		metricValue(t, reg, "dingo_dmq_validation_failures_total", "invalid"),
	)
}
