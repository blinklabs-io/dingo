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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
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
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// submitter returns a function that submits msg and waits for the server's
// verdict: nil for accept, the reject reason otherwise.
func submitter(
	t *testing.T,
	s *Stack,
) func(ocommon.DmqMessage) ocommon.RejectReason {
	t.Helper()
	accepted := make(chan struct{}, 1)
	rejected := make(chan ocommon.RejectReason, 1)
	conn := dialClient(
		t,
		s,
		localmessagesubmission.NewConfig(
			localmessagesubmission.WithAcceptMessageFunc(
				func(localmessagesubmission.CallbackContext) {
					accepted <- struct{}{}
				},
			),
			localmessagesubmission.WithRejectMessageFunc(
				func(
					_ localmessagesubmission.CallbackContext,
					reason ocommon.RejectReason,
				) {
					rejected <- reason
				},
			),
		),
		localmessagenotification.NewConfig(),
	)
	return func(msg ocommon.DmqMessage) ocommon.RejectReason {
		require.NoError(
			t,
			conn.LocalMessageSubmission().Client.SubmitMessage(&msg),
		)
		select {
		case <-accepted:
			return nil
		case reason := <-rejected:
			return reason
		case <-time.After(10 * time.Second):
			require.FailNow(t, "no accept or reject from server")
			return nil
		}
	}
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
		out = append(out, string(m.Payload.MessageBody))
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

func newNoStakeAuthenticator(t *testing.T) *ocommon.MessageAuthenticator {
	t.Helper()
	auth, err := ocommon.NewMessageAuthenticator(
		ocommon.MessageAuthenticatorConfig{StakeAuthority: noStake{}},
	)
	require.NoError(t, err)
	return auth
}

func TestStackSubmissionAdmitsToMempool(t *testing.T) {
	t.Parallel()
	s, reg := newTestStack(t, StackConfig{})
	submit := submitter(t, s)

	require.Nil(t, submit(newTestMessage([]byte("one"), soonExpiry())))
	require.Equal(t, 1, s.Mempool().Len())
	got, ok := s.Mempool().NextForPeer("probe")
	require.True(t, ok)
	require.Equal(t, []byte("one"), got.Payload.MessageBody)
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
		msg   ocommon.DmqMessage
		want  ocommon.RejectReason
		label string
	}{
		{
			name:  "expired",
			msg:   newTestMessage([]byte("old"), 1),
			want:  ocommon.ExpiredReason{},
			label: reasonExpired,
		},
		{
			name: "duplicate",
			first: func() *ocommon.DmqMessage {
				m := newTestMessage([]byte("dup"), soon)
				return &m
			}(),
			msg:   newTestMessage([]byte("dup"), soon),
			want:  ocommon.AlreadyReceivedReason{},
			label: reasonAlreadyReceived,
		},
		{
			name:  "unauthorized pool",
			cfg:   StackConfig{Authenticator: newNoStakeAuthenticator(t)},
			msg:   newTestMessage([]byte("anon"), soon),
			want:  ocommon.InvalidReason{},
			label: reasonInvalid,
		},
		{
			name:  "expiry beyond message ttl",
			msg:   newTestMessage([]byte("far"), tooFar),
			want:  ocommon.InvalidReason{},
			label: reasonInvalid,
		},
		{
			name:  "pool full",
			cfg:   StackConfig{MaxMempoolBytes: 1},
			msg:   newTestMessage([]byte("big"), soon),
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
	require.Nil(t, submit(newTestMessage([]byte("a"), soonExpiry())))
	require.Nil(t, submit(newTestMessage([]byte("b"), soonExpiry())))

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
	require.Equal(
		t,
		6.0,
		metricValue(t, reg, "dingo_dmq_messages_sent_total", ""),
	)
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
	require.Nil(t, submit(newTestMessage([]byte("late"), soonExpiry())))
	require.Equal(t, []string{"late"}, bodies(c.reply(t)))
}

func TestStackNotificationDeliversEveryMessageOnce(t *testing.T) {
	t.Parallel()
	// More messages than a consumer's queue holds, so the feeder must hold
	// the refused ones back rather than drop them.
	const total = 250
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

	require.NoError(t, c.conn.Close())
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
