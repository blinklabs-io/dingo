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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagenotification"
	"github.com/blinklabs-io/gouroboros/protocol/localmessagesubmission"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// feedRetryInterval is how long a notification feeder waits before offering a
// message again after the consumer's queue refused it.
const feedRetryInterval = 100 * time.Millisecond

// Reason labels for dingo_dmq_validation_failures_total, one per CIP-0137
// reject reason.
const (
	reasonInvalid         = "invalid"
	reasonAlreadyReceived = "already_received"
	reasonExpired         = "expired"
	reasonOther           = "other"
)

// StackConfig configures a Stack.
type StackConfig struct {
	Logger       *slog.Logger
	PromRegistry prometheus.Registerer
	// NetworkMagic is the DMQ topic's network magic, used for the local
	// handshake. It is independent of the Cardano network magic.
	NetworkMagic uint32
	// SocketPath is the Unix socket the local submission and notification
	// protocols are served on.
	SocketPath string
	// MessageTTL is the longest expiry a submitted message may claim. Zero
	// uses the CIP-0137 default of 30 minutes.
	MessageTTL time.Duration
	// MaxMempoolBytes bounds the message pool's total encoded size. Zero
	// means unlimited.
	MaxMempoolBytes int64
	// Authenticator verifies every submitted message before it is admitted.
	// Required.
	Authenticator *ocommon.MessageAuthenticator
}

// Stack is one DMQ topic instance: its own message pool, connection manager
// and local Unix socket, running alongside the Cardano stack. See the package
// documentation.
type Stack struct {
	cfg      StackConfig
	logger   *slog.Logger
	pool     *MessageMempool
	bus      *event.EventBus
	connMgr  *connmanager.ConnectionManager
	ttl      *ocommon.TTLValidator
	failures *prometheus.CounterVec
	sent     prometheus.Counter
	conns    prometheus.Gauge

	// ctx scopes the feeder goroutines; Stop cancels it and waits on wg.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	stopOnce sync.Once
	stopErr  error

	mu         sync.Mutex // guards feeders
	feeders    map[ouroboros.ConnectionId]context.CancelFunc
	inboundSub event.EventSubscriberId
}

// NewStack builds a Stack. Call Start to open its socket.
func NewStack(cfg StackConfig) (*Stack, error) {
	if cfg.Authenticator == nil {
		return nil, errors.New("dmq: stack requires a message authenticator")
	}
	if cfg.SocketPath == "" {
		return nil, errors.New("dmq: stack requires a socket path")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	logger = logger.With("component", "dmq")
	factory := promauto.With(cfg.PromRegistry)
	s := &Stack{
		cfg:    cfg,
		logger: logger,
		pool: NewMessageMempool(Config{
			Capacity:     cfg.MaxMempoolBytes,
			PromRegistry: cfg.PromRegistry,
		}),
		// The bus carries only this stack's connection events, so the
		// Cardano stack's handlers never see a DMQ connection.
		bus: event.NewEventBus(nil, logger),
		ttl: ocommon.NewTTLValidator(cfg.MessageTTL, logger),
		failures: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dingo_dmq_validation_failures_total",
			Help: "DMQ messages refused, by CIP-0137 reject reason",
		}, []string{"reason"}),
		sent: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_dmq_messages_sent_total",
			Help: "DMQ messages queued for local notification consumers",
		}),
		conns: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_dmq_connections",
			Help: "open local DMQ connections",
		}),
		feeders: make(map[ouroboros.ConnectionId]context.CancelFunc),
	}
	for _, reason := range []string{
		reasonInvalid, reasonAlreadyReceived, reasonExpired, reasonOther,
	} {
		s.failures.WithLabelValues(reason)
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.connMgr = connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger:   logger,
			EventBus: s.bus,
			// Local connections are node-to-client, and the connection
			// manager publishes its closed event only for node-to-node ones.
			ConnClosedFunc: func(id ouroboros.ConnectionId, _ bool, _ error) {
				s.handleClosed(id)
			},
			Listeners: []connmanager.ListenerConfig{{
				ListenNetwork: "unix",
				ListenAddress: cfg.SocketPath,
				UseNtC:        true,
				TrustedLocal:  true,
				ConnectionOpts: []ouroboros.ConnectionOptionFunc{
					ouroboros.WithDMQ(true),
					ouroboros.WithNetworkMagic(cfg.NetworkMagic),
					ouroboros.WithLocalMessageSubmissionConfig(
						s.submissionConfig(),
					),
					ouroboros.WithLocalMessageNotificationConfig(
						s.notificationConfig(),
					),
				},
			}},
		},
	)
	return s, nil
}

// Mempool returns the stack's message pool, so other DMQ transports can
// admit into it.
func (s *Stack) Mempool() *MessageMempool {
	return s.pool
}

// Start opens the local socket and begins serving.
func (s *Stack) Start(ctx context.Context) error {
	s.pool.Start()
	// Lossless: a detached handler would start no feeder for any later
	// connection, and nothing would resubscribe it.
	s.inboundSub = s.bus.SubscribeFuncWithBufferPolicy(
		connmanager.InboundConnectionEventType,
		event.DefaultSubscriberBuffer,
		event.SubscriberBackpressureBlock,
		s.handleInbound,
	)
	if err := s.connMgr.Start(ctx); err != nil {
		s.shutdownLocal()
		return errors.Join(
			fmt.Errorf("dmq: start connection manager: %w", err),
			s.pool.Stop(ctx),
		)
	}
	return nil
}

// Stop closes the socket and every local connection, and waits for the
// notification feeders to exit. It is safe to call more than once; later calls
// return the first call's result.
func (s *Stack) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.stopErr = s.connMgr.Stop(ctx)
		s.shutdownLocal()
		if poolErr := s.pool.Stop(ctx); poolErr != nil {
			s.stopErr = errors.Join(s.stopErr, poolErr)
		}
	})
	return s.stopErr
}

func (s *Stack) shutdownLocal() {
	s.bus.UnsubscribeAndWait(
		connmanager.InboundConnectionEventType,
		s.inboundSub,
	)
	s.cancel()
	s.wg.Wait()
	s.bus.Stop()
}

// submissionConfig serves protocol 14. Validation happens in submit, not in
// the gouroboros server, so that every refusal is counted and an expired
// message gets the expired reason rather than a generic invalid one; the
// server's own checks are therefore disabled.
func (s *Stack) submissionConfig() localmessagesubmission.Config {
	return localmessagesubmission.NewConfig(
		localmessagesubmission.WithAuthenticator(
			ocommon.NewNoOpAuthenticator(s.logger),
		),
		localmessagesubmission.WithTTLValidator(
			ocommon.NewNoOpTTLValidator(s.logger),
		),
		localmessagesubmission.WithSubmitMessageFunc(
			func(
				_ localmessagesubmission.CallbackContext,
				msg *ocommon.DmqMessage,
			) ocommon.RejectReason {
				return s.submit(msg)
			},
		),
	)
}

// submit validates and admits a locally submitted message, returning nil when
// it was accepted and the CIP-0137 reject reason otherwise.
func (s *Stack) submit(msg *ocommon.DmqMessage) ocommon.RejectReason {
	reason := s.admit(msg)
	if reason != nil {
		s.failures.WithLabelValues(reasonLabel(reason)).Inc()
		s.logger.Debug("dmq message rejected", "reason", reason)
	}
	return reason
}

func (s *Stack) admit(msg *ocommon.DmqMessage) ocommon.RejectReason {
	if !msg.IsValid() {
		return ocommon.ExpiredReason{}
	}
	if err := s.ttl.ValidateMessageTTL(msg); err != nil {
		return ocommon.InvalidReason{Message: err.Error()}
	}
	if err := s.cfg.Authenticator.VerifyMessage(msg); err != nil {
		return ocommon.InvalidReason{Message: err.Error()}
	}
	added, err := s.pool.Add(*msg)
	switch {
	case errors.Is(err, ErrExpired):
		return ocommon.ExpiredReason{}
	case errors.Is(err, ErrInvalidMessageID):
		return ocommon.InvalidReason{Message: err.Error()}
	case err != nil:
		return ocommon.OtherReason{Message: err.Error()}
	case !added:
		return ocommon.AlreadyReceivedReason{}
	}
	return nil
}

func reasonLabel(reason ocommon.RejectReason) string {
	switch reason.(type) {
	case ocommon.InvalidReason:
		return reasonInvalid
	case ocommon.AlreadyReceivedReason:
		return reasonAlreadyReceived
	case ocommon.ExpiredReason:
		return reasonExpired
	default:
		return reasonOther
	}
}

// notificationConfig serves protocol 15. Each connection's server queue is
// fed from the pool by a feeder goroutine, so the messages it holds were
// already validated on admission and need no second authentication pass.
func (s *Stack) notificationConfig() localmessagenotification.Config {
	return localmessagenotification.NewConfig(
		localmessagenotification.WithAuthenticator(
			ocommon.NewNoOpAuthenticator(s.logger),
		),
		localmessagenotification.WithTTLValidator(
			ocommon.NewNoOpTTLValidator(s.logger),
		),
	)
}

func (s *Stack) handleInbound(evt event.Event) {
	data, ok := evt.Data.(connmanager.InboundConnectionEvent)
	if !ok {
		return
	}
	conn := s.connMgr.GetConnectionById(data.ConnectionId)
	if conn == nil || conn.LocalMessageNotification() == nil {
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	s.mu.Lock()
	s.feeders[data.ConnectionId] = cancel
	s.mu.Unlock()
	s.conns.Inc()
	server := conn.LocalMessageNotification().Server
	consumerID := data.ConnectionId.String()
	s.wg.Go(func() {
		defer s.pool.RemovePeer(consumerID)
		s.feed(ctx, consumerID, server)
	})
	// This handler runs on the bus, after the connection is registered, and
	// can lose a race with the close callback. The connection manager removes
	// a connection before calling ConnClosedFunc, so a connection already gone
	// here had its close handled before this feeder was registered.
	if s.connMgr.GetConnectionById(data.ConnectionId) == nil {
		s.handleClosed(data.ConnectionId)
	}
}

func (s *Stack) handleClosed(id ouroboros.ConnectionId) {
	s.mu.Lock()
	cancel, ok := s.feeders[id]
	delete(s.feeders, id)
	s.mu.Unlock()
	if ok {
		s.conns.Dec()
		cancel()
	}
}

// feed moves every message the consumer has not yet seen from the pool into
// its notification server's queue. A message the queue refuses is held and
// offered again, so a slow consumer delays messages but skips only those that
// expire first. It returns once the consumer ends notification with
// MsgClientDone, which leaves the connection open for submission.
func (s *Stack) feed(
	ctx context.Context,
	consumerID string,
	server *localmessagenotification.Server,
) {
	var pending *ocommon.DmqMessage
	for {
		// Checked on every wake: the server's terminal state has no channel.
		if server.IsDone() {
			return
		}
		// Take the signal before draining so an admission that lands
		// between the drain and the wait still wakes this loop.
		added := s.pool.AddedSignal()
		for {
			if pending == nil {
				msg, ok := s.pool.NextForPeer(consumerID)
				if !ok {
					break
				}
				pending = &msg
			}
			if !pending.IsValid() {
				pending = nil
				continue
			}
			if err := server.AddMessage(pending); err != nil {
				s.logger.Debug(
					"dmq notification queue refused message",
					"error", err,
				)
				break
			}
			s.sent.Inc()
			pending = nil
		}
		if pending == nil {
			select {
			case <-added:
			case <-ctx.Done():
				return
			}
			continue
		}
		timer := time.NewTimer(feedRetryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}
