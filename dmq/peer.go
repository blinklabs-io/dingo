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
	"log/slog"
	"maps"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/blinklabs-io/gouroboros/connection"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/handshake"
)

// peerTuning holds the limits a peer connection runs under. Production uses
// defaultPeerTuning; tests shrink them to reach the rarer paths.
type peerTuning struct {
	// versions limits the node-to-node versions offered in the handshake.
	versions []uint16
	// initiatorOnly offers to pull and serve only on connections this side
	// opens, as a node that cannot accept inbound connections would. The
	// default offers both roles on every connection.
	initiatorOnly    bool
	handshakeTimeout time.Duration
	keepAlivePeriod  time.Duration
	keepAliveTimeout time.Duration
	// blockingTimeout, nonBlockingTimeout and sigsTimeout are how long the
	// pulling side waits for the reply to a blocking request, a non-blocking
	// one, and a request for message bodies. They are dmq-node's limits for
	// the same states, so a peer that is slower than its own implementation
	// allows is not waited on.
	blockingTimeout    time.Duration
	nonBlockingTimeout time.Duration
	sigsTimeout        time.Duration
	// blockingReplyWait is how long the serving side holds a blocking request
	// back, waiting for a message to announce, before it answers that it has
	// none. It has to be well inside the pulling side's blocking limit.
	blockingReplyWait time.Duration
	// maxUnacked bounds the message IDs announced to this side and not yet
	// acknowledged, which is what its pulling loop may leave outstanding.
	maxUnacked int
	// outboundMaxUnacked bounds the IDs this side tolerates a peer leaving
	// outstanding when it pulls from here. It is the more generous of the two
	// so a peer whose own limit is higher is not mistaken for a violator;
	// holding an ID costs 32 bytes.
	outboundMaxUnacked int
	// idBatch is how many message IDs one request asks for.
	idBatch int
	// fetchMessages and fetchBytes bound the messages one fetch asks for.
	// An announced ID beyond them waits for the next round, which is what
	// leaves IDs outstanding and so makes the next ID request non-blocking.
	fetchMessages int
	fetchBytes    uint64
	// knownIDs bounds how many IDs a peer announced that are remembered so as
	// not to announce them back.
	knownIDs int
	// pullGate, when set, holds the pulling loop back until it is closed, so
	// a test can act on the protocol before the first request goes out.
	pullGate <-chan struct{}
}

func defaultPeerTuning() peerTuning {
	return peerTuning{
		handshakeTimeout: 10 * time.Second,
		keepAlivePeriod:  keepAliveDefaultPeriod,
		keepAliveTimeout: keepAliveDefaultTimeout,
		// dmq-node waits 20 seconds for the reply to a blocking request and
		// 10 for the other two, then ends the connection.
		blockingTimeout:    20 * time.Second,
		nonBlockingTimeout: 10 * time.Second,
		sigsTimeout:        10 * time.Second,
		blockingReplyWait:  10 * time.Second,
		// Version 1 is TxSubmission2's codec under another name, which this
		// stack does not speak, so only version 2 is offered.
		versions:           []uint16{protocol.ProtocolVersionDMQNtN2},
		maxUnacked:         defaultMaxUnacked,
		outboundMaxUnacked: 1000,
		idBatch:            20,
		fetchMessages:      10,
		fetchBytes:         32 << 10,
		knownIDs:           2048,
	}
}

// PeerConn is one node-to-node DMQ connection. Both sides can pull from and
// serve the other, so a PeerConn announces this stack's messages to the remote
// and admits the remote's messages into this stack's pool.
type PeerConn struct {
	stack  *Stack
	logger *slog.Logger
	tune   peerTuning
	// cursorKey identifies the peer's FIFO cursor over the pool.
	cursorKey string
	conn      net.Conn
	dialed    bool

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex // guards closing
	closing  bool
	wg       sync.WaitGroup
	doneChan chan any
	protoErr chan error

	// protoMu guards the connection state built during the handshake, so
	// teardown sees every protocol a connect in progress has started.
	protoMu          sync.Mutex
	mux              *muxer.Muxer
	puller           *sigPuller
	server           *sigServer
	stoppers         []func()
	version          uint16
	remoteFullDuplex bool
	finishedOnce     sync.Once
	finished         chan struct{}

	// blockingRequests and nonBlockingRequests count the ID requests the
	// pulling side has sent, by kind.
	blockingRequests    atomic.Int64
	nonBlockingRequests atomic.Int64
	keepAlive           *keepAliveClient

	// pullDone is closed when the pulling loop ends.
	pullDone chan struct{}

	known      *idSet
	idReplies  chan []ocommon.MessageIDAndSize
	msgReplies chan []ocommon.DmqMessage

	errMu     sync.Mutex
	err       error
	closeOnce sync.Once
	closed    chan struct{}
}

// AttachPeer runs a node-to-node DMQ connection over conn: it negotiates the
// protocol version, starts the roles both sides agreed on, and then serves and
// pulls messages until the connection ends. dialed says whether this side
// opened the connection. AttachPeer returns once the handshake has finished
// and the protocols have started. The PeerConn owns the connection from then
// on, and the Stack closes any still open on Stop. The caller owns neither
// listening nor dialing.
func (s *Stack) AttachPeer(
	ctx context.Context,
	conn net.Conn,
	dialed bool,
) (*PeerConn, error) {
	p, err := s.registerPeer(conn, dialed)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := p.connect(ctx); err != nil {
		p.fail(err)
		<-p.closed
		return nil, err
	}
	return p, nil
}

func (s *Stack) registerPeer(conn net.Conn, dialed bool) (*PeerConn, error) {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	if s.peersClosed {
		return nil, errors.New("dmq: stack is stopped")
	}
	if s.maxPeers > 0 && len(s.peers) >= s.maxPeers {
		return nil, ErrTooManyPeers
	}
	// gouroboros formats both addresses when it logs the connection.
	if conn.LocalAddr() == nil || conn.RemoteAddr() == nil {
		return nil, errors.New("dmq: connection has no network address")
	}
	remote := conn.RemoteAddr().String()
	ctx, cancel := context.WithCancel(context.Background())
	p := &PeerConn{
		stack:  s,
		logger: s.logger.With("peer", remote),
		tune:   s.peerTune,
		cursorKey: "peer:" +
			strconv.FormatUint(s.peerSeq.Add(1), 10) + ":" + remote,
		conn:       conn,
		dialed:     dialed,
		ctx:        ctx,
		cancel:     cancel,
		doneChan:   make(chan any),
		protoErr:   make(chan error, 16),
		finished:   make(chan struct{}),
		pullDone:   make(chan struct{}),
		known:      newIDSet(s.peerTune.knownIDs),
		idReplies:  make(chan []ocommon.MessageIDAndSize, 1),
		msgReplies: make(chan []ocommon.DmqMessage, 1),
		closed:     make(chan struct{}),
	}
	s.peers[p] = struct{}{}
	s.peersCnt.Inc()
	return p, nil
}

func (s *Stack) forgetPeer(p *PeerConn) {
	s.peerMu.Lock()
	defer s.peerMu.Unlock()
	if _, ok := s.peers[p]; ok {
		delete(s.peers, p)
		s.peersCnt.Dec()
		// A lost peer may leave the stack below its minimum.
		if s.peerNet != nil && s.peerNet.manager != nil {
			s.peerNet.manager.kick()
		}
	}
}

// closePeers refuses further peers and closes every open one.
func (s *Stack) closePeers(ctx context.Context) error {
	s.peerMu.Lock()
	s.peersClosed = true
	open := slices.Collect(maps.Keys(s.peers))
	s.peerMu.Unlock()
	for _, p := range open {
		p.fail(nil)
	}
	for _, p := range open {
		select {
		case <-p.closed:
		case <-ctx.Done():
			return fmt.Errorf("dmq: closing peers: %w", ctx.Err())
		}
	}
	return nil
}

// Done is closed once the connection has been torn down.
func (p *PeerConn) Done() <-chan struct{} { return p.closed }

// Err returns the error that ended the connection, or nil if it was closed
// locally or by the remote.
func (p *PeerConn) Err() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.err
}

// RemoteAddr returns the remote end of the connection.
func (p *PeerConn) RemoteAddr() net.Addr { return p.conn.RemoteAddr() }

// Version returns the negotiated node-to-node protocol version.
func (p *PeerConn) Version() uint16 {
	p.protoMu.Lock()
	defer p.protoMu.Unlock()
	return p.version
}

// Close ends the connection and waits for it to be torn down.
func (p *PeerConn) Close() {
	p.fail(nil)
	<-p.closed
}

// fail records the error that ended the connection and starts teardown. Only
// the first call counts: an error that follows is a symptom of the teardown,
// such as the muxer reading from a connection that was just closed. A nil
// error is a clean close. It never blocks, so protocol callbacks may call it.
func (p *PeerConn) fail(err error) {
	if err != nil && endedCleanly(err) {
		err = nil
	}
	p.closeOnce.Do(func() {
		p.errMu.Lock()
		p.err = err
		p.errMu.Unlock()
		if err != nil {
			p.logger.Warn("dmq peer connection failed", "error", err)
		}
		close(p.doneChan)
		go p.teardown()
	})
}

// failSend ends the connection after a send failed. A protocol that is
// shutting down refuses sends, but the refusal is a symptom: what stopped the
// protocol, such as the muxer losing the connection or a protocol error,
// reaches fail through the muxer's or the protocols' error channel. A refusal
// recorded first would hide it, and would report a remote reset as a failure.
// A protocol the peer ended leaves the connection up for the other direction.
func (p *PeerConn) failSend(what string, err error) {
	if errors.Is(err, protocol.ErrProtocolShuttingDown) {
		return
	}
	p.fail(fmt.Errorf("dmq peer: %s: %w", what, err))
}

// endedCleanly reports whether err is how a connection normally ends: the
// remote hanging up, or this side having closed it. A remote that closes with
// unread data queued makes the kernel reset the connection, so the muxer sees
// ECONNRESET (or EPIPE on a write) where an orderly close would give EOF.
func endedCleanly(err error) bool {
	var closed *muxer.ConnectionClosedError
	return errors.As(err, &closed) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

func (p *PeerConn) teardown() {
	p.cancel()
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	p.protoMu.Lock()
	stoppers := slices.Clone(p.stoppers)
	mux := p.mux
	p.protoMu.Unlock()
	for _, stop := range stoppers {
		stop()
	}
	if mux != nil {
		mux.Stop()
	}
	// The muxer closes the connection when its read loop ends, but a loop
	// that never started would leave it open.
	_ = p.conn.Close()
	p.wg.Wait()
	p.stack.pool.RemovePeer(p.cursorKey)
	p.stack.forgetPeer(p)
	close(p.closed)
}

// spawn runs fn on a goroutine that teardown waits for. It does nothing once
// teardown has begun.
func (p *PeerConn) spawn(fn func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return
	}
	p.wg.Go(fn)
}

func (p *PeerConn) connect(ctx context.Context) error {
	connID := connection.ConnectionId{
		LocalAddr:  p.conn.LocalAddr(),
		RemoteAddr: p.conn.RemoteAddr(),
	}
	role := protocol.ProtocolRoleServer
	if p.dialed {
		role = protocol.ProtocolRoleClient
	}
	p.protoMu.Lock()
	p.mux = muxer.New(p.conn)
	mux := p.mux
	p.protoMu.Unlock()
	opts := protocol.ProtocolOptions{
		ConnectionId:       connID,
		ConnectionDoneChan: p.doneChan,
		Muxer:              mux,
		Logger:             p.logger,
		ErrorChan:          p.protoErr,
		Mode:               protocol.ProtocolModeNodeToNode,
		Role:               role,
	}
	p.spawn(func() {
		select {
		case err, ok := <-mux.ErrorChan():
			if ok {
				p.fail(fmt.Errorf("dmq peer: muxer: %w", err))
			}
		case <-p.doneChan:
		}
	})
	p.spawn(func() {
		select {
		case err, ok := <-p.protoErr:
			if ok {
				p.fail(fmt.Errorf("dmq peer: protocol: %w", err))
			}
		case <-p.doneChan:
		}
	})

	// The wire value is "initiator only", so offering both roles is false.
	diffusion := protocol.DiffusionModeInitiatorAndResponder
	if p.tune.initiatorOnly {
		diffusion = protocol.DiffusionModeInitiatorOnly
	}
	versions := protocol.GetProtocolVersionMapDMQNtN(
		p.stack.cfg.NetworkMagic,
		diffusion,
		false,
		false,
	)
	if p.tune.versions != nil {
		for version := range versions {
			if !slices.Contains(p.tune.versions, version) {
				delete(versions, version)
			}
		}
	}
	hsCfg := handshake.NewConfig(
		handshake.WithProtocolVersionMap(versions),
		handshake.WithFinishedFunc(p.handshakeFinished),
		handshake.WithTimeout(p.tune.handshakeTimeout),
	)
	hs := handshake.New(opts, &hsCfg)
	p.protoMu.Lock()
	p.stoppers = append(
		p.stoppers,
		hs.Client.Stop,
		hs.Server.Stop,
	)
	p.protoMu.Unlock()
	if p.dialed {
		hs.Client.Start()
	} else {
		hs.Server.Start()
	}
	mux.StartOnce()

	timer := time.NewTimer(p.tune.handshakeTimeout)
	defer timer.Stop()
	select {
	case <-p.finished:
	case <-p.doneChan:
		return p.endedDuringHandshake()
	case <-ctx.Done():
		return fmt.Errorf("dmq peer: handshake: %w", ctx.Err())
	case <-timer.C:
		return errors.New("dmq peer: handshake timed out")
	}
	return p.startProtocols(opts, mux)
}

// endedDuringHandshake returns why the connection ended before the handshake
// finished.
func (p *PeerConn) endedDuringHandshake() error {
	if err := p.Err(); err != nil {
		return err
	}
	return errors.New("dmq peer: connection closed during the handshake")
}

func (p *PeerConn) handshakeFinished(
	_ handshake.CallbackContext,
	version uint16,
	data protocol.VersionData,
) error {
	if data == nil ||
		!slices.Contains(protocol.GetProtocolVersionsDMQNtN(), version) {
		return fmt.Errorf(
			"dmq peer: handshake finished with unusable version %d",
			version,
		)
	}
	p.protoMu.Lock()
	p.version = version
	p.remoteFullDuplex = data.DiffusionMode() ==
		protocol.DiffusionModeInitiatorAndResponder
	p.protoMu.Unlock()
	p.finishedOnce.Do(func() { close(p.finished) })
	return nil
}

// startProtocols builds and starts the mini-protocols. The pulling side and
// the keep-alive client are the connection's initiator instances, which run
// where this side dialed or both sides offered both roles; the serving side
// and the keep-alive server are the responder instances, which run where this
// side listened or both sides offered both roles.
func (p *PeerConn) startProtocols(
	opts protocol.ProtocolOptions,
	mux *muxer.Muxer,
) error {
	p.protoMu.Lock()
	defer p.protoMu.Unlock()
	select {
	case <-p.doneChan:
		return p.endedDuringHandshake()
	default:
	}
	opts.Version = p.version
	// Both roles run on one connection only when both sides offered them.
	bothRoles := p.remoteFullDuplex && !p.tune.initiatorOnly
	initiator := bothRoles || p.dialed
	responder := bothRoles || !p.dialed

	timeouts := sigTimeouts{
		blocking:    p.tune.blockingTimeout,
		nonBlocking: p.tune.nonBlockingTimeout,
		sigs:        p.tune.sigsTimeout,
	}
	// The two sides have separate limits on outstanding IDs.
	p.puller = newSigPuller(opts, sigPullerConfig{
		maxUnacked:  p.tune.maxUnacked,
		timeouts:    timeouts,
		onReplyIDs:  p.onReplyIDs,
		onReplySigs: p.onReplySigs,
	})
	p.server = newSigServer(opts, sigServerConfig{
		maxUnacked:    p.tune.outboundMaxUnacked,
		timeouts:      timeouts,
		onRequestIDs:  p.onRequestIDs,
		onRequestSigs: p.onRequestSigs,
		onDone:        p.onDone,
	})
	kaClient := newKeepAliveClient(
		opts, p.tune.keepAlivePeriod, p.tune.keepAliveTimeout,
	)
	kaServer := newKeepAliveServer(opts)
	p.keepAlive = kaClient
	p.stoppers = append(
		p.stoppers,
		p.puller.Stop,
		p.server.Stop,
		kaClient.Stop,
		kaServer.Stop,
	)
	// Register the responders before the muxer starts reading, so a message
	// that arrives at once finds its protocol.
	if responder {
		p.server.EnsureRegistered()
		kaServer.EnsureRegistered()
	}
	if initiator {
		p.puller.Start()
		kaClient.Start()
		p.spawn(p.pull)
	}
	if responder {
		p.server.Start()
		kaServer.Start()
	}
	mode := muxer.DiffusionModeInitiator
	switch {
	case bothRoles:
		mode = muxer.DiffusionModeInitiatorAndResponder
	case !p.dialed:
		mode = muxer.DiffusionModeResponder
	}
	mux.SetDiffusionMode(mode)
	mux.Start()
	p.logger.Debug(
		"dmq peer connected",
		"version", p.version,
		"dialed", p.dialed,
		"both_roles", bothRoles,
	)
	return nil
}
