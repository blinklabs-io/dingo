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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// This file is node-to-node message submission, version 2, as dmq-node runs
// it. The gouroboros messagesubmission package is not used because dmq-node
// cannot read its messages (see sigsub_messages.go) and because it assigns the
// two sides to the opposite directions.
//
// In version 2 the side that pulls holds agency in StIdle and runs as the
// connection initiator, which is gouroboros's client role; the side that
// serves answers and runs as the responder, the server role. That is the
// reverse of TxSubmission2, and it is what removes version 1's Init message.

const (
	sigProtocolName        = "dmq-message-submission"
	sigProtocolID   uint16 = 11
	// defaultMaxUnacked bounds the message IDs this side leaves outstanding
	// when it pulls.
	defaultMaxUnacked = 100
	// sigReplyByteLimit bounds a reply that is received or sent. A reply
	// names at most 65535 items, each a few dozen bytes for an ID and at most a
	// couple of thousand for a message.
	sigReplyByteLimit = 4 << 20
	// sigRequestByteLimit bounds a request that is received or sent. A request
	// for message bodies names at most the IDs a peer holds outstanding, a
	// thousand of them at 34 bytes each at the most.
	sigRequestByteLimit = 256 << 10
	// sigServerReadLimit bounds the bytes the serving side holds for a message
	// still arriving, which is only ever a run of requests. The pulling side
	// holds a reply and the start of the next, so twice a reply.
	sigServerReadLimit = 4 * sigRequestByteLimit
	sigPullerReadLimit = 2 * sigReplyByteLimit
)

var (
	sigStateIdle           = protocol.NewState(1, "Idle")
	sigStateIDsBlocking    = protocol.NewState(2, "IDsBlocking")
	sigStateIDsNonBlocking = protocol.NewState(3, "IDsNonBlocking")
	sigStateSigs           = protocol.NewState(4, "Sigs")
	sigStateDone           = protocol.NewState(5, "Done")
)

// sigTimeouts bounds how long the pulling side waits for each kind of reply.
// Zero waits indefinitely.
type sigTimeouts struct {
	blocking    time.Duration
	nonBlocking time.Duration
	sigs        time.Duration
}

// sigStateMap is the version 2 state machine. When tolerateInboundPipelining
// is set, the messages a pulling peer may send ahead of its view of the state
// machine are accepted while this side still owes a reply, and held until it
// has sent it. That is how dmq-node's pulling side keeps requests in flight.
func sigStateMap(
	t sigTimeouts,
	tolerateInboundPipelining bool,
) protocol.StateMap {
	var pipelined []uint8
	if tolerateInboundPipelining {
		pipelined = []uint8{sigMsgRequestIDs, sigMsgRequestSigs, sigMsgDone}
	}
	isBlocking := func(_ any, msg protocol.Message) bool {
		req, ok := msg.(*msgRequestIDs)
		return ok && req.Blocking
	}
	isNonBlocking := func(_ any, msg protocol.Message) bool {
		req, ok := msg.(*msgRequestIDs)
		return ok && !req.Blocking
	}
	reply := func(
		timeout time.Duration,
		transitions ...protocol.StateTransition,
	) protocol.StateMapEntry {
		return protocol.StateMapEntry{
			Agency:                  protocol.AgencyServer,
			Timeout:                 timeout,
			PendingMessageByteLimit: sigReplyByteLimit,
			PipelinedMessageTypes:   pipelined,
			Transitions:             transitions,
		}
	}
	return protocol.StateMap{
		sigStateIdle: protocol.StateMapEntry{
			Agency:                  protocol.AgencyClient,
			PendingMessageByteLimit: sigRequestByteLimit,
			Transitions: []protocol.StateTransition{
				{
					MsgType:   sigMsgRequestIDs,
					NewState:  sigStateIDsBlocking,
					MatchFunc: isBlocking,
				},
				{
					MsgType:   sigMsgRequestIDs,
					NewState:  sigStateIDsNonBlocking,
					MatchFunc: isNonBlocking,
				},
				{MsgType: sigMsgRequestSigs, NewState: sigStateSigs},
				{MsgType: sigMsgDone, NewState: sigStateDone},
			},
		},
		sigStateIDsBlocking: reply(
			t.blocking,
			protocol.StateTransition{
				MsgType: sigMsgReplyIDs, NewState: sigStateIdle,
			},
			protocol.StateTransition{
				MsgType: sigMsgReplyNoIDs, NewState: sigStateIdle,
			},
		),
		sigStateIDsNonBlocking: reply(
			t.nonBlocking,
			protocol.StateTransition{
				MsgType: sigMsgReplyIDs, NewState: sigStateIdle,
			},
		),
		sigStateSigs: reply(
			t.sigs,
			protocol.StateTransition{
				MsgType: sigMsgReplySigs, NewState: sigStateIdle,
			},
		),
		sigStateDone: protocol.StateMapEntry{Agency: protocol.AgencyNone},
	}
}

// idRequest is a request for message IDs that has not been answered.
type idRequest struct {
	blocking bool
	ack      int
	req      int
}

// sigRequest is a request for message bodies that has not been answered.
type sigRequest struct {
	ids map[string]struct{}
}

func invalidSigMessage(format string, args ...any) error {
	return fmt.Errorf(
		"%s: %s: %w",
		sigProtocolName,
		fmt.Sprintf(format, args...),
		protocol.ErrProtocolViolationInvalidMessage,
	)
}

func exceededSigRequest(format string, args ...any) error {
	return fmt.Errorf(
		"%s: %s: %w",
		sigProtocolName,
		fmt.Sprintf(format, args...),
		protocol.ErrProtocolViolationRequestExceeded,
	)
}

// validateIDRequest applies the protocol's rules for a request for IDs when
// outstanding IDs are announced and not yet acknowledged: it may acknowledge
// no more than are outstanding; it is blocking exactly when none would remain;
// it asks for at least one ID unless it is non-blocking and acknowledges some;
// and what remains plus what it asks for stays within limit.
func validateIDRequest(
	outstanding [][]byte,
	r idRequest,
	limit int,
) error {
	if r.ack < 0 || r.ack > len(outstanding) {
		return exceededSigRequest(
			"acknowledgement %d exceeds %d outstanding message IDs",
			r.ack, len(outstanding),
		)
	}
	if r.req < 0 ||
		(r.blocking && r.req == 0) ||
		(!r.blocking && r.ack == 0 && r.req == 0) {
		return exceededSigRequest("invalid message ID request counts")
	}
	remaining := len(outstanding) - r.ack
	if r.blocking && remaining != 0 {
		return exceededSigRequest(
			"blocking request leaves %d message IDs unacknowledged",
			remaining,
		)
	}
	if !r.blocking && remaining == 0 {
		return exceededSigRequest(
			"non-blocking request leaves no message IDs outstanding",
		)
	}
	if remaining+r.req > limit {
		return exceededSigRequest(
			"request would leave %d message IDs outstanding, limit %d",
			remaining+r.req, limit,
		)
	}
	return nil
}

// reconcileIDs returns the outstanding IDs once the request is acknowledged
// and the reply's IDs are added. The reply may not name more than were asked,
// repeat an outstanding ID, or be empty for a blocking request.
func reconcileIDs(
	outstanding [][]byte,
	r idRequest,
	reply []ocommon.MessageIDAndSize,
	limit int,
) ([][]byte, error) {
	if err := validateIDRequest(outstanding, r, limit); err != nil {
		return nil, err
	}
	if len(reply) > r.req {
		return nil, exceededSigRequest(
			"reply contains %d message IDs for a request of %d",
			len(reply), r.req,
		)
	}
	if r.blocking && len(reply) == 0 {
		return nil, invalidSigMessage(
			"blocking request for %d message IDs received an empty reply",
			r.req,
		)
	}
	remaining := outstanding[r.ack:]
	seen := make(map[string]struct{}, len(remaining)+len(reply))
	next := make([][]byte, 0, len(remaining)+len(reply))
	for _, id := range remaining {
		seen[string(id)] = struct{}{}
		next = append(next, append([]byte(nil), id...))
	}
	for _, item := range reply {
		if err := checkMessageID(item.MessageID); err != nil {
			return nil, invalidSigMessage("%v", err)
		}
		key := string(item.MessageID)
		if _, dup := seen[key]; dup {
			return nil, invalidSigMessage(
				"reply repeats outstanding message ID %x", item.MessageID,
			)
		}
		seen[key] = struct{}{}
		next = append(next, append([]byte(nil), item.MessageID...))
	}
	return next, nil
}

// acknowledgeIDs returns the outstanding IDs once the request's
// acknowledgement is applied, for a reply that announces nothing.
func acknowledgeIDs(outstanding [][]byte, r idRequest) [][]byte {
	next := make([][]byte, 0, len(outstanding)-r.ack)
	for _, id := range outstanding[r.ack:] {
		next = append(next, append([]byte(nil), id...))
	}
	return next
}

// requestedSigsOutstanding checks a request for message bodies: it may name
// only IDs that are outstanding, and each only once.
func requestedSigsOutstanding(
	outstanding [][]byte,
	requested [][]byte,
) (*sigRequest, error) {
	if len(requested) > len(outstanding) {
		return nil, exceededSigRequest(
			"request names %d message IDs with only %d outstanding",
			len(requested), len(outstanding),
		)
	}
	available := make(map[string]struct{}, len(outstanding))
	for _, id := range outstanding {
		available[string(id)] = struct{}{}
	}
	ids := make(map[string]struct{}, len(requested))
	for _, id := range requested {
		if err := checkMessageID(id); err != nil {
			return nil, invalidSigMessage("%v", err)
		}
		key := string(id)
		if _, dup := ids[key]; dup {
			return nil, invalidSigMessage(
				"request names message ID %x twice", id,
			)
		}
		if _, ok := available[key]; !ok {
			return nil, invalidSigMessage(
				"request names unannounced message ID %x", id,
			)
		}
		ids[key] = struct{}{}
	}
	return &sigRequest{ids: ids}, nil
}

// validateSigReply checks a reply carrying message bodies: no more than were
// asked for, each asked for, none twice.
func validateSigReply(req *sigRequest, msgs []ocommon.DmqMessage) error {
	if req == nil {
		return invalidSigMessage("message reply without a request")
	}
	if len(msgs) > len(req.ids) {
		return exceededSigRequest(
			"reply contains %d messages for a request of %d",
			len(msgs), len(req.ids),
		)
	}
	seen := make(map[string]struct{}, len(msgs))
	for i := range msgs {
		id := msgs[i].ID()
		if err := checkMessageID(id); err != nil {
			return invalidSigMessage("%v", err)
		}
		key := string(id)
		if _, ok := req.ids[key]; !ok {
			return invalidSigMessage(
				"reply contains unrequested message ID %x", id,
			)
		}
		if _, dup := seen[key]; dup {
			return invalidSigMessage(
				"reply contains message ID %x twice", id,
			)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func cloneIDs(ids [][]byte) [][]byte {
	out := make([][]byte, len(ids))
	for i, id := range ids {
		out[i] = append([]byte(nil), id...)
	}
	return out
}

// sigPullerConfig configures the pulling side.
type sigPullerConfig struct {
	// maxUnacked bounds the message IDs announced and not yet acknowledged.
	maxUnacked int
	timeouts   sigTimeouts
	// onReplyIDs receives the IDs a reply announced. A reply that announced
	// none, whether an empty non-blocking one or MsgReplyNoIDs, passes none.
	onReplyIDs func(ids []ocommon.MessageIDAndSize)
	// onReplySigs receives the bodies a reply carried.
	onReplySigs func(msgs []ocommon.DmqMessage)
	// stateMap replaces the version 2 state machine; tests use it to let the
	// puller pipeline as dmq-node's does.
	stateMap protocol.StateMap
}

// sigPuller asks the peer which messages it holds and fetches the ones this
// side lacks. It enforces the protocol on the replies it gets and on the
// requests it sends.
type sigPuller struct {
	*protocol.Protocol
	cfg sigPullerConfig

	mu          sync.Mutex // guards the fields below
	window      [][]byte
	pendingIDs  *idRequest
	pendingSigs *sigRequest
}

func newSigPuller(
	opts protocol.ProtocolOptions,
	cfg sigPullerConfig,
) *sigPuller {
	p := &sigPuller{cfg: cfg}
	stateMap := cfg.stateMap
	if stateMap == nil {
		stateMap = sigStateMap(cfg.timeouts, false)
	}
	p.Protocol = protocol.New(protocol.ProtocolConfig{
		Name:                sigProtocolName,
		ProtocolId:          sigProtocolID,
		Muxer:               opts.Muxer,
		Logger:              opts.Logger,
		ErrorChan:           opts.ErrorChan,
		Mode:                opts.Mode,
		Role:                protocol.ProtocolRoleClient,
		MessageHandlerFunc:  p.handle,
		MessageFromCborFunc: newSigMsgFromCbor,
		StateMap:            stateMap,
		InitialState:        sigStateIdle,
		MaxReadBufferSize:   sigPullerReadLimit,
	})
	return p
}

// RequestIDs acknowledges ack of the announced IDs and asks for up to req
// more. It is blocking exactly when nothing would remain outstanding.
func (p *sigPuller) RequestIDs(blocking bool, ack, req uint16) error {
	r := idRequest{blocking: blocking, ack: int(ack), req: int(req)}
	p.mu.Lock()
	if p.pendingIDs != nil || p.pendingSigs != nil {
		p.mu.Unlock()
		return errors.New("a request is already in flight")
	}
	if err := validateIDRequest(p.window, r, p.cfg.maxUnacked); err != nil {
		p.mu.Unlock()
		return err
	}
	p.pendingIDs = &r
	p.mu.Unlock()
	if err := p.SendMessage(newMsgRequestIDs(blocking, ack, req)); err != nil {
		p.mu.Lock()
		p.pendingIDs = nil
		p.mu.Unlock()
		return err
	}
	return nil
}

// RequestSigs asks for the bodies of announced messages.
func (p *sigPuller) RequestSigs(ids [][]byte) error {
	ids = cloneIDs(ids)
	p.mu.Lock()
	if p.pendingIDs != nil || p.pendingSigs != nil {
		p.mu.Unlock()
		return errors.New("a request is already in flight")
	}
	req, err := requestedSigsOutstanding(p.window, ids)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	p.pendingSigs = req
	p.mu.Unlock()
	if err := p.SendMessage(newMsgRequestSigs(ids)); err != nil {
		p.mu.Lock()
		p.pendingSigs = nil
		p.mu.Unlock()
		return err
	}
	return nil
}

// Done ends the protocol. It can only be sent while no request is pending.
func (p *sigPuller) Done() error {
	return p.SendMessage(newMsgDone())
}

func (p *sigPuller) handle(msg protocol.Message) error {
	switch m := msg.(type) {
	case *msgReplyIDs:
		return p.handleReplyIDs(m)
	case *msgReplyNoIDs:
		return p.handleReplyNoIDs()
	case *msgReplySigs:
		return p.handleReplySigs(m)
	default:
		return fmt.Errorf(
			"%s: pulling side received unexpected message type %d",
			sigProtocolName, msg.Type(),
		)
	}
}

func (p *sigPuller) handleReplyIDs(m *msgReplyIDs) error {
	p.mu.Lock()
	req := p.pendingIDs
	if req == nil {
		p.mu.Unlock()
		return invalidSigMessage("message ID reply without a request")
	}
	next, err := reconcileIDs(p.window, *req, m.IDs, p.cfg.maxUnacked)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	p.window = next
	p.pendingIDs = nil
	p.mu.Unlock()
	if p.cfg.onReplyIDs != nil {
		p.cfg.onReplyIDs(m.IDs)
	}
	return nil
}

func (p *sigPuller) handleReplyNoIDs() error {
	p.mu.Lock()
	req := p.pendingIDs
	if req == nil || !req.blocking {
		p.mu.Unlock()
		return invalidSigMessage(
			"reply of no message IDs without a blocking request",
		)
	}
	p.window = acknowledgeIDs(p.window, *req)
	p.pendingIDs = nil
	p.mu.Unlock()
	if p.cfg.onReplyIDs != nil {
		p.cfg.onReplyIDs(nil)
	}
	return nil
}

func (p *sigPuller) handleReplySigs(m *msgReplySigs) error {
	p.mu.Lock()
	req := p.pendingSigs
	if err := validateSigReply(req, m.Msgs); err != nil {
		p.mu.Unlock()
		return err
	}
	p.pendingSigs = nil
	p.mu.Unlock()
	if p.cfg.onReplySigs != nil {
		p.cfg.onReplySigs(m.Msgs)
	}
	return nil
}

// sigServerConfig configures the serving side.
type sigServerConfig struct {
	// maxUnacked bounds the message IDs a peer may leave outstanding.
	maxUnacked int
	timeouts   sigTimeouts
	// onRequestIDs and onRequestSigs run when the peer asks. They run on the
	// protocol's goroutine, so one that waits does so elsewhere. The peer's
	// next request is held until the reply is sent.
	onRequestIDs  func(blocking bool, ack, req uint16)
	onRequestSigs func(ids [][]byte)
	// onDone runs when the peer ends its pulling.
	onDone func()
}

// sigServer announces the messages this side holds and returns the ones the
// peer asks for. It enforces the protocol on the requests it gets and on the
// replies it sends.
type sigServer struct {
	*protocol.Protocol
	cfg sigServerConfig

	mu          sync.Mutex // guards the fields below
	window      [][]byte
	pendingIDs  *idRequest
	pendingSigs *sigRequest
}

func newSigServer(
	opts protocol.ProtocolOptions,
	cfg sigServerConfig,
) *sigServer {
	s := &sigServer{cfg: cfg}
	s.Protocol = protocol.New(protocol.ProtocolConfig{
		Name:                sigProtocolName,
		ProtocolId:          sigProtocolID,
		Muxer:               opts.Muxer,
		Logger:              opts.Logger,
		ErrorChan:           opts.ErrorChan,
		Mode:                opts.Mode,
		Role:                protocol.ProtocolRoleServer,
		MessageHandlerFunc:  s.handle,
		MessageFromCborFunc: newSigMsgFromCbor,
		StateMap:            sigStateMap(cfg.timeouts, true),
		InitialState:        sigStateIdle,
		MaxReadBufferSize:   sigServerReadLimit,
		IngressLimit:        sigServerReadLimit,
	})
	return s
}

// ReplyIDs announces ids, which may be empty only for a non-blocking request.
func (s *sigServer) ReplyIDs(ids []ocommon.MessageIDAndSize) error {
	s.mu.Lock()
	req := s.pendingIDs
	if req == nil {
		s.mu.Unlock()
		return errors.New("no message ID request is outstanding")
	}
	next, err := reconcileIDs(s.window, *req, ids, s.cfg.maxUnacked)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.window = next
	s.pendingIDs = nil
	s.mu.Unlock()
	return s.SendMessage(newMsgReplyIDs(ids))
}

// ReplyNoIDs answers a blocking request without announcing anything.
func (s *sigServer) ReplyNoIDs() error {
	s.mu.Lock()
	req := s.pendingIDs
	if req == nil || !req.blocking {
		s.mu.Unlock()
		return errors.New("no blocking message ID request is outstanding")
	}
	s.window = acknowledgeIDs(s.window, *req)
	s.pendingIDs = nil
	s.mu.Unlock()
	return s.SendMessage(newMsgReplyNoIDs())
}

// ReplySigs sends the bodies of some or all of the requested messages.
func (s *sigServer) ReplySigs(msgs []ocommon.DmqMessage) error {
	s.mu.Lock()
	req := s.pendingSigs
	if req == nil {
		s.mu.Unlock()
		return errors.New("no message request is outstanding")
	}
	if err := validateSigReply(req, msgs); err != nil {
		s.mu.Unlock()
		return err
	}
	s.pendingSigs = nil
	s.mu.Unlock()
	return s.SendMessage(newMsgReplySigs(msgs))
}

func (s *sigServer) handle(msg protocol.Message) error {
	switch m := msg.(type) {
	case *msgRequestIDs:
		return s.handleRequestIDs(m)
	case *msgRequestSigs:
		return s.handleRequestSigs(m)
	case *msgDone:
		if s.cfg.onDone != nil {
			s.cfg.onDone()
		}
		return nil
	default:
		return fmt.Errorf(
			"%s: serving side received unexpected message type %d",
			sigProtocolName, msg.Type(),
		)
	}
}

func (s *sigServer) handleRequestIDs(m *msgRequestIDs) error {
	r := idRequest{blocking: m.Blocking, ack: int(m.Ack), req: int(m.Req)}
	s.mu.Lock()
	if s.pendingIDs != nil || s.pendingSigs != nil {
		s.mu.Unlock()
		return invalidSigMessage("a request is already pending")
	}
	if err := validateIDRequest(s.window, r, s.cfg.maxUnacked); err != nil {
		s.mu.Unlock()
		return err
	}
	s.pendingIDs = &r
	s.mu.Unlock()
	if s.cfg.onRequestIDs != nil {
		s.cfg.onRequestIDs(m.Blocking, m.Ack, m.Req)
	}
	return nil
}

func (s *sigServer) handleRequestSigs(m *msgRequestSigs) error {
	s.mu.Lock()
	if s.pendingIDs != nil || s.pendingSigs != nil {
		s.mu.Unlock()
		return invalidSigMessage("a request is already pending")
	}
	req, err := requestedSigsOutstanding(s.window, m.IDs)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.pendingSigs = req
	s.mu.Unlock()
	if s.cfg.onRequestSigs != nil {
		s.cfg.onRequestSigs(cloneIDs(m.IDs))
	}
	return nil
}
