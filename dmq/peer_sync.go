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

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// The message submission protocol has two sides per direction. The serving
// side holds messages and answers requests; the pulling side asks what the
// other holds and fetches what it lacks. Which side this stack plays on a
// connection is decided in startProtocols.

// onRequestIDs answers the peer's request for IDs from this stack's pool. A
// blocking request waits for something to announce, so it does so on a
// goroutine rather than holding up the protocol.
func (p *PeerConn) onRequestIDs(blocking bool, _, count uint16) {
	p.spawn(func() { p.answerMessageIDs(blocking, int(count)) })
}

// answerMessageIDs replies to a request for up to limit IDs. A blocking
// request needs at least one ID, and an empty list is a protocol violation, so
// it waits for one for blockingReplyWait. After that it answers with
// MsgReplyNoIDs, because the peer ends the connection if a blocking request
// goes unanswered for 20 seconds, and the peer asks again at once.
func (p *PeerConn) answerMessageIDs(blocking bool, limit int) {
	var deadline <-chan time.Time
	if blocking {
		timer := time.NewTimer(p.tune.blockingReplyWait)
		defer timer.Stop()
		deadline = timer.C
	}
	for {
		// Take the signal before reading the pool, so a message admitted in
		// between still ends the wait.
		added := p.stack.pool.AddedSignal()
		ids := p.nextMessageIDs(limit)
		if len(ids) > 0 || !blocking {
			if err := p.server.ReplyIDs(ids); err != nil {
				p.failSend("reply message IDs", err)
			}
			return
		}
		select {
		case <-added:
		case <-deadline:
			if err := p.server.ReplyNoIDs(); err != nil {
				p.failSend("reply no message IDs", err)
			}
			return
		case <-p.ctx.Done():
			return
		}
	}
}

// nextMessageIDs takes up to limit messages the peer has not been told about
// from the peer's cursor over the pool. Expired messages, and messages the
// peer itself announced, are passed over: the cursor still moves past them.
func (p *PeerConn) nextMessageIDs(limit int) []ocommon.MessageIDAndSize {
	now := p.stack.cfg.Now()
	ids := make([]ocommon.MessageIDAndSize, 0, limit)
	for len(ids) < limit {
		msg, ok := p.stack.pool.NextForPeer(p.cursorKey)
		if !ok {
			break
		}
		if !msg.IsValidAt(now) {
			continue
		}
		id := msg.ID()
		if p.known.has(id) {
			continue
		}
		raw, err := msg.MarshalCBOR()
		if err != nil {
			p.logger.Warn("dmq peer: encode message", "error", err)
			continue
		}
		ids = append(ids, ocommon.MessageIDAndSize{
			MessageID: id,
			// #nosec G115 -- a message is far smaller than 4 GiB
			SizeInBytes: uint32(len(raw)),
		})
	}
	return ids
}

// onRequestSigs sends the bodies of the requested messages that the pool
// still holds. A message may have expired since it was announced, so the
// reply can name fewer than were requested, which the protocol allows.
func (p *PeerConn) onRequestSigs(ids [][]byte) {
	p.spawn(func() {
		now := p.stack.cfg.Now()
		msgs := make([]ocommon.DmqMessage, 0, len(ids))
		for _, id := range ids {
			msg, ok := p.stack.pool.Get(id)
			if !ok || !msg.IsValidAt(now) {
				continue
			}
			msgs = append(msgs, msg)
		}
		if err := p.server.ReplySigs(msgs); err != nil {
			p.failSend("reply messages", err)
			return
		}
		p.stack.sent.Add(float64(len(msgs)))
	})
}

// onDone runs when the peer ends its pulling. It no longer reads from this
// stack, so its cursor is released.
func (p *PeerConn) onDone() {
	p.stack.pool.RemovePeer(p.cursorKey)
	p.logger.Debug("dmq peer stopped pulling")
}

// onReplyIDs hands the peer's announced IDs to the pulling loop, and
// remembers them so this stack does not announce them back.
func (p *PeerConn) onReplyIDs(ids []ocommon.MessageIDAndSize) {
	for _, item := range ids {
		p.known.add(item.MessageID)
	}
	select {
	case p.idReplies <- ids:
	default:
		// One request is outstanding at a time, so a second reply means the
		// protocol's bookkeeping and this loop disagree.
		p.fail(errors.New("dmq peer: unexpected message ID reply"))
	}
}

func (p *PeerConn) onReplySigs(msgs []ocommon.DmqMessage) {
	select {
	case p.msgReplies <- msgs:
	default:
		p.fail(errors.New("dmq peer: unexpected message reply"))
	}
}

// announced is a message ID the peer announced and this stack has not yet
// acknowledged.
type announced struct {
	id   []byte
	size uint32
	// handled is set once the message was fetched, or was not needed.
	handled bool
}

// The pulling loop waits between blocking requests that a peer answers with
// nothing, starting at emptyReplyBackoff and doubling to maxEmptyReplyBackoff,
// so a peer that answers at once cannot keep it spinning.
const (
	emptyReplyBackoff    = 100 * time.Millisecond
	maxEmptyReplyBackoff = 2 * time.Second
)

// pull asks the peer what it holds and fetches what this stack lacks.
//
// IDs stay in window from the moment they are announced until the next
// request acknowledges them, in announcement order, which is the order the
// protocol acknowledges in. A request is blocking exactly when window is
// empty after the acknowledgement, because the protocol demands that; with
// IDs still outstanding, such as when a fetch left some for the next round,
// it is non-blocking and so does not wait for the peer to have something new.
func (p *PeerConn) pull() {
	defer close(p.pullDone)
	if gate := p.tune.pullGate; gate != nil {
		select {
		case <-gate:
		case <-p.ctx.Done():
			return
		}
	}
	window := make([]announced, 0, p.tune.maxUnacked)
	emptyReplies := 0
	for {
		// A finished protocol means this side ended its pulling or the peer
		// ended the connection's serving. The pulling stops, and the connection
		// stays up for whichever direction still runs. A request sent now
		// would never reach the wire.
		if p.ctx.Err() != nil || p.puller.IsDone() {
			return
		}
		ack := 0
		for ack < len(window) && window[ack].handled {
			ack++
		}
		window = window[ack:]
		outstanding := len(window)
		want := min(p.tune.maxUnacked-outstanding, p.tune.idBatch)
		if want > 0 || ack > 0 {
			// #nosec G115 -- both are bounded by maxUnacked
			ackCount, wantCount := uint16(ack), uint16(want)
			blocking := outstanding == 0
			if blocking {
				p.blockingRequests.Add(1)
			} else {
				p.nonBlockingRequests.Add(1)
			}
			if err := p.puller.RequestIDs(
				blocking, ackCount, wantCount,
			); err != nil {
				p.failSend("request message IDs", err)
				return
			}
			ids, ok := p.awaitMessageIDs()
			if !ok {
				return
			}
			for _, item := range ids {
				window = append(window, announced{
					id:   item.MessageID,
					size: item.SizeInBytes,
				})
			}
			if blocking && len(ids) == 0 {
				emptyReplies++
				if !p.waitEmptyReplyBackoff(emptyReplies) {
					return
				}
			} else {
				emptyReplies = 0
			}
		}
		fetch := p.nextFetch(window)
		if len(fetch) == 0 {
			continue
		}
		ids := make([][]byte, 0, len(fetch))
		for _, i := range fetch {
			ids = append(ids, window[i].id)
		}
		if err := p.puller.RequestSigs(ids); err != nil {
			p.failSend("request messages", err)
			return
		}
		msgs, ok := p.awaitMessages()
		if !ok {
			return
		}
		if err := p.stack.acceptFromPeer(msgs); err != nil {
			p.fail(err)
			return
		}
		// A requested message the peer did not send has expired or been
		// evicted there, and will not be asked for again.
		for _, i := range fetch {
			window[i].handled = true
		}
	}
}

// waitEmptyReplyBackoff waits out the pause after the nth empty reply in a
// row. It reports false if the connection closed first.
func (p *PeerConn) waitEmptyReplyBackoff(n int) bool {
	wait := emptyReplyBackoff
	for range min(n-1, 5) {
		wait = min(2*wait, maxEmptyReplyBackoff)
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-p.ctx.Done():
		return false
	}
}

// nextFetch picks the announced messages to fetch next, as indexes into
// window: the first not-yet-handled ones, within the message and byte limits.
// The first is always taken, so a single message larger than the byte limit
// is still fetched. An announced message that arrived from another peer in
// the meantime is marked handled and skipped.
func (p *PeerConn) nextFetch(window []announced) []int {
	var picked []int
	var bytes uint64
	for i := range window {
		entry := &window[i]
		if entry.handled {
			continue
		}
		if p.stack.pool.Has(entry.id) {
			entry.handled = true
			continue
		}
		if len(picked) >= p.tune.fetchMessages {
			break
		}
		if len(picked) > 0 && bytes+uint64(entry.size) > p.tune.fetchBytes {
			break
		}
		bytes += uint64(entry.size)
		picked = append(picked, i)
	}
	return picked
}

func (p *PeerConn) awaitMessageIDs() ([]ocommon.MessageIDAndSize, bool) {
	return awaitReply(p, p.idReplies)
}

func (p *PeerConn) awaitMessages() ([]ocommon.DmqMessage, bool) {
	return awaitReply(p, p.msgReplies)
}

// doneCheckInterval is how often a pulling loop that is waiting for a reply
// checks whether its protocol has ended. A protocol that reaches its terminal
// state signals nothing a waiter can select on, so the check is a poll.
const doneCheckInterval = 250 * time.Millisecond

// awaitReply waits for the next reply on replies. It gives up, reporting
// false, when the connection is closing or the protocol has ended.
func awaitReply[T any](p *PeerConn, replies <-chan T) (T, bool) {
	ticker := time.NewTicker(doneCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case reply := <-replies:
			return reply, true
		case <-p.ctx.Done():
		case <-ticker.C:
			if !p.puller.IsDone() {
				continue
			}
		}
		var none T
		return none, false
	}
}

// acceptFromPeer admits one reply's messages through the same checks a local
// submission gets. A message that is expired, already held, or refused for a
// reason that is not the peer's doing, such as a full pool, is counted and
// dropped. An invalid one is a protocol violation: the error ends the
// connection, and none of the reply is admitted.
//
// Signature verification dominates admission cost, so the reply is
// authenticated as one batch at one current slot, a message already held is
// dropped before it is verified, and a lookup that fails for want of chain
// state ends the batch rather than being repeated for every message.
func (s *Stack) acceptFromPeer(msgs []ocommon.DmqMessage) error {
	// The KES period comes from the same chain state as the slot, so it too
	// is resolved once, and a failure to resolve it is not retried.
	kesPeriod := s.cfg.CurrentKESPeriod
	if kesPeriod != nil {
		kesPeriod = sync.OnceValues(kesPeriod)
	}
	batch := msgs[:0]
	for i := range msgs {
		msg := &msgs[i]
		reason := s.precheck(msg, kesPeriod)
		if reason == nil && s.pool.Has(msg.ID()) {
			reason = ocommon.AlreadyReceivedReason{}
		}
		if reason != nil {
			if err := s.refuseFromPeer(reason); err != nil {
				return err
			}
			continue
		}
		batch = append(batch, *msg)
	}
	commit, err := s.cfg.Authenticator.PrepareMessages(batch)
	if err == nil {
		err = commit()
	}
	if err != nil {
		reason := authReason(err)
		if _, invalid := reason.(ocommon.InvalidReason); invalid {
			return s.refuseFromPeer(reason)
		}
		s.failures.WithLabelValues(reasonLabel(reason)).Add(float64(len(batch)))
		s.logger.Debug(
			"dmq peer messages refused", "count", len(batch), "reason", reason,
		)
		return nil
	}
	for i := range batch {
		if reason := s.addToPool(&batch[i]); reason != nil {
			if err := s.refuseFromPeer(reason); err != nil {
				return err
			}
		}
	}
	return nil
}

// refuseFromPeer counts a refused message, returning an error only when the
// refusal is a protocol violation.
func (s *Stack) refuseFromPeer(reason ocommon.RejectReason) error {
	s.failures.WithLabelValues(reasonLabel(reason)).Inc()
	s.logger.Debug("dmq peer message refused", "reason", reason)
	if invalid, ok := reason.(ocommon.InvalidReason); ok {
		return fmt.Errorf(
			"dmq peer: sent an invalid message: %s", invalid.Message,
		)
	}
	return nil
}
