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

package ouroboros

import (
	"errors"
	"fmt"
	"slices"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	oleiosvotes "github.com/blinklabs-io/gouroboros/protocol/leiosvotes"
)

const (
	leiosVoteCacheMaxEntries = 4096
	leiosVoteCacheTTL        = 10 * time.Minute
)

var errLeiosVotesUnavailable = errors.New("leios votes unavailable")

type leiosVoteData struct {
	vote       oleiosvotes.Vote
	insertedAt time.Time
	sequence   uint64
}

func (o *Ouroboros) leiosvotesServerConnOpts() []oleiosvotes.LeiosVotesOptionFunc {
	return []oleiosvotes.LeiosVotesOptionFunc{
		oleiosvotes.WithRequestNextFunc(
			o.instrumentLeiosvotesRequestNext(o.leiosvotesServerRequestNext),
		),
	}
}

func (o *Ouroboros) leiosvotesClientConnOpts() []oleiosvotes.LeiosVotesOptionFunc {
	return []oleiosvotes.LeiosVotesOptionFunc{
		oleiosvotes.WithRequestNextCount(1),
		oleiosvotes.WithVoteFunc(
			o.instrumentLeiosvotesVote(o.leiosvotesClientVote),
		),
	}
}

func (o *Ouroboros) leiosvotesClientStart(connId ouroboros.ConnectionId) error {
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf(
			"failed to lookup connection ID: %s",
			leiosConnectionIdString(connId),
		)
	}
	if conn.LeiosVotes() == nil || conn.LeiosVotes().Client == nil {
		return nil
	}
	return conn.LeiosVotes().Client.Sync()
}

func (o *Ouroboros) instrumentLeiosvotesRequestNext(
	fn func(oleiosvotes.CallbackContext, uint64) ([]oleiosvotes.Vote, error),
) func(oleiosvotes.CallbackContext, uint64) ([]oleiosvotes.Vote, error) {
	return func(
		ctx oleiosvotes.CallbackContext,
		count uint64,
	) ([]oleiosvotes.Vote, error) {
		start := time.Now()
		votes, err := fn(ctx, count)
		o.recordProtocolMessage("leiosvotes", err, time.Since(start))
		return votes, err
	}
}

func (o *Ouroboros) instrumentLeiosvotesVote(
	fn func(oleiosvotes.CallbackContext, oleiosvotes.Vote) error,
) func(oleiosvotes.CallbackContext, oleiosvotes.Vote) error {
	return func(
		ctx oleiosvotes.CallbackContext,
		vote oleiosvotes.Vote,
	) error {
		start := time.Now()
		err := fn(ctx, vote)
		o.recordProtocolMessage("leiosvotes", err, time.Since(start))
		return err
	}
}

func (o *Ouroboros) leiosvotesServerRequestNext(
	ctx oleiosvotes.CallbackContext,
	count uint64,
) ([]oleiosvotes.Vote, error) {
	done := (<-chan struct{})(nil)
	if ctx.Server != nil {
		done = ctx.Server.DoneChan()
	}
	votes, err := o.waitLeiosVotesForRequestNext(
		ctx.ConnectionId,
		count,
		done,
	)
	if err == nil {
		o.config.Logger.Debug(
			"serving leios votes",
			"protocol", "leios-votes",
			"connection_id", leiosConnectionIdString(ctx.ConnectionId),
			"count", count,
		)
		return votes, nil
	}
	o.config.Logger.Debug(
		"leios votes requested but unavailable",
		"protocol", "leios-votes",
		"connection_id", leiosConnectionIdString(ctx.ConnectionId),
		"count", count,
		"error", err,
	)
	return nil, err
}

func (o *Ouroboros) leiosvotesClientVote(
	ctx oleiosvotes.CallbackContext,
	vote oleiosvotes.Vote,
) error {
	if err := o.storeLeiosVote(vote); err != nil {
		return err
	}
	o.config.Logger.Debug(
		"received leios vote",
		"protocol", "leios-votes",
		"connection_id", leiosConnectionIdString(ctx.ConnectionId),
		"slot", vote.SlotNo,
		"voter_id", vote.VoterId,
		"endorser_block_hash", vote.EndorserBlockHash.String(),
	)
	return nil
}

func cloneLeiosVote(vote oleiosvotes.Vote) oleiosvotes.Vote {
	ret := vote
	ret.VoteSignature = slices.Clone(vote.VoteSignature)
	if raw := vote.Cbor(); len(raw) > 0 {
		ret.SetCbor(slices.Clone(raw))
	} else {
		ret.SetCbor(nil)
	}
	return ret
}

func leiosVoteKey(vote oleiosvotes.Vote) string {
	return fmt.Sprintf(
		"%d:%s:%d",
		vote.SlotNo,
		vote.EndorserBlockHash.String(),
		vote.VoterId,
	)
}

func (data *leiosVoteData) expired(now time.Time) bool {
	return data != nil &&
		data.insertedAt.Before(now.Add(-leiosVoteCacheTTL))
}

func (o *Ouroboros) storeLeiosVote(vote oleiosvotes.Vote) error {
	if err := vote.Validate(); err != nil {
		return err
	}
	key := leiosVoteKey(vote)
	now := time.Now()
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	if o.leiosVotes == nil {
		o.leiosVotes = make(map[string]*leiosVoteData)
	}
	if o.leiosVoteCursors == nil {
		o.leiosVoteCursors = make(map[string]uint64)
	}
	if data, ok := o.leiosVotes[key]; ok {
		data.vote = cloneLeiosVote(vote)
		data.insertedAt = now
		return nil
	}
	o.leiosVoteSeq++
	o.leiosVotes[key] = &leiosVoteData{
		vote:       cloneLeiosVote(vote),
		insertedAt: now,
		sequence:   o.leiosVoteSeq,
	}
	o.leiosVoteOrder = append(o.leiosVoteOrder, key)
	o.pruneLeiosVoteCacheLocked(now)
	o.notifyLeiosVoteWaitersLocked()
	return nil
}

func (o *Ouroboros) notifyLeiosVoteWaitersLocked() {
	if o.leiosVoteNotify == nil {
		o.leiosVoteNotify = make(chan struct{})
		return
	}
	close(o.leiosVoteNotify)
	o.leiosVoteNotify = make(chan struct{})
}

func (o *Ouroboros) pruneLeiosVoteCacheLocked(now time.Time) {
	if len(o.leiosVotes) == 0 {
		o.leiosVoteOrder = nil
		return
	}
	liveOrder := o.leiosVoteOrder[:0]
	for _, key := range o.leiosVoteOrder {
		data, ok := o.leiosVotes[key]
		if !ok || data == nil {
			continue
		}
		if data.expired(now) {
			delete(o.leiosVotes, key)
			continue
		}
		liveOrder = append(liveOrder, key)
	}
	o.leiosVoteOrder = liveOrder
	if len(o.leiosVoteOrder) <= leiosVoteCacheMaxEntries {
		return
	}
	evictCount := len(o.leiosVoteOrder) - leiosVoteCacheMaxEntries
	for _, key := range o.leiosVoteOrder[:evictCount] {
		delete(o.leiosVotes, key)
	}
	o.leiosVoteOrder = o.leiosVoteOrder[evictCount:]
}

func (o *Ouroboros) leiosVotesForRequestNext(
	connId ouroboros.ConnectionId,
	count uint64,
) ([]oleiosvotes.Vote, error) {
	if count == 0 {
		return nil, errors.New("leios votes request count must be positive")
	}
	cursorKey := leiosConnectionIdString(connId)
	now := time.Now()
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	o.pruneLeiosVoteCacheLocked(now)
	return o.leiosVotesForRequestNextLocked(cursorKey, count)
}

func (o *Ouroboros) waitLeiosVotesForRequestNext(
	connId ouroboros.ConnectionId,
	count uint64,
	done <-chan struct{},
) ([]oleiosvotes.Vote, error) {
	if done == nil {
		return o.leiosVotesForRequestNext(connId, count)
	}
	if count == 0 {
		return nil, errors.New("leios votes request count must be positive")
	}
	cursorKey := leiosConnectionIdString(connId)
	for {
		now := time.Now()
		o.leiosMu.Lock()
		o.pruneLeiosVoteCacheLocked(now)
		votes, err := o.leiosVotesForRequestNextLocked(cursorKey, count)
		if err == nil || !errors.Is(err, errLeiosVotesUnavailable) {
			o.leiosMu.Unlock()
			return votes, err
		}
		notify := o.leiosVoteNotify
		if notify == nil {
			notify = make(chan struct{})
			o.leiosVoteNotify = notify
		}
		o.leiosMu.Unlock()
		select {
		case <-notify:
		case <-done:
			return nil, protocol.ErrProtocolShuttingDown
		}
	}
}

func (o *Ouroboros) leiosVotesForRequestNextLocked(
	cursorKey string,
	count uint64,
) ([]oleiosvotes.Vote, error) {
	cursor := o.leiosVoteCursors[cursorKey]
	votes := make([]oleiosvotes.Vote, 0, count)
	var lastSequence uint64
	for _, key := range o.leiosVoteOrder {
		data := o.leiosVotes[key]
		if data == nil || data.sequence <= cursor {
			continue
		}
		votes = append(votes, cloneLeiosVote(data.vote))
		lastSequence = data.sequence
		if uint64(len(votes)) == count {
			break
		}
	}
	if uint64(len(votes)) != count {
		return nil, fmt.Errorf(
			"%w: requested %d, available %d",
			errLeiosVotesUnavailable,
			count,
			len(votes),
		)
	}
	if o.leiosVoteCursors == nil {
		o.leiosVoteCursors = make(map[string]uint64)
	}
	o.leiosVoteCursors[cursorKey] = lastSequence
	return votes, nil
}

func (o *Ouroboros) leiosVotesForIds(
	voteIds []lcommon.LeiosVoteId,
) []oleiosvotes.Vote {
	if len(voteIds) == 0 {
		return nil
	}
	now := time.Now()
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	o.pruneLeiosVoteCacheLocked(now)
	ret := make([]oleiosvotes.Vote, 0, len(voteIds))
	for _, voteId := range voteIds {
		// gouroboros v0.180.0 vote IDs omit the EB hash. Preserve all
		// cached votes for the requested slot/voter pair instead of
		// collapsing competing EB votes here.
		for _, key := range o.leiosVoteOrder {
			data := o.leiosVotes[key]
			if data == nil ||
				data.vote.SlotNo != voteId.SlotNo ||
				data.vote.VoterId != voteId.VoterId {
				continue
			}
			ret = append(ret, cloneLeiosVote(data.vote))
		}
	}
	return ret
}
