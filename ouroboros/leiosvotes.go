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
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	oleiosvotes "github.com/blinklabs-io/gouroboros/protocol/leiosvotes"
)

var errLeiosVotesUnavailable = errors.New("leios votes unavailable")

func (o *Ouroboros) leiosvotesServerConnOpts() []oleiosvotes.LeiosVotesOptionFunc {
	return []oleiosvotes.LeiosVotesOptionFunc{
		oleiosvotes.WithRequestNextFunc(
			o.instrumentLeiosvotesRequestNext(o.leiosvotesServerRequestNext),
		),
		oleiosvotes.WithTimeout(oleiosvotes.DefaultTimeout),
	}
}

func (o *Ouroboros) leiosvotesClientConnOpts() []oleiosvotes.LeiosVotesOptionFunc {
	return []oleiosvotes.LeiosVotesOptionFunc{
		oleiosvotes.WithVoteFunc(
			o.instrumentLeiosvotesVote(o.leiosvotesClientVote),
		),
		// Stream votes one at a time with pipelined requests so a
		// peer with few votes does not stall a large batch request.
		oleiosvotes.WithRequestNextCount(1),
		oleiosvotes.WithPipelineLimit(16),
		oleiosvotes.WithTimeout(oleiosvotes.DefaultTimeout),
	}
}

// leiosvotesClientStart begins the vote sync loop toward a peer.
func (o *Ouroboros) leiosvotesClientStart(
	connId ouroboros.ConnectionId,
) error {
	conn := o.connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf(
			"failed to lookup connection ID: %s",
			leiosConnectionIdString(connId),
		)
	}
	if conn.LeiosVotes() == nil || conn.LeiosVotes().Client == nil {
		// Return silently if LeiosVotes protocol is not supported by peer
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
	if o.leiosVotes == nil {
		o.config.Logger.Debug(
			"leios votes requested but vote manager is not wired",
			"protocol", "leios-votes",
			"connection_id", leiosConnectionIdString(ctx.ConnectionId),
			"count", count,
		)
		return nil, errLeiosVotesUnavailable
	}
	// The protocol requires exactly count votes per request, so the
	// vote manager blocks until enough votes are available. Observe the
	// stop request because this callback runs on a protocol loop that must
	// return before DoneChan can close.
	done := (<-chan struct{})(nil)
	if ctx.Server != nil {
		if protocol := ctx.Server.ProtocolInstance(); protocol != nil {
			done = protocol.StopChan()
		}
	}
	connectionDone := make(chan struct{})
	o.registerLeiosVoteWait(ctx.ConnectionId, connectionDone)
	defer o.unregisterLeiosVoteWait(ctx.ConnectionId, connectionDone)
	combinedDone := make(chan struct{})
	cancelCombine := make(chan struct{})
	defer close(cancelCombine)
	go func() {
		select {
		case <-done:
		case <-connectionDone:
		case <-cancelCombine:
			return
		}
		close(combinedDone)
	}()
	return o.leiosVotes.NextVotes(
		combinedDone,
		leiosConnectionIdString(ctx.ConnectionId),
		count,
	)
}

func (o *Ouroboros) registerLeiosVoteWait(
	connId ouroboros.ConnectionId,
	done chan struct{},
) {
	o.leiosVoteWaitMu.Lock()
	defer o.leiosVoteWaitMu.Unlock()
	if o.leiosVoteWaits == nil {
		o.leiosVoteWaits = make(map[ouroboros.ConnectionId]map[chan struct{}]struct{})
	}
	waits := o.leiosVoteWaits[connId]
	if waits == nil {
		waits = make(map[chan struct{}]struct{})
		o.leiosVoteWaits[connId] = waits
	}
	waits[done] = struct{}{}
}

func (o *Ouroboros) unregisterLeiosVoteWait(
	connId ouroboros.ConnectionId,
	done chan struct{},
) {
	o.leiosVoteWaitMu.Lock()
	defer o.leiosVoteWaitMu.Unlock()
	waits := o.leiosVoteWaits[connId]
	delete(waits, done)
	if len(waits) == 0 {
		delete(o.leiosVoteWaits, connId)
	}
}

func (o *Ouroboros) closeLeiosVoteWaits(connId ouroboros.ConnectionId) {
	o.leiosVoteWaitMu.Lock()
	defer o.leiosVoteWaitMu.Unlock()
	for done := range o.leiosVoteWaits[connId] {
		close(done)
	}
	delete(o.leiosVoteWaits, connId)
}

func (o *Ouroboros) leiosvotesClientVote(
	ctx oleiosvotes.CallbackContext,
	vote oleiosvotes.Vote,
) error {
	connId := leiosConnectionIdString(ctx.ConnectionId)
	o.config.Logger.Debug(
		"received leios vote",
		"protocol", "leios-votes",
		"connection_id", connId,
		"slot", vote.SlotNo,
		"voter_id", vote.VoterId,
		"endorser_block_hash", vote.EndorserBlockHash.String(),
	)
	if o.leiosVotes == nil {
		return nil
	}
	return o.leiosVotes.HandleVote(connId, vote)
}
