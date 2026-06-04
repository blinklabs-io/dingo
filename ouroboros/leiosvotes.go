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
	"time"

	oleiosvotes "github.com/blinklabs-io/gouroboros/protocol/leiosvotes"
)

var errLeiosVotesUnavailable = errors.New("leios votes unavailable")

func (o *Ouroboros) leiosvotesServerConnOpts() []oleiosvotes.LeiosVotesOptionFunc {
	return []oleiosvotes.LeiosVotesOptionFunc{
		oleiosvotes.WithRequestNextFunc(
			o.instrumentLeiosvotesRequestNext(o.leiosvotesServerRequestNext),
		),
	}
}

func (o *Ouroboros) leiosvotesClientConnOpts() []oleiosvotes.LeiosVotesOptionFunc {
	return []oleiosvotes.LeiosVotesOptionFunc{
		oleiosvotes.WithVoteFunc(
			o.instrumentLeiosvotesVote(o.leiosvotesClientVote),
		),
	}
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
	o.config.Logger.Debug(
		"leios votes requested but vote store is not implemented",
		"protocol", "leios-votes",
		"connection_id", leiosConnectionIdString(ctx.ConnectionId),
		"count", count,
	)
	return nil, errLeiosVotesUnavailable
}

func (o *Ouroboros) leiosvotesClientVote(
	ctx oleiosvotes.CallbackContext,
	vote oleiosvotes.Vote,
) error {
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
