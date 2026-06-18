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
	"fmt"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
)

func (o *Ouroboros) leiosfetchServerConnOpts() []oleiosfetch.LeiosFetchOptionFunc {
	return []oleiosfetch.LeiosFetchOptionFunc{
		oleiosfetch.WithBlockRequestFunc(
			o.instrumentLeiosfetchBlockRequest(o.leiosfetchServerBlockRequest),
		),
		oleiosfetch.WithBlockTxsRequestFunc(
			o.instrumentLeiosfetchBlockTxsRequest(o.leiosfetchServerBlockTxsRequest),
		),
		oleiosfetch.WithVotesRequestFunc(
			o.instrumentLeiosfetchVotesRequest(o.leiosfetchServerVotesRequest),
		),
		oleiosfetch.WithBlockRangeRequestFunc(
			o.instrumentLeiosfetchBlockRangeRequest(o.leiosfetchServerBlockRangeRequest),
		),
	}
}

func (o *Ouroboros) leiosfetchClientConnOpts() []oleiosfetch.LeiosFetchOptionFunc {
	return []oleiosfetch.LeiosFetchOptionFunc{
		// NOTE: this is purposely empty
	}
}

func (o *Ouroboros) instrumentLeiosfetchBlockRequest(
	fn func(oleiosfetch.CallbackContext, ocommon.Point) (protocol.Message, error),
) func(oleiosfetch.CallbackContext, ocommon.Point) (protocol.Message, error) {
	return func(
		ctx oleiosfetch.CallbackContext,
		point ocommon.Point,
	) (protocol.Message, error) {
		start := time.Now()
		msg, err := fn(ctx, point)
		o.recordProtocolMessage("leiosfetch", err, time.Since(start))
		return msg, err
	}
}

func (o *Ouroboros) instrumentLeiosfetchBlockTxsRequest(
	fn func(oleiosfetch.CallbackContext, ocommon.Point, map[uint16]uint64) (protocol.Message, error),
) func(oleiosfetch.CallbackContext, ocommon.Point, map[uint16]uint64) (protocol.Message, error) {
	return func(
		ctx oleiosfetch.CallbackContext,
		point ocommon.Point,
		txBitmap map[uint16]uint64,
	) (protocol.Message, error) {
		start := time.Now()
		msg, err := fn(ctx, point, txBitmap)
		o.recordProtocolMessage("leiosfetch", err, time.Since(start))
		return msg, err
	}
}

func (o *Ouroboros) instrumentLeiosfetchVotesRequest(
	fn func(oleiosfetch.CallbackContext, []oleiosfetch.MsgVotesRequestVoteId) (protocol.Message, error),
) func(oleiosfetch.CallbackContext, []oleiosfetch.MsgVotesRequestVoteId) (protocol.Message, error) {
	return func(
		ctx oleiosfetch.CallbackContext,
		voteIds []oleiosfetch.MsgVotesRequestVoteId,
	) (protocol.Message, error) {
		start := time.Now()
		msg, err := fn(ctx, voteIds)
		o.recordProtocolMessage("leiosfetch", err, time.Since(start))
		return msg, err
	}
}

func (o *Ouroboros) instrumentLeiosfetchBlockRangeRequest(
	fn func(oleiosfetch.CallbackContext, ocommon.Point, ocommon.Point) error,
) func(oleiosfetch.CallbackContext, ocommon.Point, ocommon.Point) error {
	return func(
		ctx oleiosfetch.CallbackContext,
		start ocommon.Point,
		end ocommon.Point,
	) error {
		startTime := time.Now()
		err := fn(ctx, start, end)
		o.recordProtocolMessage("leiosfetch", err, time.Since(startTime))
		return err
	}
}

func (o *Ouroboros) leiosfetchServerBlockRequest(
	ctx oleiosfetch.CallbackContext,
	point ocommon.Point,
) (protocol.Message, error) {
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	if !ok {
		return nil, fmt.Errorf(
			"leios endorser block not found: %d.%x",
			point.Slot,
			point.Hash,
		)
	}
	return oleiosfetch.NewMsgBlock(cbor.RawMessage(data.blockRaw)), nil
}

func (o *Ouroboros) leiosfetchServerBlockTxsRequest(
	ctx oleiosfetch.CallbackContext,
	point ocommon.Point,
	txBitmap map[uint16]uint64,
) (protocol.Message, error) {
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	if !ok {
		return nil, fmt.Errorf(
			"leios endorser block txs not found: %d.%x",
			point.Slot,
			point.Hash,
		)
	}
	if !data.completeTxCache() {
		return nil, fmt.Errorf(
			"leios endorser block tx cache incomplete: %d.%x: have %d txs for %d cached txs",
			point.Slot,
			point.Hash,
			len(data.txsRaw),
			data.txCount,
		)
	}
	if err := validateLeiosTxBitmap(len(data.txsRaw), txBitmap); err != nil {
		return nil, err
	}
	return oleiosfetch.NewMsgBlockTxs(
		leiosTxsFromBitmap(data.txsRaw, txBitmap),
	), nil
}

func (o *Ouroboros) leiosfetchServerVotesRequest(
	ctx oleiosfetch.CallbackContext,
	voteIds []oleiosfetch.MsgVotesRequestVoteId,
) (protocol.Message, error) {
	if o.LeiosVotes == nil {
		return nil, errLeiosVotesUnavailable
	}
	// MsgVotesRequestVoteId aliases lcommon.LeiosVoteId; unknown ids
	// are omitted from the response.
	return oleiosfetch.NewMsgVotes(o.LeiosVotes.VotesByIds(voteIds)), nil
}

func (o *Ouroboros) leiosfetchServerBlockRangeRequest(
	ctx oleiosfetch.CallbackContext,
	start ocommon.Point,
	end ocommon.Point,
) error {
	// TODO
	return nil
}
