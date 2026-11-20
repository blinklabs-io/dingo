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
	"context"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
)

// maxLeiosFetchVoteIDs bounds vote-manager lookup and response cloning per
// request, matching the batch size supported by the standalone vote protocol.
// This is a local serving policy, not a LeiosFetch wire-format restriction.
const maxLeiosFetchVoteIDs = 1000

func (o *Ouroboros) leiosfetchServerConnOpts() []oleiosfetch.LeiosFetchOptionFunc {
	return []oleiosfetch.LeiosFetchOptionFunc{
		oleiosfetch.WithBlockRequestFunc(
			o.instrumentLeiosfetchBlockRequest(o.leiosfetchServerBlockRequest),
		),
		oleiosfetch.WithBlockTxsRequestFunc(
			o.instrumentLeiosfetchBlockTxsRequest(
				o.leiosfetchServerBlockTxsRequest,
			),
		),
		oleiosfetch.WithVotesRequestFunc(
			o.instrumentLeiosfetchVotesRequest(o.leiosfetchServerVotesRequest),
		),
		oleiosfetch.WithBlockRangeRequestFunc(
			o.instrumentLeiosfetchBlockRangeRequest(
				o.leiosfetchServerBlockRangeRequest,
			),
		),
	}
}

// leiosFetchResponseTimeout bounds how long the leios-fetch client waits for a
// server response (a Block or BlockTxs message) when the caller has no attempt
// deadline of its own, i.e. the tip-driven fetches. The protocol default is 5s,
// which is too short during a from-scratch catch-up: the connection's muxer is
// saturated by blockfetch pulling ranking blocks, so the relay's endorser-block
// response routinely arrives later than 5s. leios-notify is set to no timeout
// for the same reason (it long-polls for offers); leios-fetch gets a generous
// but finite bound so a genuinely unresponsive request still fails instead of
// hanging a fetch goroutine forever.
//
// NOTE: WithTimeout below does NOT reach the Block / BlockTxs states.
// gouroboros deliberately leaves those two out of the leios-fetch StateMap
// timeouts, because a state timeout there fires SendError and tears down every
// mini-protocol on the shared bearer. leiosFetchRequestContext is therefore the
// only thing bounding a request that receives no response at all, so every
// leios-fetch client call must pass a bounded context. Keep this no greater
// than the by-point backfill attempt budget.
const leiosFetchResponseTimeout = leiosBackfillPerAttemptTimeout

func (o *Ouroboros) leiosfetchClientConnOpts() []oleiosfetch.LeiosFetchOptionFunc {
	return []oleiosfetch.LeiosFetchOptionFunc{
		oleiosfetch.WithTimeout(leiosFetchResponseTimeout),
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

// leiosServingRefused reports whether point names an endorser block the
// reference node would have garbage-collected with its volatile database:
// one behind the immutable point, or behind the Leios store's collection
// watermark, that no certifying immutable ranking block has pinned. Such a
// block is answered as unknown even while it is still cached or persisted.
// The live immutable point leads the persisted watermark, which only moves
// when the collection worker runs. An unresolvable immutable point or pin
// lookup refuses nothing it cannot prove stale: serving an extra block is
// harmless.
func (o *Ouroboros) leiosServingRefused(point ocommon.Point) bool {
	db := o.leiosDatabase()
	if db == nil {
		return false
	}
	bound, ok := o.leiosServingBound(db)
	if !ok || point.Slot >= bound {
		return false
	}
	pinned, err := db.IsLeiosEBPinned(point.Slot, point.Hash)
	return err == nil && !pinned
}

// leiosServingBound is the slot below which an unpinned endorser block is
// refused to peers. With the pin walk wired it is the smaller of the live
// immutable slot and the pin watermark, the collector's own bound, raised to
// the collection watermark: an endorser block between the watermark and the
// immutable slot may be certified by a ranking block the walk has not reached,
// so refusal never depends on how far the walk has got. Without a pin walk
// nothing is ever pinned and the retention bound applies.
func (o *Ouroboros) leiosServingBound(db *database.Database) (uint64, bool) {
	if o.leiosPinAdvance == nil {
		return o.leiosRetentionBound(db)
	}
	store, err := db.LeiosDB()
	if err != nil {
		return 0, false
	}
	wm, err := store.Watermarks()
	if err != nil {
		return 0, false
	}
	var (
		bound uint64
		known bool
	)
	if o.leiosImmutableSlot != nil && wm.PinnedThroughSet {
		slot, found, err := o.leiosImmutableSlot(context.Background())
		if err == nil && found {
			bound, known = min(slot, wm.PinnedThrough.Slot), true
		}
	}
	if wm.GCBefore > 0 {
		bound, known = max(bound, wm.GCBefore), true
	}
	return bound, known
}

// leiosRetentionBound is the slot below which an unpinned endorser block is
// stale: the larger of the live immutable slot and the store's collection
// watermark. ok is false when neither is known.
func (o *Ouroboros) leiosRetentionBound(db *database.Database) (uint64, bool) {
	var (
		bound uint64
		known bool
	)
	if o.leiosImmutableSlot != nil {
		slot, found, err := o.leiosImmutableSlot(context.Background())
		if err == nil && found {
			bound, known = slot, true
		}
	}
	if store, err := db.LeiosDB(); err == nil {
		if wm, err := store.Watermarks(); err == nil && wm.GCBefore > 0 {
			bound, known = max(bound, wm.GCBefore), true
		}
	}
	return bound, known
}

func (o *Ouroboros) leiosfetchServerBlockRequest(
	ctx oleiosfetch.CallbackContext,
	point ocommon.Point,
) (protocol.Message, error) {
	notFound := func() error {
		return fmt.Errorf(
			"leios endorser block not found: %d.%x: %w",
			point.Slot,
			point.Hash,
			oleiosfetch.ErrBlockNotFound,
		)
	}
	if o.leiosServingRefused(point) {
		return nil, notFound()
	}
	data, ok := o.lookupLeiosFetchServingData(ctx, point, nil)
	if !ok {
		return nil, notFound()
	}
	return oleiosfetch.NewMsgBlock(cbor.RawMessage(data.blockRaw)), nil
}

func (o *Ouroboros) leiosfetchServerBlockTxsRequest(
	ctx oleiosfetch.CallbackContext,
	point ocommon.Point,
	txBitmap map[uint16]uint64,
) (protocol.Message, error) {
	notAvailable := func() error {
		return fmt.Errorf(
			"leios endorser block not available: %d.%x: %w",
			point.Slot,
			point.Hash,
			oleiosfetch.ErrBlockTxsNotFound,
		)
	}
	if o.leiosServingRefused(point) {
		return nil, notAvailable()
	}
	data, ok := o.lookupLeiosFetchServingData(ctx, point, func(data *leiosEndorserBlockData) bool {
		return leiosRequestedTxsAvailable(data, txBitmap)
	})
	if !ok {
		return nil, notAvailable()
	}
	if err := validateLeiosTxBitmap(data.txCount, txBitmap); err != nil {
		return nil, err
	}
	var txs []cbor.RawMessage
	if data.completeTxCache() {
		txs = leiosTxsFromBitmap(data.txsRaw, txBitmap)
	} else {
		available := make([]cbor.RawMessage, data.txCount)
		data.seedLeiosPartialTxsLocked(available, nil)
		_, txs = leiosAvailableTxsFromBitmap(available, txBitmap)
	}
	// MsgBlockTxs carries no bitmap, so a reply must hold exactly the
	// requested transactions; a partial reply would be misread positionally.
	if want := len(leiosBitmapTxIndices(txBitmap)); len(txs) != want {
		return nil, fmt.Errorf(
			"leios endorser block transactions not available: %d.%x: "+
				"%d of %d requested: %w",
			point.Slot,
			point.Hash,
			len(txs),
			want,
			oleiosfetch.ErrBlockTxsNotFound,
		)
	}
	return oleiosfetch.NewMsgBlockTxs(txs), nil
}

func (o *Ouroboros) leiosfetchServerVotesRequest(
	ctx oleiosfetch.CallbackContext,
	voteIds []oleiosfetch.MsgVotesRequestVoteId,
) (protocol.Message, error) {
	if o.leiosVotes == nil {
		// Unknown vote IDs are already omitted from this response. Treat an
		// unavailable vote manager as an empty result instead of failing the
		// shared bearer.
		return oleiosfetch.NewMsgVotes(make([]cbor.RawMessage, 0)), nil
	}
	if len(voteIds) > maxLeiosFetchVoteIDs {
		return nil, fmt.Errorf(
			"leios-fetch vote ID request exceeds limit: %d > %d",
			len(voteIds),
			maxLeiosFetchVoteIDs,
		)
	}
	// MsgVotesRequestVoteId aliases lcommon.LeiosVoteId; unknown ids
	// are omitted from the response.
	return oleiosfetch.NewMsgVotes(o.leiosVotes.VotesByIds(voteIds)), nil
}

// leiosfetchServerBlockRangeRequest declines a leios-fetch block-range
// request, which dingo does not serve.
//
// NOTE: this MUST NOT return nil. gouroboros reads a nil return from
// BlockRangeRequestFunc as "the callback started an async process that will
// send NextBlockAndTxsInRange / LastBlockAndTxsInRange", so returning nil
// without sending anything leaves this server holding leios-fetch agency in
// StateBlockRange forever. The requesting peer's client can then never issue
// another leios-fetch request on that connection -- its protocol send loop
// waits for agency that only the missing response returns -- and it has no way
// to detect the condition. That is the same permanent desynchronisation that
// stalled dingo's own endorser-block backfill with dingo on
// the serving side of it.
//
// There is no absence reply for a range request (LastBlockAndTxsInRange
// carries a mandatory block), so declining necessarily fails the leios-fetch
// connection. An observable connection error is recoverable by the peer's
// governance; a silent hang is not.
func (o *Ouroboros) leiosfetchServerBlockRangeRequest(
	ctx oleiosfetch.CallbackContext,
	start ocommon.Point,
	end ocommon.Point,
) error {
	return fmt.Errorf(
		"leios-fetch block range requests are not served: %d.%x-%d.%x",
		start.Slot,
		start.Hash,
		end.Slot,
		end.Hash,
	)
}
