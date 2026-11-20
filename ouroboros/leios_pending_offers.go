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
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
)

// leiosPendingOfferKey identifies one connection's offer of one part of an
// endorser block; the manifest and the transactions are offered separately.
type leiosPendingOfferKey struct {
	connId ouroboros.ConnectionId
	txs    bool
}

// leiosPendingOffer is an offer that arrived before the election it concerns
// was observed. It is only a record of which peer can serve the point, and is
// replayed once the election is observed.
type leiosPendingOffer struct {
	ctx        oleiosnotify.CallbackContext
	msg        protocol.Message
	recordedAt time.Time
	seq        uint64
}

// rememberLeiosOffer keeps an offer that cannot start a fetch yet. The table
// shares the backfill-source bounds: a per-point peer cap, a per-connection
// point cap that only displaces that connection's own oldest offers, the
// endorser-block cache's point cap, and its TTL.
func (o *Ouroboros) rememberLeiosOffer(
	ctx oleiosnotify.CallbackContext,
	msg protocol.Message,
	point ocommon.Point,
	txs bool,
) {
	o.storeLeiosPendingOffer(ctx, msg, point, txs)
	// The election can be observed between the caller's check and the store
	// above, after its replay found nothing to release. Re-check once the offer
	// is visible to that replay, so one of the two releases it.
	if o.leiosElectionObserved(point) {
		o.replayLeiosPendingOffers(point)
	}
}

func (o *Ouroboros) storeLeiosPendingOffer(
	ctx oleiosnotify.CallbackContext,
	msg protocol.Message,
	point ocommon.Point,
	txs bool,
) {
	now := time.Now()
	key := leiosBlockKey(point.Slot, point.Hash)
	offerKey := leiosPendingOfferKey{connId: ctx.ConnectionId, txs: txs}
	o.leiosPendingOffersMu.Lock()
	defer o.leiosPendingOffersMu.Unlock()
	o.pruneLeiosPendingOffersLocked(now)
	if _, held := o.leiosPendingOffers[key][offerKey]; !held {
		o.evictLeiosPendingOfferConnOverflowLocked(ctx.ConnectionId)
	}
	offers := o.leiosPendingOffers[key]
	if offers == nil {
		offers = make(map[leiosPendingOfferKey]leiosPendingOffer)
		o.leiosPendingOffers[key] = offers
	}
	o.leiosPendingOfferSeq++
	offers[offerKey] = leiosPendingOffer{
		ctx:        ctx,
		msg:        msg,
		recordedAt: now,
		seq:        o.leiosPendingOfferSeq,
	}
	o.pruneLeiosPendingOffersLocked(now)
}

// replayLeiosPendingOffers re-submits the offers remembered for point. It runs
// after the election for point is observed, so each replayed offer now passes
// the election check and fetches from the peer that offered it.
func (o *Ouroboros) replayLeiosPendingOffers(point ocommon.Point) {
	now := time.Now()
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosPendingOffersMu.Lock()
	o.pruneLeiosPendingOffersLocked(now)
	offers := o.leiosPendingOffers[key]
	delete(o.leiosPendingOffers, key)
	o.leiosPendingOffersMu.Unlock()
	for _, offer := range offers {
		if leiosAnnouncementSourceClosed(offer.ctx.ConnectionDoneChan) {
			continue
		}
		if err := o.handleLeiosNotifyMessage(
			offer.ctx, offer.msg, true,
		); err != nil {
			o.config.Logger.Debug(
				"replaying leios offer failed",
				"component", "network",
				"protocol", "leios-notify",
				"connection_id", leiosConnectionIdString(offer.ctx.ConnectionId),
				"slot", point.Slot,
				"error", err,
			)
		}
	}
}

// evictLeiosPendingOfferConnOverflowLocked drops connId's own oldest offers
// until it holds fewer than leiosBackfillMaxPointsPerConn. Callers hold
// leiosPendingOffersMu.
func (o *Ouroboros) evictLeiosPendingOfferConnOverflowLocked(
	connId ouroboros.ConnectionId,
) {
	for {
		count := 0
		var oldestKey string
		var oldestOffer leiosPendingOfferKey
		var oldestSeq uint64
		for key, offers := range o.leiosPendingOffers {
			for offerKey, offer := range offers {
				if offerKey.connId != connId {
					continue
				}
				if count == 0 || offer.seq < oldestSeq {
					oldestKey, oldestOffer, oldestSeq = key, offerKey, offer.seq
				}
				count++
			}
		}
		if count < leiosBackfillMaxPointsPerConn {
			return
		}
		o.deleteLeiosPendingOfferLocked(oldestKey, oldestOffer)
	}
}

func (o *Ouroboros) deleteLeiosPendingOfferLocked(
	key string,
	offerKey leiosPendingOfferKey,
) {
	offers := o.leiosPendingOffers[key]
	delete(offers, offerKey)
	if len(offers) == 0 {
		delete(o.leiosPendingOffers, key)
	}
}

// pruneLeiosPendingOffersLocked applies the TTL and the per-point and global
// bounds. Callers hold leiosPendingOffersMu.
func (o *Ouroboros) pruneLeiosPendingOffersLocked(now time.Time) {
	cutoff := now.Add(-leiosEndorserBlockCacheTTL)
	for key, offers := range o.leiosPendingOffers {
		for offerKey, offer := range offers {
			if offer.recordedAt.Before(cutoff) {
				delete(offers, offerKey)
			}
		}
		for len(offers) > leiosBackfillMaxSourcesPerPoint {
			var oldest leiosPendingOfferKey
			var oldestSeq uint64
			first := true
			for offerKey, offer := range offers {
				if first || offer.seq < oldestSeq {
					oldest, oldestSeq, first = offerKey, offer.seq, false
				}
			}
			delete(offers, oldest)
		}
		if len(offers) == 0 {
			delete(o.leiosPendingOffers, key)
		}
	}
	for len(o.leiosPendingOffers) > leiosEndorserBlockCacheMaxEntries {
		var oldestKey string
		var oldestSeq uint64
		for key, offers := range o.leiosPendingOffers {
			for _, offer := range offers {
				if oldestKey == "" || offer.seq < oldestSeq {
					oldestKey, oldestSeq = key, offer.seq
				}
			}
		}
		delete(o.leiosPendingOffers, oldestKey)
	}
}
