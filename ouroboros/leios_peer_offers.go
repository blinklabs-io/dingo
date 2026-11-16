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
	"errors"
	"fmt"
	"slices"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// leiosMaxEndorserBlockBytes is the largest size a body offer may claim: the
// ceiling on an endorser block's reference list, maxLeiosEbBytesSize in the
// reference LeiosFetch module. The ledger parameter in force can only be
// smaller, and the offered size is what a fetch goes on to request, so this
// is all that bounds it.
const leiosMaxEndorserBlockBytes = 512 * 1024

// leiosPeerMaxAnnouncements bounds the announcements remembered for one
// connection, and with them the offers: an offer is only accepted for an
// announced point, so dropping an announcement drops its offer record too. An
// honest peer announces at most two per election, so reaching it needs far
// more than the announcement window holds; the oldest slots are dropped
// first.
const leiosPeerMaxAnnouncements = 4096

// leiosLocalAnnouncementSource labels announcements this node forged itself,
// which no peer made and so are not remembered per connection.
const leiosLocalAnnouncementSource = "local"

// errLeiosInvalidOffer wraps every condition under which a received offer ends
// the connection that sent it.
var errLeiosInvalidOffer = errors.New("invalid leios offer")

var (
	errLeiosOfferTooOld = fmt.Errorf(
		"%w: below the pruned immutable slot", errLeiosInvalidOffer,
	)
	errLeiosOfferRepeated = fmt.Errorf(
		"%w: repeated", errLeiosInvalidOffer,
	)
	errLeiosBlockOfferSize = fmt.Errorf(
		"%w: body size is zero or above the endorser block maximum",
		errLeiosInvalidOffer,
	)
	errLeiosOfferUnannounced = fmt.Errorf(
		"%w: endorser block not announced by this peer", errLeiosInvalidOffer,
	)
)

type leiosPeerPointKey struct {
	slot uint64
	hash string
}

type leiosPeerOffered struct {
	body    bool
	closure bool
}

// leiosPeerOfferState is what one connection's LeiosNotify peer has announced
// and offered. A connection that reconnects starts empty, as does a peer's
// state once its connection has closed.
type leiosPeerOfferState struct {
	owner     <-chan any
	pruneSlot uint64
	announced map[leiosPeerPointKey]struct{}
	offered   map[leiosPeerPointKey]leiosPeerOffered
}

// leiosPeerOfferStateLocked returns the state for connKey, replacing one that
// belongs to an earlier connection with the same identity and discarding the
// states of connections that have closed. Callers hold leiosPeerOffersMu.
func (o *Ouroboros) leiosPeerOfferStateLocked(
	connKey string,
	owner <-chan any,
) *leiosPeerOfferState {
	if o.leiosPeerOffers == nil {
		o.leiosPeerOffers = make(map[string]*leiosPeerOfferState)
	}
	for key, state := range o.leiosPeerOffers {
		if key != connKey && leiosAnnouncementSourceClosed(state.owner) {
			delete(o.leiosPeerOffers, key)
		}
	}
	state := o.leiosPeerOffers[connKey]
	if state == nil || state.owner != owner {
		state = &leiosPeerOfferState{
			owner:     owner,
			announced: make(map[leiosPeerPointKey]struct{}),
			offered:   make(map[leiosPeerPointKey]leiosPeerOffered),
		}
		o.leiosPeerOffers[connKey] = state
	}
	return state
}

// forgetLeiosPeerOfferStates releases the state of every closed connection.
func (o *Ouroboros) forgetLeiosPeerOfferStates() {
	o.leiosPeerOffersMu.Lock()
	defer o.leiosPeerOffersMu.Unlock()
	for key, state := range o.leiosPeerOffers {
		if leiosAnnouncementSourceClosed(state.owner) {
			delete(o.leiosPeerOffers, key)
		}
	}
}

// leiosPeerPruneSlot returns the immutable slot offers are measured against,
// and false while it is unknown. An unresolvable slot prunes nothing.
func (o *Ouroboros) leiosPeerPruneSlot() (uint64, bool) {
	if o.leiosImmutableSlot == nil {
		return 0, false
	}
	slot, found, err := o.leiosImmutableSlot(context.Background())
	if err != nil || !found {
		return 0, false
	}
	return slot, true
}

// prune advances the state to the immutable slot and drops what lies below
// it. Offers below it are rejected as too old, so dropping their records and
// the announcements they would rely on loses nothing.
func (s *leiosPeerOfferState) prune(immutableSlot uint64) {
	if immutableSlot <= s.pruneSlot {
		return
	}
	s.pruneSlot = immutableSlot
	for key := range s.announced {
		if key.slot < immutableSlot {
			delete(s.announced, key)
		}
	}
	for key := range s.offered {
		if key.slot < immutableSlot {
			delete(s.offered, key)
		}
	}
}

// recordLeiosPeerAnnouncement remembers that the peer on connection source
// announced the endorser block at (slot, hash). owner is the connection's done
// channel. An announcement that no peer sent is not remembered.
func (o *Ouroboros) recordLeiosPeerAnnouncement(
	source string,
	owner <-chan any,
	slot uint64,
	hash []byte,
) {
	if source == leiosLocalAnnouncementSource ||
		leiosAnnouncementSourceClosed(owner) {
		return
	}
	immutableSlot, known := o.leiosPeerPruneSlot()
	o.leiosPeerOffersMu.Lock()
	defer o.leiosPeerOffersMu.Unlock()
	state := o.leiosPeerOfferStateLocked(source, owner)
	if known {
		state.prune(immutableSlot)
	}
	if slot < state.pruneSlot {
		return
	}
	state.announced[leiosPeerPointKey{slot: slot, hash: string(hash)}] = struct{}{}
	if len(state.announced) <= leiosPeerMaxAnnouncements {
		return
	}
	keys := make([]leiosPeerPointKey, 0, len(state.announced))
	for key := range state.announced {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b leiosPeerPointKey) int {
		switch {
		case a.slot < b.slot:
			return -1
		case a.slot > b.slot:
			return 1
		}
		return 0
	})
	for _, key := range keys[:len(keys)-leiosPeerMaxAnnouncements] {
		delete(state.announced, key)
		delete(state.offered, key)
	}
}

// checkLeiosPeerOffer accepts a body offer (closure false) or a closure
// offer (closure true) from the peer on connKey, or returns the error that
// must end its connection. The checks and their order are those of the
// reference's checkLeiosBlockOffer and checkLeiosClosureOffer; an accepted
// offer is recorded so that repeating it is refused, whatever the caller
// then does with it.
func (o *Ouroboros) checkLeiosPeerOffer(
	connKey string,
	owner <-chan any,
	point ocommon.Point,
	size uint64,
	closure bool,
) error {
	immutableSlot, known := o.leiosPeerPruneSlot()
	key := leiosPeerPointKey{slot: point.Slot, hash: string(point.Hash)}
	o.leiosPeerOffersMu.Lock()
	defer o.leiosPeerOffersMu.Unlock()
	state := o.leiosPeerOfferStateLocked(connKey, owner)
	if known {
		state.prune(immutableSlot)
	}
	seen := state.offered[key]
	switch {
	case point.Slot < state.pruneSlot:
		return fmt.Errorf(
			"%w: slot %d, pruned to %d",
			errLeiosOfferTooOld, point.Slot, state.pruneSlot,
		)
	case closure && seen.closure, !closure && seen.body:
		return fmt.Errorf("%w: slot %d", errLeiosOfferRepeated, point.Slot)
	case !closure && (size == 0 || size > leiosMaxEndorserBlockBytes):
		return fmt.Errorf(
			"%w: claimed %d, maximum %d",
			errLeiosBlockOfferSize, size, leiosMaxEndorserBlockBytes,
		)
	}
	if _, announced := state.announced[key]; !announced {
		return fmt.Errorf(
			"%w: slot %d", errLeiosOfferUnannounced, point.Slot,
		)
	}
	if closure {
		seen.closure = true
	} else {
		seen.body = true
	}
	state.offered[key] = seen
	return nil
}
