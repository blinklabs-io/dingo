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
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/protocol"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	leiosEndorserBlockCacheMaxEntries = 1024
	leiosEndorserBlockCacheTTL        = 10 * time.Minute
	// defaultLeiosClosureWaitTimeout is the fallback window the NtC chainsync
	// server waits for a certifying ranking block's endorser block
	// transaction closure to become available. It is used only when the
	// ledger's Leios pipeline timing is unavailable (e.g. no ledger state or
	// unknown slot length); production derives the window from that timing
	// via leiosClosureWaitTimeout. The value mirrors the pipeline's default
	// 20-slot certify-by deadline at the 1s Musashi slot length, so it is not
	// shorter than the documented healthy closure-delivery path.
	defaultLeiosClosureWaitTimeout = 20 * time.Second
)

// leiosEndorserBlockCacheMaxEntryBytes and leiosEndorserBlockCacheMaxBytes are
// declared as vars, not consts, solely so tests can lower them temporarily
// (save, override, t.Cleanup restore) to exercise byte-budget rejection and
// eviction without allocating hundreds of megabytes of test data. Production
// code only reads them.
var (
	// leiosEndorserBlockCacheMaxEntryBytes bounds the retained size (manifest
	// plus every transaction body held, complete or partial) of a single
	// cached endorser block. It bounds one oversized or malicious entry
	// independently of leiosEndorserBlockCacheMaxEntries, which only bounds
	// entry count. An offer whose declared size already exceeds this, or
	// whose fetched body would put the entry over it, is rejected rather
	// than cached; see storeLeiosEndorserBlock and
	// fetchAndValidateLeiosEbManifest.
	leiosEndorserBlockCacheMaxEntryBytes = 16 << 20 // 16 MiB
	// leiosEndorserBlockCacheMaxBytes bounds the aggregate retained size of
	// the endorser-block cache. leiosEndorserBlockCacheMaxEntries alone would
	// let leiosEndorserBlockCacheMaxEntries maximally-sized entries retain
	// several GiB; this bounds actual memory directly. Eviction at either
	// budget follows the same oldest-inserted-first policy; see
	// pruneLeiosEndorserBlockCacheLocked.
	leiosEndorserBlockCacheMaxBytes = 256 << 20 // 256 MiB
)

// errLeiosClosureUnresolved is returned by the NtC serving path when a
// certifying ranking block's endorser closure does not arrive within the wait
// window. The caller closes the connection rather than serving an incomplete
// (empty-transaction) CertRB, so the client retries from its last point.
var errLeiosClosureUnresolved = errors.New(
	"leios endorser closure unresolved before timeout",
)

// leiosConnDoneContext adapts a serving connection's release channel to
// context.Context so it can be combined with context.WithTimeout via
// context.WithTimeout(leiosConnDoneContext{done: connDone}, timeout). The
// resulting context is done when either the timeout elapses or connDone
// closes, whichever comes first, so a closure wait started for one NtC
// request cannot outlive the connection it serves. A nil channel behaves like
// context.Background(): it never fires on its own, leaving the timeout as the
// only bound.
//
// The channel must come from registerLeiosServeWaiter, not from
// Protocol.DoneChan(). The chainsync server callback runs inside gouroboros's
// recvLoop, and recvLoop closes recvDoneChan only after the callback returns;
// doneChan in turn closes only after recvDoneChan does. DoneChan() therefore
// cannot close while this wait is in progress, and using it here would leave
// the wait bounded by the timeout alone.
type leiosConnDoneContext struct {
	done <-chan struct{}
}

func (c leiosConnDoneContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c leiosConnDoneContext) Done() <-chan struct{} {
	return c.done
}

func (c leiosConnDoneContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func (c leiosConnDoneContext) Value(any) any {
	return nil
}

// registerLeiosServeWaiter returns a channel closed when connId's connection
// goes away, so an NtC serving wait on that connection is released as soon as
// the client disconnects. The returned cancel function deregisters the waiter
// and must always be called.
//
// The liveness re-check after registration closes the race with a connection
// that is already going away: connmanager removes the connection from its map
// before invoking ConnClosedFunc, so a nil lookup here means either the
// release has already run (and would have closed a registered channel) or it
// is about to. Either way the caller must not wait, so the channel is closed
// before it is returned.
func (o *Ouroboros) registerLeiosServeWaiter(
	connId ouroboros.ConnectionId,
) (done <-chan struct{}, cancel func()) {
	ch := make(chan struct{})
	o.leiosServeWaitersMu.Lock()
	if o.leiosServeWaiters == nil {
		o.leiosServeWaiters = make(
			map[ouroboros.ConnectionId][]chan struct{},
		)
	}
	o.leiosServeWaiters[connId] = append(o.leiosServeWaiters[connId], ch)
	o.leiosServeWaitersMu.Unlock()

	cancel = func() {
		o.leiosServeWaitersMu.Lock()
		defer o.leiosServeWaitersMu.Unlock()
		waiters := o.leiosServeWaiters[connId]
		for i, w := range waiters {
			if w == ch {
				o.leiosServeWaiters[connId] = slices.Delete(waiters, i, i+1)
				break
			}
		}
		if len(o.leiosServeWaiters[connId]) == 0 {
			delete(o.leiosServeWaiters, connId)
		}
	}

	// The connection manager is absent in unit tests that exercise the
	// serving decision directly; there is no liveness to check, so the
	// timeout remains the only bound.
	if o.connManager != nil &&
		o.connManager.GetConnectionById(connId) == nil {
		o.releaseLeiosServeWaiter(connId, ch)
	}
	return ch, cancel
}

// releaseLeiosServeWaiter deregisters one waiter channel and closes it, both
// under the lock so it cannot double-close against a concurrent
// ReleaseLeiosServeWaiters: whichever runs first removes the channel, and the
// other no longer finds it.
func (o *Ouroboros) releaseLeiosServeWaiter(
	connId ouroboros.ConnectionId,
	ch chan struct{},
) {
	o.leiosServeWaitersMu.Lock()
	defer o.leiosServeWaitersMu.Unlock()
	waiters := o.leiosServeWaiters[connId]
	for i, w := range waiters {
		if w == ch {
			o.leiosServeWaiters[connId] = slices.Delete(waiters, i, i+1)
			if len(o.leiosServeWaiters[connId]) == 0 {
				delete(o.leiosServeWaiters, connId)
			}
			close(ch)
			return
		}
	}
}

// ReleaseLeiosServeWaiters wakes every NtC serving wait pending on connId and
// clears them. It is called from the node's connection-closed callback, which
// connmanager drives from a per-connection goroutine blocked on the
// connection's ErrorChan. That goroutine is independent of the chainsync
// server callback, so it still runs while the callback is parked waiting for
// an endorser closure -- which is precisely why the release cannot come from
// the protocol's own done channel.
func (o *Ouroboros) ReleaseLeiosServeWaiters(
	connId ouroboros.ConnectionId,
) {
	o.leiosServeWaitersMu.Lock()
	waiters := o.leiosServeWaiters[connId]
	delete(o.leiosServeWaiters, connId)
	o.leiosServeWaitersMu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

// RegisterLeiosServeWaiterForTesting exposes registerLeiosServeWaiter so the
// root package can prove its connection-closed callback actually releases a
// parked NtC serving wait. The release wiring spans two packages, so the
// assertion cannot be made from inside either one alone.
func (o *Ouroboros) RegisterLeiosServeWaiterForTesting(
	connId ouroboros.ConnectionId,
) (done <-chan struct{}, cancel func()) {
	return o.registerLeiosServeWaiter(connId)
}

// leiosClosureWaitTimeout returns how long the NtC serving path waits for a
// certifying ranking block's endorser closure. An explicit config override
// wins (tests/tuning); otherwise it uses the same pipeline timing
// (EndorserBlockWaitSlots × slot length) the ledger uses to gate applying a
// referenced endorser block, so NtC serving and ledger application wait for the
// same healthy window. It falls back to a conservative default only when that
// timing is unavailable.
func (o *Ouroboros) leiosClosureWaitTimeout() time.Duration {
	if o.config.LeiosClosureWaitTimeout > 0 {
		return o.config.LeiosClosureWaitTimeout
	}
	if o.ledgerState != nil {
		if d := o.ledgerState.EndorserBlockWaitDuration(); d > 0 {
			return d
		}
	}
	return defaultLeiosClosureWaitTimeout
}

type leiosEndorserBlockData struct {
	point    ocommon.Point
	blockRaw []byte
	txsRaw   []cbor.RawMessage
	// partialTxs retains an incomplete fetch: a txCount-long slice whose nil
	// entries are the transactions still missing (the same representation
	// leiosNeededBitmap turns into a request bitmap). The relay diffuses an
	// endorser block's transactions over several seconds, so a fetch near the
	// live tip routinely runs out of served transactions before the block is
	// whole. Keeping what it did gather here lets the next offer of the same
	// block fetch only the missing tail instead of starting over, without
	// holding a per-connection fetch slot open across the gap (issue #2629).
	// It is
	// cleared once txsRaw is complete, and it is bounded by the same cache
	// TTL and entry cap as any other cached endorser block.
	partialTxs []cbor.RawMessage
	txCount    int
	cacheKeys  []string
	insertedAt time.Time
	// seq is a monotonic insertion sequence assigned while leiosMu is held
	// (see Ouroboros.leiosEndorserBlockSeq), used to order eviction instead of
	// insertedAt. insertedAt is a wall-clock timestamp captured before the
	// lock is acquired, so a delayed goroutine can carry an earlier timestamp
	// into the map after a goroutine that actually inserted first; sorting
	// eviction by seq instead avoids evicting the truly-newer entry first.
	seq uint64
	// slotVerified reports whether point.Slot has been corroborated by a
	// source dingo trusts, rather than being the offering connection's
	// unchecked claim. Everything keyed on the slot -- vote emission,
	// pipeline observation, blob persistence, and the slot handed to the
	// ledger by EndorserBlockTxsByHash -- is withheld until it is true, so a
	// peer cannot bind an authentic manifest to a fabricated slot by offering
	// it before its announcement arrives (issue #3513). This field is never
	// mutated on a cache entry that is already reachable by another reader:
	// the cache stores *leiosEndorserBlockData, and lookupLeiosEndorserBlock
	// hands that pointer to callers outside leiosMu, so promoting an entry
	// (bindLeiosEndorserBlockSlot) replaces it with a new, distinct value
	// under the lock rather than flipping this field on the shared one.
	slotVerified bool
}

// leiosStoreOrigin records whether a store's point is already authoritative.
type leiosStoreOrigin uint8

const (
	// leiosStorePeerOffered is a store driven by a peer's leios-notify offer.
	// The point is that connection's claim, so it must be corroborated by a
	// validated announcement or the ledger's chain-derived reference before
	// anything keyed on its slot is published.
	leiosStorePeerOffered leiosStoreOrigin = iota
	// leiosStoreAuthoritative is a store whose slot dingo established itself:
	// a locally forged endorser block, or a by-point backfill whose point came
	// from the ranking block the ledger is applying.
	leiosStoreAuthoritative
)

func leiosBlockKey(hash []byte) string {
	return string(hash)
}

func cloneRawMessages(in []cbor.RawMessage) []cbor.RawMessage {
	if len(in) == 0 {
		return nil
	}
	out := make([]cbor.RawMessage, len(in))
	for i := range in {
		out[i] = slices.Clone(in[i])
	}
	return out
}

// validateLeiosEndorserBlockTxs binds fetched transaction wire values to the
// manifest before the complete set is cached or applied. Leios transaction
// references use the Cardano transaction ID (the hash of the transaction body)
// and the complete transaction's encoded size, while leios-fetch carries each
// transaction either directly or in a CBOR byte-string wrapper.
func validateLeiosEndorserBlockTxs(
	manifestRaw []byte,
	txsRaw []cbor.RawMessage,
) error {
	block, err := lcommon.NewLeiosEndorserBlockFromCbor(manifestRaw)
	if err != nil {
		return fmt.Errorf("decode leios endorser block: %w", err)
	}
	if err := block.Validate(); err != nil {
		return fmt.Errorf("validate leios endorser block references: %w", err)
	}
	if len(txsRaw) != len(block.TransactionReferences) {
		return fmt.Errorf(
			"leios endorser block transaction count mismatch: got %d, want %d",
			len(txsRaw),
			len(block.TransactionReferences),
		)
	}
	for i, raw := range txsRaw {
		if err := validateLeiosEndorserBlockTx(
			i,
			block.TransactionReferences[i],
			raw,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateLeiosEndorserBlockTx(
	index int,
	ref lcommon.LeiosTransactionReference,
	raw cbor.RawMessage,
) error {
	txCbor := []byte(raw)
	if len(txCbor) > 0 && txCbor[0]>>5 == 2 {
		var inner []byte
		bytesRead, err := cbor.Decode(txCbor, &inner)
		if err != nil {
			return fmt.Errorf("unwrap endorser tx %d: %w", index, err)
		}
		if bytesRead != len(txCbor) {
			return fmt.Errorf("endorser tx %d has trailing wrapper bytes", index)
		}
		txCbor = inner
	}
	var txElems []cbor.RawMessage
	bytesRead, err := cbor.Decode(txCbor, &txElems)
	if err != nil {
		return fmt.Errorf("decode endorser tx %d envelope: %w", index, err)
	}
	if bytesRead != len(txCbor) {
		return fmt.Errorf("endorser tx %d has trailing envelope bytes", index)
	}
	if len(txElems) == 0 {
		return fmt.Errorf("endorser tx %d has no body", index)
	}
	if len(txCbor) != int(ref.TransactionSize) {
		return fmt.Errorf(
			"endorser tx %d size mismatch: got %d, want %d",
			index,
			len(txCbor),
			ref.TransactionSize,
		)
	}
	if bodyHash := lcommon.Blake2b256Hash(txElems[0]); bodyHash != ref.TransactionHash {
		return fmt.Errorf("endorser tx %d body hash mismatch", index)
	}
	return nil
}

func leiosEndorserBlockTxValidator(
	manifestRaw []byte,
	txCount int,
) (func(int, cbor.RawMessage) error, error) {
	block, err := lcommon.NewLeiosEndorserBlockFromCbor(manifestRaw)
	if err != nil {
		return nil, fmt.Errorf("decode leios endorser block: %w", err)
	}
	if err := block.Validate(); err != nil {
		return nil, fmt.Errorf("validate leios endorser block references: %w", err)
	}
	if len(block.TransactionReferences) != txCount {
		return nil, fmt.Errorf(
			"leios endorser block transaction count mismatch: got %d, want %d",
			txCount,
			len(block.TransactionReferences),
		)
	}
	return func(index int, raw cbor.RawMessage) error {
		if index < 0 || index >= len(block.TransactionReferences) {
			return fmt.Errorf("endorser tx index %d out of range", index)
		}
		return validateLeiosEndorserBlockTx(
			index,
			block.TransactionReferences[index],
			raw,
		)
	}, nil
}

func (o *Ouroboros) storeLeiosEndorserBlock(
	point ocommon.Point,
	blockRaw []byte,
	txsRaw []cbor.RawMessage,
	origin leiosStoreOrigin,
) error {
	if len(blockRaw) == 0 {
		return errors.New("leios endorser block cache: empty block")
	}
	if len(point.Hash) == 0 {
		return errors.New("leios endorser block cache: empty point hash")
	}
	// Verify the served bytes hash to the requested point BEFORE decoding.
	// A peer that returns an empty, truncated, or otherwise wrong manifest
	// (the prototype relay returns empty manifests for large endorser blocks
	// when hammered; see leiosBackfiller) must be diagnosed as a fetch/serving
	// problem ("point hash mismatch") rather than misreported as a decode
	// invariant violation ("must contain at least one transaction reference").
	// The hash covers the full manifest and does not require decoding, so
	// checking it first is strictly safe and turns a wrong response into a
	// retryable fetch error instead of a terminal-looking decode failure.
	blockHash := lcommon.Blake2b256Hash(blockRaw)
	if !slices.Equal(blockHash.Bytes(), point.Hash) {
		return errors.New("leios endorser block cache: point hash mismatch")
	}
	// Bind the entry to the slot its announcement actually vouched for. A
	// peer-offered point is only that connection's claim: the manifest is
	// content-addressed, so an authentic endorser block can be replayed under
	// any slot. Reject a store that contradicts a recorded announcement, and
	// mark the entry verified only when a trusted source establishes the slot
	// (issue #3513).
	//
	// The announcement need not have arrived yet -- the relay, and dingo's own
	// forge path, queue the block offer before the ranking-block announcement
	// -- so an unannounced peer-offered store is cached but left unverified
	// rather than rejected, which would drop endorser blocks on the normal
	// ordering. slotVerified gates everything keyed on the slot; the binding is
	// reconciled later by bindLeiosEndorserBlockSlot when the announcement (or
	// the ledger's chain-derived reference) arrives.
	//
	// leiosAnnouncementsMu is held from this check through the cache
	// insertion below, not just for the read: recordLeiosAnnouncement holds
	// the same lock across recording an announcement and reconciling any
	// already-cached entry for it (bindLeiosEndorserBlockSlot), and that
	// reconciliation is one-shot -- it only runs once, when the announcement
	// is recorded. Without a shared lock, a concurrent announcement could be
	// recorded and reconciled against a cache that does not contain this
	// entry yet (it is inserted below), see this check find nothing, and then
	// insert an entry nothing will ever come back to verify. Lock order is
	// always leiosAnnouncementsMu before leiosMu.
	o.leiosAnnouncementsMu.Lock()
	verified := origin == leiosStoreAuthoritative
	if announcedSlot, ok := o.leiosAnnouncedSlotLocked(point.Hash); ok {
		if announcedSlot != point.Slot {
			o.leiosAnnouncementsMu.Unlock()
			return fmt.Errorf(
				"leios endorser block cache: point slot does not match announced point: announced %d, got %d",
				announcedSlot,
				point.Slot,
			)
		}
		verified = true
	}
	block, err := lcommon.NewLeiosEndorserBlockFromCbor(blockRaw)
	if err != nil {
		o.leiosAnnouncementsMu.Unlock()
		return fmt.Errorf("decode leios endorser block: %w", err)
	}
	cacheKeys := []string{leiosBlockKey(point.Hash)}
	data := &leiosEndorserBlockData{
		point:        point,
		blockRaw:     slices.Clone(blockRaw),
		txsRaw:       cloneRawMessages(txsRaw),
		txCount:      len(block.TransactionReferences),
		cacheKeys:    cacheKeys,
		insertedAt:   time.Now(),
		slotVerified: verified,
	}
	o.leiosMu.Lock()
	if o.leiosEndorserBlocks == nil {
		o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	}
	o.pruneLeiosEndorserBlockCacheLocked(time.Now())
	o.leiosEndorserBlockSeq++
	data.seq = o.leiosEndorserBlockSeq
	if existing := o.leiosEndorserBlocks[cacheKeys[0]]; existing != nil &&
		existing.point.Slot != point.Slot && origin != leiosStoreAuthoritative {
		// A peer-offered store contradicting whatever is currently cached is
		// rejected outright; a peer's claim alone must not override a
		// resident entry, whether that entry is itself verified, unverified,
		// or a reload from the blob store.
		o.leiosMu.Unlock()
		o.leiosAnnouncementsMu.Unlock()
		return fmt.Errorf(
			"leios endorser block cache: point slot mismatch for hash: cached %d, got %d",
			existing.point.Slot,
			point.Slot,
		)
	} else if existing != nil && existing.point.Slot != point.Slot {
		// origin == leiosStoreAuthoritative here: an authoritative source (a
		// locally forged block, or a ledger-driven by-point backfill)
		// supersedes whatever is currently cached for this hash, including a
		// stale entry reloaded from the blob store. The manifest is
		// content-addressed, so the same hash can legitimately recur at a
		// different slot, and the cached occurrence's slot binding does not
		// apply to this one (issue #3513 review). Nothing is carried
		// forward from it below -- the old occurrence's transaction set and
		// partial-fetch state belong to a different slot -- data (already
		// built fresh above) fully replaces it when inserted.
	} else if existing != nil {
		// Never regress a cached transaction set. The relay offers each
		// endorser block on every connection, so a manifest-only store
		// (txsRaw nil) routinely arrives after another connection has already
		// fetched the transactions. Overwriting made a complete endorser block
		// report itself unavailable again, which stalled the ledger's
		// certified closure ("certified Leios endorser block unavailable")
		// and made leios-fetch serving fail for downstream peers until some
		// peer happened to redeliver the transactions. The manifest is
		// content-addressed by point.Hash and verified above, so blockRaw and
		// txCount are identical across stores for the same hash and only the
		// transaction set can differ. The retained slice is never mutated
		// after being stored, so it can be aliased rather than re-cloned.
		if len(existing.txsRaw) > len(data.txsRaw) {
			data.txsRaw = existing.txsRaw
		}
		// Carry retained partial-fetch state forward for the same reason: a
		// manifest-only store arrives on every connection that offers the
		// block, and dropping the partial would send the next re-offer back to
		// a from-scratch fetch.
		data.partialTxs = existing.partialTxs
		// Carrying the partial must not also restart the block's cache
		// lifetime. This store rebuilds the entry with a fresh insertedAt, and
		// the relay re-offers each endorser block on every connection, so a
		// steady trickle of offers would keep refreshing an incomplete entry
		// just before expiry and it would never be pruned -- now holding
		// transaction bodies rather than just a manifest. Keeping the original
		// timestamp while the block is still incomplete bounds a never-
		// completing block to one TTL, after which pruning evicts it and a
		// later offer starts over from the manifest. A store that completes
		// the transaction set does take the fresh timestamp: it has become a
		// servable entry and earns the same lifetime as any other.
		if len(data.partialTxs) > 0 && !data.completeTxCache() {
			data.insertedAt = existing.insertedAt
			data.seq = existing.seq
		}
		// A binding already established for this hash survives a later
		// peer-offered store. The slots are equal (checked above), so an
		// unverified store cannot demote an entry a trusted source already
		// bound.
		data.slotVerified = data.slotVerified || existing.slotVerified
	}
	if data.completeTxCache() {
		// The transaction set is whole; the resume state is now dead weight.
		data.partialTxs = nil
	}
	// Reject rather than cache an entry that would exceed the per-entry byte
	// budget. Any smaller entry already cached under cacheKeys (e.g. a
	// manifest-only store) is left untouched, since the map has not been
	// mutated yet at this point.
	if n := data.approxBytes(); n > leiosEndorserBlockCacheMaxEntryBytes {
		o.leiosMu.Unlock()
		o.leiosAnnouncementsMu.Unlock()
		return fmt.Errorf(
			"leios endorser block cache: entry size %d exceeds max %d",
			n,
			leiosEndorserBlockCacheMaxEntryBytes,
		)
	}
	for _, key := range cacheKeys {
		o.leiosEndorserBlocks[key] = data
	}
	// Wake any NtC serving path waiting on this closure once its transaction
	// set is complete. Completeness (txsRaw count == reference count) is the
	// same readiness predicate the resolver uses, so a waiter is only woken
	// when a subsequent merge would succeed.
	if data.completeTxCache() {
		for _, key := range cacheKeys {
			o.signalLeiosClosureWaitersLocked(key)
		}
	}
	o.pruneLeiosEndorserBlockCacheLocked(time.Now())
	o.leiosMu.Unlock()
	// The announcement/store race window this lock closes ends at the cache
	// insertion above; publishing has no further need for it.
	o.leiosAnnouncementsMu.Unlock()
	// Withhold everything keyed on the slot until the binding is verified. An
	// unverified entry is published by bindLeiosEndorserBlockSlot once its
	// announcement (or the ledger's chain-derived reference) corroborates the
	// slot, so a peer that offers before announcing cannot make dingo vote,
	// track pipeline timing, or persist a blob under a slot of its choosing.
	if data.slotVerified {
		o.publishLeiosEndorserBlock(point, blockRaw, blockHash, data)
	}
	return nil
}

// publishLeiosEndorserBlock performs the side effects keyed on a verified
// endorser-block slot. It must be called with leiosMu released.
func (o *Ouroboros) publishLeiosEndorserBlock(
	point ocommon.Point,
	blockRaw []byte,
	blockHash lcommon.Blake2b256,
	data *leiosEndorserBlockData,
) {
	// Queue manifest and (when complete) txs for asynchronous persistence to
	// the blob store so they can be served to downstream peers after the
	// in-memory cache expires. Best-effort and off the hot path: the write
	// happens on a background writer, not under the leios-fetch guard, so it
	// does not serialize against block application during catch-up.
	o.enqueueLeiosPersist(point, blockRaw, data)
	// Trigger local vote emission for the stored block, outside the
	// cache lock
	if o.leiosVotes != nil {
		o.leiosVotes.HandleEndorserBlock(point.Slot, blockHash)
	}
	// Register the block into the Leios pipeline for stage/timing
	// tracking and EB equivocation detection
	if o.leiosPipeline != nil {
		o.leiosPipeline.ObserveEndorserBlock(point.Slot, blockHash)
	}
}

// bindLeiosEndorserBlockSlot reconciles a cached endorser block against an
// authoritative slot -- a validated ranking-block announcement, or the point a
// ranking block the ledger is applying references. An entry cached before that
// authority arrived carries the offering connection's unverified claim, so it is
// promoted when the slot agrees and evicted when it does not, before anything
// keyed on the slot is published (issue #3513).
func (o *Ouroboros) bindLeiosEndorserBlockSlot(ebHash []byte, slot uint64) {
	// lookupLeiosEndorserBlock, not a direct map read: an entry may exist
	// only in the blob store (evicted from memory by TTL, or never loaded
	// this run), and loadLeiosEBFromDB reconstructs a reload as already
	// verified -- for whatever occurrence wrote it, which this authority can
	// still contradict (issue #3513 review; see the slot check below).
	data, ok := o.lookupLeiosEndorserBlock(ebHash)
	if !ok || data == nil {
		return
	}
	if data.point.Slot != slot {
		// The cached (or just-reloaded) entry was bound to a slot this
		// authority contradicts. This is checked before, and regardless of,
		// data.slotVerified: "verified" only means the slot was correct for
		// whatever occurrence established it, and the manifest is
		// content-addressed, so the same hash can legitimately recur at a
		// different slot later. Drop it: its bytes are content-addressed and
		// refetchable, and keeping it would leave a poisoned or stale slot in
		// the cache for the leios-fetch server, the ledger provider, and any
		// later store to inherit.
		o.leiosMu.Lock()
		if cur := o.leiosEndorserBlocks[leiosBlockKey(ebHash)]; cur != nil &&
			cur == data {
			o.deleteLeiosEndorserBlockDataLocked(cur)
		}
		o.leiosMu.Unlock()
		o.config.Logger.Debug(
			"evicted leios EB cached under a slot this authority contradicts",
			"component", "network",
			"protocol", "leios-notify",
			"cached_slot", data.point.Slot,
			"authoritative_slot", slot,
			"hash", hex.EncodeToString(ebHash),
		)
		return
	}
	if data.slotVerified {
		return
	}
	o.leiosMu.Lock()
	cur, ok := o.leiosEndorserBlocks[leiosBlockKey(ebHash)]
	if !ok || cur != data {
		// Superseded (or evicted) by a concurrent update since the lookup
		// above; nothing to promote.
		o.leiosMu.Unlock()
		return
	}
	// Cached entries are replaced, never mutated in place: lookupLeiosEndorserBlock
	// hands out this pointer and readers use it without leiosMu held, so
	// flipping slotVerified here directly would race a concurrent unlocked
	// read of the same field. Publish a copy instead, the same pattern
	// retainLeiosPartialTxs uses.
	verified := *data
	verified.slotVerified = true
	for _, key := range data.cacheKeys {
		if o.leiosEndorserBlocks[key] == data {
			o.leiosEndorserBlocks[key] = &verified
		}
	}
	o.leiosMu.Unlock()
	o.publishLeiosEndorserBlock(
		verified.point,
		verified.blockRaw,
		lcommon.Blake2b256Hash(verified.blockRaw),
		&verified,
	)
}

// leiosDatabase returns the underlying Database when the LedgerState is wired
// up, or nil when running without a database (unit tests, etc.).
func (o *Ouroboros) leiosDatabase() *database.Database {
	if o.ledgerState == nil {
		return nil
	}
	return o.ledgerState.Database()
}

func (data *leiosEndorserBlockData) completeTxCache() bool {
	return data != nil && len(data.txsRaw) == data.txCount
}

// approxBytes estimates the memory retained by one cache entry: the manifest
// plus every transaction body currently held, complete (txsRaw) or partial
// (partialTxs). It undercounts slice/map overhead, which is acceptable for a
// budget meant to bound the dominant cost -- the transaction and manifest
// payloads themselves -- rather than account for every byte.
func (data *leiosEndorserBlockData) approxBytes() int {
	if data == nil {
		return 0
	}
	total := len(data.blockRaw)
	for _, raw := range data.txsRaw {
		total += len(raw)
	}
	for _, raw := range data.partialTxs {
		total += len(raw)
	}
	return total
}

// partialTxCount returns how many of the endorser block's transactions are
// held from an incomplete fetch.
func (data *leiosEndorserBlockData) partialTxCount() int {
	if data == nil {
		return 0
	}
	n := 0
	for _, raw := range data.partialTxs {
		if raw != nil {
			n++
		}
	}
	return n
}

// mergeLeiosPartialTxs unions two sparse transaction slices into a txCount-long
// slice, preferring entries already held. Cached transaction bytes are never
// mutated after being published, so entries are aliased rather than cloned.
func mergeLeiosPartialTxs(
	held, add []cbor.RawMessage,
	txCount int,
) (merged []cbor.RawMessage, added int) {
	if txCount <= 0 {
		return nil, 0
	}
	merged = make([]cbor.RawMessage, txCount)
	copy(merged, held)
	for idx, raw := range add {
		if idx >= txCount || raw == nil || merged[idx] != nil {
			continue
		}
		merged[idx] = raw
		added++
	}
	return merged, added
}

// seedLeiosPartialTxsLocked fills result with the transactions already held for
// this endorser block. txsRaw is a dense prefix (a complete set, or one stored
// by an earlier caller); partialTxs is sparse.
func (data *leiosEndorserBlockData) seedLeiosPartialTxsLocked(
	result []cbor.RawMessage,
	validate func(int, cbor.RawMessage) error,
) {
	for idx, raw := range data.txsRaw {
		if idx >= len(result) || raw == nil {
			break
		}
		if validate != nil {
			if err := validate(idx, raw); err != nil {
				continue
			}
		}
		result[idx] = raw
	}
	for idx, raw := range data.partialTxs {
		if idx >= len(result) || raw == nil || result[idx] != nil {
			continue
		}
		if validate != nil {
			if err := validate(idx, raw); err != nil {
				continue
			}
		}
		result[idx] = raw
	}
}

// seedLeiosPartialTxs primes a fetch working slice with everything already held
// for hash, so a resumed fetch requests only the still-missing transactions.
// It reads the in-memory cache only: a fetch is driven by an offer for a block
// dingo is currently tracking, so a blob-store round trip on the fetch path
// would cost I/O without adding coverage.
func (o *Ouroboros) seedLeiosPartialTxs(
	hash []byte,
	result []cbor.RawMessage,
	validate func(int, cbor.RawMessage) error,
) {
	if len(result) == 0 {
		return
	}
	o.leiosMu.RLock()
	defer o.leiosMu.RUnlock()
	data, ok := o.leiosEndorserBlocks[leiosBlockKey(hash)]
	if !ok || data == nil || data.expired(time.Now()) {
		return
	}
	data.seedLeiosPartialTxsLocked(result, validate)
}

// retainLeiosPartialTxs merges an incomplete fetch result into the cached
// endorser block so a later offer of the same block can complete it. It is a
// no-op for a block that is not cached (nothing to complete) or already
// complete. The retained set only grows: two connections that each fetch part
// of the block contribute to one union rather than overwriting each other.
func (o *Ouroboros) retainLeiosPartialTxs(
	hash []byte,
	partial []cbor.RawMessage,
	validate func(int, cbor.RawMessage) error,
) {
	if len(partial) == 0 {
		return
	}
	key := leiosBlockKey(hash)
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	existing, ok := o.leiosEndorserBlocks[key]
	if !ok || existing == nil || existing.completeTxCache() ||
		existing.txCount <= 0 {
		return
	}
	held := existing.partialTxs
	add := partial
	removedHeld := false
	if validate != nil {
		held = cloneRawMessages(held)
		for idx, raw := range held {
			if raw != nil {
				if err := validate(idx, raw); err != nil {
					held[idx] = nil
					removedHeld = true
				}
			}
		}
		add = cloneRawMessages(add)
		for idx, raw := range add {
			if raw != nil {
				if err := validate(idx, raw); err != nil {
					add[idx] = nil
				}
			}
		}
	}
	merged, added := mergeLeiosPartialTxs(
		held,
		add,
		existing.txCount,
	)
	if added == 0 && !removedHeld {
		return
	}
	// Cached entries are replaced, never mutated in place: lookups hand out the
	// pointer and readers then use it without the lock, so publishing a copy
	// keeps those readers on a consistent snapshot. insertedAt is preserved so
	// retaining a partial cannot extend an endorser block's cache lifetime
	// indefinitely.
	updated := *existing
	updated.partialTxs = merged
	if updated.partialTxCount() == 0 {
		updated.partialTxs = nil
	}
	// Bound retained partial-fetch growth the same way storeLeiosEndorserBlock
	// bounds a full store: a peer that dribbles enough small partial responses
	// across repeated attempts could otherwise grow partialTxs past the
	// per-entry byte budget without ever going through that check, since this
	// path publishes directly rather than via storeLeiosEndorserBlock. Reject
	// the merge and keep the existing (smaller) cached entry rather than
	// publish an over-budget one.
	if n := updated.approxBytes(); n > leiosEndorserBlockCacheMaxEntryBytes {
		return
	}
	for _, cacheKey := range existing.cacheKeys {
		if o.leiosEndorserBlocks[cacheKey] == existing {
			o.leiosEndorserBlocks[cacheKey] = &updated
		}
	}
	o.pruneLeiosEndorserBlockCacheLocked(time.Now())
}

func (data *leiosEndorserBlockData) expired(now time.Time) bool {
	return data != nil &&
		data.insertedAt.Before(now.Add(-leiosEndorserBlockCacheTTL))
}

func (o *Ouroboros) pruneLeiosEndorserBlockCacheLocked(now time.Time) {
	if len(o.leiosEndorserBlocks) == 0 {
		return
	}
	uniqueBlocks := make(
		map[*leiosEndorserBlockData]struct{},
		len(o.leiosEndorserBlocks),
	)
	for key, data := range o.leiosEndorserBlocks {
		if data == nil {
			delete(o.leiosEndorserBlocks, key)
			continue
		}
		uniqueBlocks[data] = struct{}{}
	}
	for data := range uniqueBlocks {
		if data.expired(now) {
			o.deleteLeiosEndorserBlockDataLocked(data)
			delete(uniqueBlocks, data)
		}
	}
	totalBytes := totalLeiosEndorserBlockBytes(uniqueBlocks)
	if len(uniqueBlocks) <= leiosEndorserBlockCacheMaxEntries &&
		totalBytes <= leiosEndorserBlockCacheMaxBytes {
		return
	}
	blocks := make([]*leiosEndorserBlockData, 0, len(uniqueBlocks))
	for data := range uniqueBlocks {
		blocks = append(blocks, data)
	}
	// Sort by seq, not insertedAt: insertedAt is a wall-clock timestamp
	// captured before leiosMu is acquired, so a delayed goroutine can carry an
	// earlier timestamp into the map after a goroutine that actually won the
	// lock and inserted first. seq is assigned while the lock is held, so it
	// reflects true insertion order.
	slices.SortFunc(blocks, func(a, b *leiosEndorserBlockData) int {
		return cmp.Compare(a.seq, b.seq)
	})
	// Evict oldest-inserted entries first until both the entry-count and the
	// aggregate byte budget are satisfied. totalBytes is updated as each
	// victim is chosen instead of being recomputed from scratch.
	evict := 0
	for evict < len(blocks) &&
		(len(blocks)-evict > leiosEndorserBlockCacheMaxEntries ||
			totalBytes > leiosEndorserBlockCacheMaxBytes) {
		totalBytes -= blocks[evict].approxBytes()
		evict++
	}
	for _, data := range blocks[:evict] {
		o.deleteLeiosEndorserBlockDataLocked(data)
	}
}

// totalLeiosEndorserBlockBytes sums approxBytes across every unique cached
// endorser block, for the aggregate byte budget enforced by
// pruneLeiosEndorserBlockCacheLocked.
func totalLeiosEndorserBlockBytes(
	blocks map[*leiosEndorserBlockData]struct{},
) int {
	total := 0
	for data := range blocks {
		total += data.approxBytes()
	}
	return total
}

func (o *Ouroboros) deleteLeiosEndorserBlockDataLocked(
	data *leiosEndorserBlockData,
) {
	if len(data.cacheKeys) > 0 {
		for _, key := range data.cacheKeys {
			if o.leiosEndorserBlocks[key] == data {
				delete(o.leiosEndorserBlocks, key)
			}
		}
		return
	}
	for key, cached := range o.leiosEndorserBlocks {
		if cached == data {
			delete(o.leiosEndorserBlocks, key)
		}
	}
}

func (o *Ouroboros) lookupLeiosEndorserBlock(
	hash []byte,
) (*leiosEndorserBlockData, bool) {
	key := leiosBlockKey(hash)
	now := time.Now()
	o.leiosMu.RLock()
	data, ok := o.leiosEndorserBlocks[key]
	if !ok || data == nil {
		o.leiosMu.RUnlock()
		// Memory cache miss: try the persistent blob store so we can serve
		// historical EBs whose in-memory TTL has elapsed.
		return o.loadLeiosEBFromDB(hash)
	}
	if !data.expired(now) {
		o.leiosMu.RUnlock()
		return data, true
	}
	o.leiosMu.RUnlock()

	o.leiosMu.Lock()
	data, ok = o.leiosEndorserBlocks[key]
	if !ok || data == nil {
		o.leiosMu.Unlock()
		return o.loadLeiosEBFromDB(hash)
	}
	if data.expired(now) {
		o.deleteLeiosEndorserBlockDataLocked(data)
		o.leiosMu.Unlock()
		return o.loadLeiosEBFromDB(hash)
	}
	o.leiosMu.Unlock()
	return data, true
}

// loadLeiosEBFromDB loads a Leios endorser block's manifest (and txs, if
// stored) from the persistent blob store and caches the result in memory.
// Returns (nil, false) when the blob store has no manifest for this hash.
func (o *Ouroboros) loadLeiosEBFromDB(
	hash []byte,
) (*leiosEndorserBlockData, bool) {
	db := o.leiosDatabase()
	if db == nil {
		return nil, false
	}
	slot, manifestRaw, err := db.GetLeiosEBManifest(hash)
	if err != nil {
		// ErrBlobKeyNotFound is the normal "not stored" path; anything else
		// is worth surfacing at Debug for diagnostics.
		if !errors.Is(err, types.ErrBlobKeyNotFound) {
			o.config.Logger.Debug(
				"failed to load leios EB manifest from blob store",
				"component", "network",
				"error", err,
			)
		}
		return nil, false
	}
	block, err := lcommon.NewLeiosEndorserBlockFromCbor(manifestRaw)
	if err != nil {
		o.config.Logger.Debug(
			"failed to decode leios EB manifest loaded from blob store",
			"component", "network",
			"error", err,
		)
		return nil, false
	}
	// Load txs if they were persisted (best-effort; may not be present for
	// EBs that completed before tx persistence was added).
	txsRaw, err := db.GetLeiosEBTxs(hash)
	if err != nil && !errors.Is(err, types.ErrBlobKeyNotFound) {
		o.config.Logger.Debug(
			"failed to load leios EB txs from blob store",
			"component", "network",
			"error", err,
		)
		return nil, false
	}
	if len(txsRaw) > 0 {
		if err := validateLeiosEndorserBlockTxs(manifestRaw, txsRaw); err != nil {
			// Transaction blobs written before manifest binding was enforced (or
			// corrupted afterward) are only a historical-serving cache. Keep the
			// content-addressed manifest, but discard the untrusted bodies so the
			// normal by-point path fetches and persists a verified replacement.
			o.config.Logger.Debug(
				"discarding leios EB txs that mismatch persisted manifest",
				"component", "network",
				"hash", hex.EncodeToString(hash),
				"error", err,
			)
			txsRaw = nil
		}
	}

	cacheKeys := []string{leiosBlockKey(hash)}
	data := &leiosEndorserBlockData{
		point:      ocommon.Point{Slot: slot, Hash: slices.Clone(hash)},
		blockRaw:   slices.Clone(manifestRaw),
		txsRaw:     cloneRawMessages(txsRaw),
		txCount:    len(block.TransactionReferences),
		cacheKeys:  cacheKeys,
		insertedAt: time.Now(),
		// The blob store is written only from publishLeiosEndorserBlock,
		// which never runs before slotVerified is true (see
		// storeLeiosEndorserBlock and bindLeiosEndorserBlockSlot), so a
		// persisted (slot, hash) pair is already an authoritatively bound
		// one. Reconstructing it as unverified would withhold it from
		// EndorserBlockTxsByHash until something re-verifies a hash whose
		// announcement may have long since aged out of the acceptance
		// window (issue #3513 review).
		slotVerified: true,
	}
	// A persisted (or pre-cap-era) blob can exceed the per-entry byte budget
	// even though storeLeiosEndorserBlock would reject it on the write path;
	// this reload path must apply the same check rather than let a
	// leios-fetch MsgBlockRequest for an old point repopulate the cache past
	// the limit on every cache miss. Serve it to the caller uncached rather
	// than dropping it outright.
	if n := data.approxBytes(); n > leiosEndorserBlockCacheMaxEntryBytes {
		o.config.Logger.Debug(
			"leios EB reloaded from blob store exceeds max entry size; serving uncached",
			"component", "network",
			"hash", hex.EncodeToString(hash),
			"size", n,
			"max_size", leiosEndorserBlockCacheMaxEntryBytes,
		)
		return data, true
	}
	// Populate the in-memory cache so subsequent lookups skip the DB.
	o.leiosMu.Lock()
	if o.leiosEndorserBlocks == nil {
		o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	}
	// Only cache if no fresher entry has appeared while we were loading.
	if existing := o.leiosEndorserBlocks[cacheKeys[0]]; existing == nil ||
		existing.expired(time.Now()) {
		o.pruneLeiosEndorserBlockCacheLocked(time.Now())
		o.leiosEndorserBlockSeq++
		data.seq = o.leiosEndorserBlockSeq
		for _, key := range cacheKeys {
			o.leiosEndorserBlocks[key] = data
		}
		// Prune again after inserting so a reload that fits the per-entry
		// budget but pushes the aggregate over its budget is evicted
		// promptly, the same way storeLeiosEndorserBlock prunes both before
		// and after admitting an entry.
		o.pruneLeiosEndorserBlockCacheLocked(time.Now())
	} else {
		data = existing
	}
	o.leiosMu.Unlock()
	return data, true
}

func leiosTxsFromBitmap(
	txs []cbor.RawMessage,
	bitmaps map[uint16]uint64,
) []cbor.RawMessage {
	if len(txs) == 0 || len(bitmaps) == 0 {
		return nil
	}
	ret := make([]cbor.RawMessage, 0, len(txs))
	for idx, tx := range txs {
		bucket := idx / 64
		if bucket > math.MaxUint16 {
			break
		}
		mask := bitmaps[uint16(bucket)] // #nosec G115 -- checked above
		// MSB-first bitmap (see leiosWindowNeededMask): the tx at window
		// offset o is bit 63-o.
		if mask&(1<<uint(63-(idx%64))) == 0 {
			continue
		}
		ret = append(ret, slices.Clone(tx))
	}
	return ret
}

func validateLeiosTxBitmap(count int, bitmaps map[uint16]uint64) error {
	for bucket, mask := range bitmaps {
		if mask == 0 {
			continue
		}
		baseIdx := int(bucket) * 64
		for bit := range 64 {
			if mask&(1<<uint(bit)) == 0 {
				continue
			}
			// MSB-first bitmap (see leiosWindowNeededMask): bit b denotes
			// window offset 63-b.
			idx := baseIdx + (63 - bit)
			if idx >= count {
				return fmt.Errorf(
					"leios tx bitmap references tx index %d beyond %d cached txs",
					idx,
					count,
				)
			}
		}
	}
	return nil
}

// EndorserBlockTxsByHash returns the slot and the complete set of standalone
// transaction CBORs of the cached endorser block with the given hash, for the
// ledger to apply when the referencing Dijkstra ranking block is processed. ok
// is false when the endorser block is not cached, its transactions are
// incomplete, or its slot binding is not yet verified -- the ledger keys the
// endorser blob it persists on this slot, so an unverified peer claim must not
// reach it (issue #3513). It satisfies ledger.EndorserBlockProviderFunc.
func (o *Ouroboros) EndorserBlockTxsByHash(
	ebHash []byte,
) (uint64, []cbor.RawMessage, bool) {
	data, ok := o.lookupLeiosEndorserBlock(ebHash)
	if !ok || !data.completeTxCache() || !data.slotVerified {
		return 0, nil, false
	}
	return data.point.Slot, cloneRawMessages(data.txsRaw), true
}

// EndorserBlockTxHashesByHash returns the manifest-order transaction hashes of
// a complete cached endorser block. The forge loop uses this to build the
// prototype-2026w29 post-certificate mempool view: transactions in the EB being
// certified are excluded from the new EB announced by the same ranking block.
func (o *Ouroboros) EndorserBlockTxHashesByHash(
	ebHash []byte,
) ([]string, bool) {
	data, ok := o.lookupLeiosEndorserBlock(ebHash)
	if !ok || !data.completeTxCache() {
		return nil, false
	}
	block, err := lcommon.NewLeiosEndorserBlockFromCbor(data.blockRaw)
	if err != nil {
		return nil, false
	}
	hashes := make([]string, len(block.TransactionReferences))
	for i, ref := range block.TransactionReferences {
		hashes[i] = hex.EncodeToString(ref.TransactionHash.Bytes())
	}
	return hashes, true
}

// leiosAnnouncementFromBlockCbor returns the endorser block announced by the
// given ranking block's header, if any. It works for both the Conway-tagged
// (5-component) and Dijkstra (2-component) Musashi block shapes because the
// header is element 0 in both, and the Leios-extended header decodes uniformly
// via DijkstraBlockHeader.
func leiosAnnouncementFromBlockCbor(
	blockCbor []byte,
) (lcommon.Blake2b256, bool) {
	var top []cbor.RawMessage
	if _, err := cbor.Decode(blockCbor, &top); err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, false
	}
	var header gdijkstra.DijkstraBlockHeader
	if _, err := cbor.Decode(top[0], &header); err != nil {
		return lcommon.Blake2b256{}, false
	}
	ebHash, _, ok := header.LeiosAnnouncement()
	if !ok {
		return lcommon.Blake2b256{}, false
	}
	return ebHash, true
}

// certifiedEndorserBlockHash resolves the endorser block a certifying ranking
// block (CertRB) inlines over node-to-client. It returns two independent flags:
//   - certified: the header is a CertRB (leios_certified is set). Once true, the
//     block must never be served raw — the caller either serves the merged block
//     or closes the connection.
//   - resolved: the certified endorser block hash (ebHash) was resolved.
//     resolved implies certified. When certified is true but resolved is false
//     (parent lookup or announcement resolution failed) the caller must
//     disconnect rather than fall back to the raw, empty-transaction block.
//
// As of prototype-2026w29 the endorser block a CertRB certifies is not named in
// the CertRB itself: the CertRB carries a leios_certificate and empty
// transaction segments, and the endorser block is the one announced by the
// immediately-preceding block on the chain (the prototype's prevAnn mechanism;
// see ouroboros-consensus MiniProtocol/ChainSync/Server.hs). We reproduce that
// by resolving the parent via the header prev-hash and reading its
// leios_announcement. In w29 the CertRB may independently announce a new EB;
// that current announcement is not the certified closure resolved here.
func (o *Ouroboros) certifiedEndorserBlockHash(
	blockCbor []byte,
) (ebHash lcommon.Blake2b256, certified bool, resolved bool) {
	var top []cbor.RawMessage
	if _, err := cbor.Decode(blockCbor, &top); err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, false, false
	}
	var header gdijkstra.DijkstraBlockHeader
	if _, err := cbor.Decode(top[0], &header); err != nil {
		return lcommon.Blake2b256{}, false, false
	}
	if cert, present := header.LeiosCertified(); !present || !cert {
		return lcommon.Blake2b256{}, false, false
	}
	// The header is certified from here on; a resolution failure below keeps
	// certified=true so the caller disconnects instead of serving raw.
	if o.ledgerState == nil {
		return lcommon.Blake2b256{}, true, false
	}
	prevHash := header.PrevHash()
	parent, err := o.ledgerState.BlockByHash(prevHash.Bytes())
	if err != nil {
		return lcommon.Blake2b256{}, true, false
	}
	hash, ok := leiosAnnouncementFromBlockCbor(parent.Cbor)
	if !ok {
		return lcommon.Blake2b256{}, true, false
	}
	return hash, true, true
}

// resolveCertifiedEndorserTxs returns the endorser-block transactions that a
// certifying ranking block (CertRB) inlines over node-to-client, or ok=false
// when the block is not a CertRB or its endorser block is not fully available.
func (o *Ouroboros) resolveCertifiedEndorserTxs(
	blockCbor []byte,
) ([]cbor.RawMessage, bool) {
	ebHash, _, resolved := o.certifiedEndorserBlockHash(blockCbor)
	if !resolved {
		return nil, false
	}
	data, found := o.lookupLeiosEndorserBlock(ebHash.Bytes())
	if !found || !data.completeTxCache() {
		return nil, false
	}
	return cloneRawMessages(data.txsRaw), true
}

// leiosClosureCompleteLocked reports whether a complete transaction closure is
// cached in memory for the given cache key, using the same readiness predicate
// (completeTxCache) as the resolver. The caller must hold leiosMu.
func (o *Ouroboros) leiosClosureCompleteLocked(key string) bool {
	data, ok := o.leiosEndorserBlocks[key]
	return ok && data.completeTxCache()
}

// signalLeiosClosureWaitersLocked wakes and clears every waiter registered for
// the given cache key. The caller must hold leiosMu.
func (o *Ouroboros) signalLeiosClosureWaitersLocked(key string) {
	waiters := o.leiosClosureWaiters[key]
	if len(waiters) == 0 {
		return
	}
	for _, ch := range waiters {
		close(ch)
	}
	delete(o.leiosClosureWaiters, key)
}

// removeLeiosClosureWaiter deregisters a waiter channel, e.g. after its context
// is cancelled. It does not close the channel.
func (o *Ouroboros) removeLeiosClosureWaiter(key string, ch chan struct{}) {
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	waiters := o.leiosClosureWaiters[key]
	for i, w := range waiters {
		if w == ch {
			o.leiosClosureWaiters[key] = slices.Delete(waiters, i, i+1)
			break
		}
	}
	if len(o.leiosClosureWaiters[key]) == 0 {
		delete(o.leiosClosureWaiters, key)
	}
}

// waitForLeiosEndorserClosure blocks until a complete transaction closure for
// ebHash is cached in memory or ctx is done. It returns true once the closure
// is available.
func (o *Ouroboros) waitForLeiosEndorserClosure(
	ctx context.Context,
	ebHash []byte,
) bool {
	key := leiosBlockKey(ebHash)
	o.leiosMu.Lock()
	if o.leiosClosureCompleteLocked(key) {
		o.leiosMu.Unlock()
		return true
	}
	ch := make(chan struct{})
	o.leiosClosureWaiters[key] = append(o.leiosClosureWaiters[key], ch)
	o.leiosMu.Unlock()
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		o.removeLeiosClosureWaiter(key, ch)
		// The store path may have completed the closure between ctx
		// cancellation and deregistration; re-check to avoid a lost wakeup.
		o.leiosMu.RLock()
		defer o.leiosMu.RUnlock()
		return o.leiosClosureCompleteLocked(key)
	}
}

// awaitMergedLeiosRankingBlock waits (bounded by ctx) for a certifying ranking
// block's endorser closure and returns the merged CBOR once available. It
// returns ok=false if the closure does not arrive before ctx is done.
func (o *Ouroboros) awaitMergedLeiosRankingBlock(
	ctx context.Context,
	blockCbor []byte,
	ebHash lcommon.Blake2b256,
) ([]byte, bool) {
	if !o.waitForLeiosEndorserClosure(ctx, ebHash.Bytes()) {
		return nil, false
	}
	merged, ok, err := o.mergedLeiosRankingBlockCbor(blockCbor)
	if err != nil || !ok {
		return nil, false
	}
	return merged, true
}

// spliceEndorserTxsIntoDijkstraBlock returns rankingBlockCbor with the endorser
// block's transactions inlined into the ranking block's (empty) transaction
// segment, matching the node-to-client "merged" block the prototype serves for
// a certifying ranking block. The Dijkstra block is [header, block_body] with
// block_body = [invalid_transactions, transactions, leios_certificate,
// peras_certificate]; only the transactions element (index 1) is replaced. The
// header, certificate, peras, and invalid-transactions elements are preserved
// verbatim so the served block's hash (a hash of the header) is unchanged; the
// header's block_body_hash intentionally no longer matches, which is acceptable
// over node-to-client because local clients do not re-verify the body hash.
//
// It returns an error (and the caller serves the raw block) when the block is
// not a fillable CertRB shape: the top level must have two elements, the body
// four, and the existing transactions segment must be empty. ebTxsRaw must be
// complete Dijkstra transactions ([transaction_body, transaction_witness_set,
// auxiliary_data/nil]) in endorser-block order.
func spliceEndorserTxsIntoDijkstraBlock(
	rankingBlockCbor []byte,
	ebTxsRaw []cbor.RawMessage,
) ([]byte, error) {
	var top []cbor.RawMessage
	if _, err := cbor.Decode(rankingBlockCbor, &top); err != nil {
		return nil, fmt.Errorf("decode dijkstra block: %w", err)
	}
	if len(top) != 2 {
		return nil, fmt.Errorf(
			"dijkstra block has %d top-level elements, expected 2",
			len(top),
		)
	}
	var body []cbor.RawMessage
	if _, err := cbor.Decode(top[1], &body); err != nil {
		return nil, fmt.Errorf("decode dijkstra block body: %w", err)
	}
	if len(body) != 4 {
		return nil, fmt.Errorf(
			"dijkstra block body has %d elements, expected 4",
			len(body),
		)
	}
	var existingTxs []cbor.RawMessage
	if _, err := cbor.Decode(body[1], &existingTxs); err != nil {
		return nil, fmt.Errorf("decode dijkstra transactions: %w", err)
	}
	if len(existingTxs) != 0 {
		return nil, fmt.Errorf(
			"ranking block already has %d transactions; not a fillable CertRB",
			len(existingTxs),
		)
	}
	newTxs, err := cbor.Encode(ebTxsRaw)
	if err != nil {
		return nil, fmt.Errorf("encode endorser transactions: %w", err)
	}
	newBody, err := cbor.Encode([]cbor.RawMessage{
		body[0], cbor.RawMessage(newTxs), body[2], body[3],
	})
	if err != nil {
		return nil, fmt.Errorf("encode merged block body: %w", err)
	}
	merged, err := cbor.Encode([]cbor.RawMessage{
		top[0], cbor.RawMessage(newBody),
	})
	if err != nil {
		return nil, fmt.Errorf("encode merged block: %w", err)
	}
	return merged, nil
}

// mergedLeiosRankingBlockCbor returns the node-to-client representation of a
// ranking block. For a certifying ranking block it inlines the certified
// endorser block's transactions (ok=true); every other block is returned
// unchanged (ok=false). An error is returned only when a CertRB was identified
// but its bytes could not be spliced, in which case the caller serves the raw
// block.
func (o *Ouroboros) mergedLeiosRankingBlockCbor(
	blockCbor []byte,
) ([]byte, bool, error) {
	ebTxsRaw, ok := o.resolveCertifiedEndorserTxs(blockCbor)
	if !ok {
		return blockCbor, false, nil
	}
	merged, err := spliceEndorserTxsIntoDijkstraBlock(blockCbor, ebTxsRaw)
	if err != nil {
		return blockCbor, false, err
	}
	return merged, true, nil
}

// chainsyncServerBlockCbor returns the CBOR to serve for a block over NtC
// chainsync. For a certifying ranking block it inlines the certified endorser
// block's transactions. It returns errLeiosClosureUnresolved when a CertRB
// cannot be served with its transactions (closure did not arrive within the
// wait window, or the certified endorser reference could not be resolved); the
// caller must then close the connection (rather than RollForward an incomplete
// block) so the client retries the same point once the closure is available.
func (o *Ouroboros) chainsyncServerBlockCbor(
	ctx ochainsync.CallbackContext,
	block models.Block,
) ([]byte, error) {
	if !o.config.EnableLeios ||
		block.Type != uint(gdijkstra.BlockTypeDijkstra) ||
		ctx.Server == nil {
		return block.Cbor, nil
	}
	p := ctx.Server.ProtocolInstance()
	if p == nil || p.Mode() != protocol.ProtocolModeNodeToClient {
		return block.Cbor, nil
	}
	// The connection id, not p.DoneChan(): this callback runs inside
	// gouroboros's recvLoop, which cannot finish (and so cannot close
	// DoneChan) until the callback returns. A pending closure wait is
	// released through connmanager's per-connection watcher instead.
	return o.serveLeiosRankingBlockCbor(block, ctx.ConnectionId)
}

// serveLeiosRankingBlockCbor resolves the NtC representation of a Dijkstra
// ranking block, once the caller has confirmed Leios NtC serving applies. It is
// separated from chainsyncServerBlockCbor's protocol guards so the serving
// decision (merge / serve-raw / disconnect) is unit-testable without a live
// chainsync server. A block whose header is certified is never downgraded to
// the raw serve path: it is either merged or an error is returned so the
// connection is closed. connId identifies the serving connection so that any
// closure wait is bounded by that connection's lifetime in addition to the
// configured timeout.
func (o *Ouroboros) serveLeiosRankingBlockCbor(
	block models.Block,
	connId ouroboros.ConnectionId,
) ([]byte, error) {
	merged, ok, err := o.mergedLeiosRankingBlockCbor(block.Cbor)
	if err != nil {
		// A CertRB was identified but its bytes could not be spliced (malformed
		// shape). This is a structural fault, not a missing closure; serve the
		// raw block as a CBOR-safety fallback rather than wedging the client.
		o.config.Logger.Warn(
			"failed to build merged Leios block for NtC chainsync",
			"error", err,
			"slot", block.Slot,
		)
		return block.Cbor, nil
	}
	if ok {
		o.recordLeiosCertRbOutcome("merged")
		o.config.Logger.Debug(
			"serving merged Leios block over NtC chainsync",
			"slot", block.Slot,
			"hash", hex.EncodeToString(block.Hash),
		)
		return merged, nil
	}
	ebHash, certified, resolved := o.certifiedEndorserBlockHash(block.Cbor)
	if !certified {
		// Not a certifying ranking block (announcing or plain); serve as-is.
		return block.Cbor, nil
	}
	if !resolved {
		// The header is certified but the endorser reference could not be
		// resolved (parent block missing or no announcement). Never serve a
		// certified block raw; close the connection so the client retries once
		// the parent/closure is available.
		o.recordLeiosCertRbOutcome("unresolved")
		o.config.Logger.Warn(
			"certified ranking block with unresolvable endorser reference over NtC chainsync; closing connection so the client retries rather than recording a block with no transactions",
			"slot",
			block.Slot,
			"hash",
			hex.EncodeToString(block.Hash),
		)
		return nil, fmt.Errorf(
			"%w: certified block slot %d hash %s has no resolvable endorser reference",
			errLeiosClosureUnresolved,
			block.Slot,
			hex.EncodeToString(block.Hash),
		)
	}
	// Certified and resolved: wait a bounded window for the endorser closure.
	return o.serveLeiosCertRbWithWait(block, ebHash, connId)
}

// serveLeiosCertRbWithWait waits a bounded window for a certifying ranking
// block's endorser closure to be cached, returning the merged block if it
// arrives. The wait is bounded by two independent things, whichever elapses
// first: the ledger's endorser-block wait window (leiosClosureWaitTimeout),
// and the lifetime of the serving connection connId. A client that
// disconnects while the wait is pending therefore wakes it immediately rather
// than leaving it parked for the rest of the window. On timeout or
// cancellation it returns errLeiosClosureUnresolved so the caller closes the
// connection instead of serving an incomplete (empty-transaction) CertRB —
// the client then retries the same point, avoiding a permanently-incomplete
// record.
func (o *Ouroboros) serveLeiosCertRbWithWait(
	block models.Block,
	ebHash lcommon.Blake2b256,
	connId ouroboros.ConnectionId,
) ([]byte, error) {
	connDone, cancelWaiter := o.registerLeiosServeWaiter(connId)
	defer cancelWaiter()
	ctx, cancel := context.WithTimeout(
		leiosConnDoneContext{done: connDone},
		o.leiosClosureWaitTimeout(),
	)
	defer cancel()
	start := time.Now()
	merged, ok := o.awaitMergedLeiosRankingBlock(ctx, block.Cbor, ebHash)
	waited := time.Since(start)
	if ok {
		o.recordLeiosCertRbOutcome("merged_after_wait")
		o.recordLeiosCertRbWait("resolved", waited)
		o.config.Logger.Debug(
			"serving merged Leios block over NtC chainsync after closure wait",
			"slot", block.Slot,
			"hash", hex.EncodeToString(block.Hash),
			"eb_hash", ebHash.String(),
			"waited", waited,
		)
		return merged, nil
	}
	// Distinguish why the wait ended: the configured window elapsed
	// (timeout), or connDone closed first (cancelled) -- e.g. the client
	// disconnected or the request ended while the closure was still missing.
	// context.WithTimeout propagates the parent's cancellation cause, so a
	// parent-driven end reports context.Canceled here even though the
	// deadline had not yet elapsed.
	waitOutcome := "timeout"
	if errors.Is(ctx.Err(), context.Canceled) {
		waitOutcome = "cancelled"
	}
	o.recordLeiosCertRbOutcome("unresolved")
	o.recordLeiosCertRbWait(waitOutcome, waited)
	o.config.Logger.Warn(
		"endorser closure unresolved for CertRB within wait window; closing NtC chainsync connection so the client retries rather than recording a block with no transactions",
		"slot",
		block.Slot,
		"hash",
		hex.EncodeToString(block.Hash),
		"eb_hash",
		ebHash.String(),
		"waited",
		waited,
		"wait_outcome",
		waitOutcome,
	)
	return nil, fmt.Errorf(
		"%w: slot %d hash %s eb %s (waited %s, %s)",
		errLeiosClosureUnresolved,
		block.Slot,
		hex.EncodeToString(block.Hash),
		ebHash.String(),
		waited,
		waitOutcome,
	)
}
