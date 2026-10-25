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
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
	"unsafe"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/internal/txresponse"
	"github.com/blinklabs-io/dingo/ledger"
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

type leiosServeWaiter struct {
	owner *ochainsync.Server
	ch    chan struct{}
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
// goes away, so a serving wait is released as soon as the peer disconnects.
// The returned cancel function deregisters the waiter and must always be called.
//
// The liveness re-check after registration closes the race with a connection
// that is already going away: connmanager removes the connection from its map
// before invoking its close callback, so a missing or replaced owner means either the
// release has already run (and would have closed a registered channel) or it
// is about to. Either way the caller must not wait, so the channel is closed
// before it is returned.
func (o *Ouroboros) registerLeiosServeWaiter(
	connId ouroboros.ConnectionId,
	owner *ochainsync.Server,
	checkLiveness ...bool,
) (done <-chan struct{}, cancel func()) {
	ch := make(chan struct{})
	if owner == nil {
		close(ch)
		return ch, func() {}
	}
	o.leiosServeWaitersMu.Lock()
	if o.leiosServeWaiters == nil {
		o.leiosServeWaiters = make(
			map[ouroboros.ConnectionId][]leiosServeWaiter,
		)
	}
	o.leiosServeWaiters[connId] = append(
		o.leiosServeWaiters[connId],
		leiosServeWaiter{owner: owner, ch: ch},
	)
	o.leiosServeWaitersMu.Unlock()

	cancel = func() {
		o.leiosServeWaitersMu.Lock()
		defer o.leiosServeWaitersMu.Unlock()
		waiters := o.leiosServeWaiters[connId]
		for i, w := range waiters {
			if w.ch == ch {
				o.leiosServeWaiters[connId] = slices.Delete(waiters, i, i+1)
				break
			}
		}
		if len(o.leiosServeWaiters[connId]) == 0 {
			delete(o.leiosServeWaiters, connId)
		}
	}

	// The connection manager is absent in unit tests that exercise the
	// serving decision directly; there is no liveness to check. Such callers
	// must arrange explicit release or their own timeout.
	if (len(checkLiveness) == 0 || checkLiveness[0]) && o.connManager != nil {
		conn := o.connManager.GetConnectionById(connId)
		if conn == nil || (owner != nil &&
			(conn.ChainSync() == nil || conn.ChainSync().Server != owner)) {
			o.releaseLeiosServeWaiter(connId, ch)
		}
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
		if w.ch == ch {
			o.leiosServeWaiters[connId] = slices.Delete(waiters, i, i+1)
			if len(o.leiosServeWaiters[connId]) == 0 {
				delete(o.leiosServeWaiters, connId)
			}
			close(ch)
			return
		}
	}
}

// ReleaseLeiosServeWaiters wakes every serving wait pending on connId and
// clears them. It is retained for tests and whole-instance cleanup; live
// connection-close handling uses the owner-scoped variant below.
func (o *Ouroboros) ReleaseLeiosServeWaiters(
	connId ouroboros.ConnectionId,
) {
	o.leiosServeWaitersMu.Lock()
	waiters := o.leiosServeWaiters[connId]
	delete(o.leiosServeWaiters, connId)
	o.leiosServeWaitersMu.Unlock()
	for _, waiter := range waiters {
		close(waiter.ch)
	}
}

// ReleaseLeiosServeWaitersOwner wakes only waits owned by one chainsync
// server instance, so a delayed close callback cannot cancel a replacement.
func (o *Ouroboros) ReleaseLeiosServeWaitersOwner(
	connId ouroboros.ConnectionId,
	owner *ochainsync.Server,
) {
	if owner == nil {
		return
	}
	o.leiosServeWaitersMu.Lock()
	waiters := o.leiosServeWaiters[connId]
	remaining := waiters[:0]
	for _, waiter := range waiters {
		if waiter.owner == owner {
			close(waiter.ch)
		} else {
			remaining = append(remaining, waiter)
		}
	}
	if len(remaining) < len(waiters) {
		clear(waiters[len(remaining):])
	}
	if len(remaining) == 0 {
		delete(o.leiosServeWaiters, connId)
	} else {
		o.leiosServeWaiters[connId] = remaining
	}
	o.leiosServeWaitersMu.Unlock()
}

// RegisterLeiosServeWaiterForTesting exposes registerLeiosServeWaiter so the
// root package can prove its connection-closed callback actually releases a
// parked NtC serving wait. The release wiring spans two packages, so the
// assertion cannot be made from inside either one alone.
func (o *Ouroboros) RegisterLeiosServeWaiterForTesting(
	connId ouroboros.ConnectionId,
) (done <-chan struct{}, cancel func()) {
	var owner *ochainsync.Server
	if o.connManager != nil {
		if conn := o.connManager.GetConnectionById(connId); conn != nil &&
			conn.ChainSync() != nil {
			owner = conn.ChainSync().Server
		}
	}
	return o.registerLeiosServeWaiter(connId, owner)
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

const (
	leiosEBValidationUnknown uint8 = iota
	leiosEBValidationPending
	leiosEBValidationValid
	leiosEBValidationInvalid
)

type leiosEndorserBlockData struct {
	point                    ocommon.Point
	blockRaw                 []byte
	txsRaw                   []cbor.RawMessage
	announcementHeaderRaw    []byte
	semanticValidationStatus uint8
	semanticValidationParent []byte
	semanticValidationDone   chan struct{}
	// partialTxs retains an incomplete fetch: a txCount-long slice whose nil
	// entries are the transactions still missing (the same representation
	// leiosNeededBitmap turns into a request bitmap). The relay diffuses an
	// endorser block's transactions over several seconds, so a fetch near the
	// live tip routinely runs out of served transactions before the block is
	// whole. Keeping what it did gather here lets the next offer of the same
	// block fetch only the missing tail instead of starting over, without
	// holding a per-connection fetch slot open across the gap.
	// It is
	// cleared once txsRaw is complete, and it is bounded by the same cache
	// TTL and entry cap as any other cached endorser block.
	partialTxs []cbor.RawMessage
	txCount    int
	cacheKeys  []string
	insertedAt time.Time
	// relayOfferRequested is set for content received through LeiosNotify or
	// historical by-point fetch. The cached EB is re-offered only after its slot
	// is verified, and its transactions only after their complete set has been
	// validated.
	relayOfferRequested      bool
	relayManifestOffered     bool
	relayTransactionsOffered bool
	// relayOfferSource identifies the most recent live connection that offered
	// this exact occurrence. LeiosFetch has no not-found response, so a
	// by-point request to a peer that never advertised the occurrence closes
	// the shared bearer. Keeping one bounded source lets historical backfill
	// try a peer known to hold the requested point first.
	relayOfferSource string
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
	// it before its announcement arrives. This field is never
	// mutated on a cache entry that is already reachable by another reader:
	// the cache stores *leiosEndorserBlockData, and lookupLeiosEndorserBlock
	// hands that pointer to callers outside leiosMu, so promoting an entry
	// (bindLeiosEndorserBlockSlot) replaces it with a new, distinct value
	// under the lock rather than flipping this field on the shared one.
	slotVerified bool
}

type selectedLeiosHeader struct {
	raw  []byte
	refs uint64
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
	// a locally forged endorser block.
	leiosStoreAuthoritative
	// leiosStoreBackfill is a store fetched from a peer for a point established
	// by the ranking block the ledger is applying. It is authoritative and is
	// relayed after validation so downstream peers can fetch the same closure.
	leiosStoreBackfill
)

// leiosBlockKey returns the leiosEndorserBlocks cache identity for one
// occurrence of an endorser block. It composes slot and hash, not hash
// alone: the manifest is content-addressed, so the same hash can be a live,
// independently required occurrence at more than one slot at once (two
// elections producing an identical transaction-reference set), and a
// hash-only key could hold only one of them at a time -- silently evicting
// or masking the other. Pending fetches and closure waiters use the same
// composite identity so each path agrees on what "the same occurrence" means.
func leiosBlockKey(slot uint64, hash []byte) string {
	return fmt.Sprintf("%d:%s", slot, hash)
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
// manifest before the complete set is cached or applied. Each Leios manifest
// reference is content-addressed over the FULL serialized transaction: the
// TransactionHash is Blake2b256 of the complete block_transaction CBOR (NOT the
// Cardano tx-id / body hash) and TransactionSize is that CBOR's length. Both
// the fetch-side validator here and the forge-side reference builder
// (buildLeiosEB) use this same full-transaction contract, matching Haskell
// reference nodes. The bytes are the Dijkstra block_transaction, including its
// is_valid flag.
func validateLeiosEndorserBlockTxs(
	manifestRaw []byte,
	txsRaw []cbor.RawMessage,
) error {
	block, err := decodeLeiosEndorserBlock(manifestRaw)
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

// validateLeiosEndorserBlockTx checks one peer-delivered endorser transaction
// against its manifest reference. Both envelope decodes read leios-fetch bytes
// and go through safedecode.Cbor, so a decoder panic is reported as the
// decode failure this function already returns rather than unwinding into the
// Leios protocol worker. The function is a pure predicate over (ref, raw) --
// it decodes into locals, hashes, and compares -- so containing a panic here
// cannot leave shared state half-updated.
func validateLeiosEndorserBlockTx(
	index int,
	ref lcommon.LeiosTransactionReference,
	raw cbor.RawMessage,
) error {
	txCbor, err := leiosEndorserBlockTxCbor(raw)
	if err != nil {
		return fmt.Errorf("decode endorser tx %d envelope: %w", index, err)
	}
	if len(txCbor) == 0 {
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
	if txHash := lcommon.Blake2b256Hash(txCbor); txHash != ref.TransactionHash {
		return fmt.Errorf("endorser tx %d hash mismatch", index)
	}
	return nil
}

func validateLeiosEndorserBlockTxStructure(
	index int,
	raw cbor.RawMessage,
) error {
	txCbor, err := leiosEndorserBlockTxCbor(raw)
	if err != nil {
		return fmt.Errorf("decode endorser tx %d envelope: %w", index, err)
	}
	if _, err := safedecode.DijkstraBlockTransaction(txCbor); err != nil {
		return fmt.Errorf("decode endorser tx %d: %w", index, err)
	}
	return nil
}

func validateLeiosEndorserBlockTxStructures(
	txsRaw []cbor.RawMessage,
) error {
	for i, raw := range txsRaw {
		if err := validateLeiosEndorserBlockTxStructure(i, raw); err != nil {
			return err
		}
	}
	return nil
}

// leiosEndorserBlockTxCbor checks that raw is exactly one Dijkstra
// block_transaction, [body, witnesses, auxiliary_data / null, is_valid], and
// returns it. leios-fetch hands over the transaction already unwrapped from
// its tag-24 byte string, so any other envelope is rejected rather than
// guessed at: the manifest hash and size cover these exact bytes.
func leiosEndorserBlockTxCbor(raw cbor.RawMessage) ([]byte, error) {
	txCbor := []byte(raw)
	txElems, bytesRead, err := safedecode.Cbor[[]cbor.RawMessage](txCbor)
	if err != nil {
		return nil, fmt.Errorf("decode transaction envelope: %w", err)
	}
	if bytesRead != len(txCbor) {
		return nil, errors.New("endorser transaction has trailing envelope bytes")
	}
	if len(txElems) != 4 {
		return nil, fmt.Errorf(
			"endorser transaction has %d elements, expected 4",
			len(txElems),
		)
	}
	if len(txElems[3]) != 1 ||
		(txElems[3][0] != 0xf4 && txElems[3][0] != 0xf5) {
		return nil, errors.New("endorser transaction has no is_valid flag")
	}
	return txCbor, nil
}

func leiosEndorserBlockTxValidator(
	manifestRaw []byte,
	txCount int,
) (func(int, cbor.RawMessage) error, error) {
	block, err := decodeLeiosEndorserBlock(manifestRaw)
	if err != nil {
		return nil, fmt.Errorf("decode leios endorser block: %w", err)
	}
	if err := block.Validate(); err != nil {
		return nil, fmt.Errorf(
			"validate leios endorser block references: %w",
			err,
		)
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
	block, err := decodeLeiosEndorserBlock(blockRaw)
	if err != nil {
		return fmt.Errorf("decode leios endorser block: %w", err)
	}
	if len(txsRaw) == len(block.TransactionReferences) {
		if err := validateLeiosEndorserBlockTxStructures(txsRaw); err != nil {
			return fmt.Errorf("leios endorser block cache: %w", err)
		}
	}
	// Bind the entry to the slot its announcement actually vouched for. A
	// peer-offered point is only that connection's claim: the manifest is
	// content-addressed, so an authentic endorser block can be replayed under
	// any slot, and can be a live, independently
	// required occurrence at more than one slot at once. Mark the entry
	// verified only when a trusted source establishes this specific (slot,
	// hash) pair.
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
	verified := origin == leiosStoreAuthoritative ||
		origin == leiosStoreBackfill ||
		o.leiosAnnouncementBindsSlotLocked(point.Hash, point.Slot)
	cacheKeys := []string{leiosBlockKey(point.Slot, point.Hash)}
	relayOfferRequested := origin == leiosStorePeerOffered ||
		origin == leiosStoreBackfill
	data := &leiosEndorserBlockData{
		point:                    point,
		blockRaw:                 slices.Clone(blockRaw),
		txsRaw:                   cloneRawMessages(txsRaw),
		announcementHeaderRaw:    o.leiosAnnouncementHeaderLocked(point.Hash, point.Slot),
		txCount:                  len(block.TransactionReferences),
		cacheKeys:                cacheKeys,
		insertedAt:               time.Now(),
		relayOfferRequested:      relayOfferRequested,
		slotVerified:             verified,
		semanticValidationStatus: leiosEBValidationUnknown,
	}
	o.leiosMu.Lock()
	if !data.slotVerified {
		if selectedHeader := o.selectedLeiosHeaderForDataLocked(data); len(selectedHeader) > 0 {
			data.slotVerified = true
			data.announcementHeaderRaw = selectedHeader
		}
	}
	if o.leiosEndorserBlocks == nil {
		o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	}
	o.pruneLeiosEndorserBlockCacheLocked(time.Now())
	o.leiosEndorserBlockSeq++
	data.seq = o.leiosEndorserBlockSeq
	// The cache key is (slot, hash), so an existing entry found here is
	// necessarily the same occurrence -- a peer-offered or authoritative
	// store for a *different* slot of the same hash lands under its own,
	// distinct key instead of colliding with (or needing to evict) this one.
	// Every remaining branch below is about the same
	// occurrence's own transaction/verification state, not a slot conflict.
	existing := o.leiosEndorserBlocks[cacheKeys[0]]
	// Whether the closure was already complete and slot-verified before this
	// store, so the arrival is recorded once, at the transition.
	wasClosureReady := existing != nil && existing.completeTxCache() &&
		existing.slotVerified
	if existing != nil {
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
		data.relayOfferRequested = data.relayOfferRequested ||
			existing.relayOfferRequested
		data.relayOfferSource = existing.relayOfferSource
		data.relayManifestOffered = existing.relayManifestOffered
		data.relayTransactionsOffered = existing.relayTransactionsOffered
		data.semanticValidationStatus = existing.semanticValidationStatus
		data.semanticValidationParent = slices.Clone(
			existing.semanticValidationParent,
		)
		data.semanticValidationDone = existing.semanticValidationDone
		if len(data.announcementHeaderRaw) == 0 {
			data.announcementHeaderRaw = slices.Clone(
				existing.announcementHeaderRaw,
			)
		}
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
		// A binding already established for this occurrence survives a later
		// peer-offered store (same key, so same slot): an unverified store
		// cannot demote an entry a trusted source already bound.
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
	// set is complete and slot-verified. Both are the same readiness predicate
	// the resolver (and leiosClosureCompleteLocked) use, so a waiter is only
	// woken when a subsequent merge would actually succeed; an unverified
	// completion is left unsignaled and is instead woken by
	// bindLeiosEndorserBlockSlot once the slot is corroborated.
	if data.completeTxCache() && data.slotVerified {
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
		o.publishLeiosEndorserBlock(point, blockRaw, data)
	}
	// A block this node forged itself did not arrive from anywhere.
	if origin != leiosStoreAuthoritative && !wasClosureReady &&
		data.completeTxCache() && data.slotVerified {
		o.recordLeiosEbArrival(point, data.txCount)
	}
	return nil
}

// publishLeiosEndorserBlock performs the side effects keyed on a verified
// endorser-block slot. It must be called with leiosMu released.
func (o *Ouroboros) publishLeiosEndorserBlock(
	point ocommon.Point,
	blockRaw []byte,
	data *leiosEndorserBlockData,
) {
	if data == nil || !data.slotVerified {
		return
	}
	o.enqueueLeiosRelayOffers(point, data)
	// Persist verified content and advance the advisory slot watermark even
	// when it is still manifest-only. Voting and pipeline admission remain
	// separately gated on complete transaction bodies and semantic validation.
	o.advanceLeiosVerifiedEbSlot(point.Slot)
	o.enqueueLeiosPersist(point, blockRaw, data)
	o.scheduleLeiosEndorserBlockValidation(point, data)
}

// enqueueLeiosRelayOffers forwards a peer-offered endorser block after its
// announced slot has been verified. The manifest can be forwarded before the
// transaction fetch completes; the transaction offer waits until every body
// has been verified against the manifest.
func (o *Ouroboros) enqueueLeiosRelayOffers(
	point ocommon.Point,
	data *leiosEndorserBlockData,
) {
	if data == nil || !data.relayOfferRequested || !data.slotVerified {
		return
	}
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosMu.Lock()
	current := o.leiosEndorserBlocks[key]
	if current == nil || !current.relayOfferRequested || !current.slotVerified {
		o.leiosMu.Unlock()
		return
	}
	updated := *current
	// An intent-only entry has no manifest to size the offer with. Receivers
	// reject a zero-size offer by closing the connection, so defer until the
	// manifest is stored (which re-enters here) and only then mark it offered.
	manifestOffer := !updated.relayManifestOffered && updated.hasManifest()
	if manifestOffer {
		updated.relayManifestOffered = true
	}
	transactionsOffer := updated.txCount > 0 && updated.completeTxCache() &&
		!updated.relayTransactionsOffered
	if transactionsOffer {
		updated.relayTransactionsOffered = true
	}
	if manifestOffer || transactionsOffer {
		for _, cacheKey := range current.cacheKeys {
			if o.leiosEndorserBlocks[cacheKey] == current {
				o.leiosEndorserBlocks[cacheKey] = &updated
			}
		}
	}
	if manifestOffer {
		forwardedPoint := ocommon.Point{
			Slot: point.Slot,
			Hash: slices.Clone(point.Hash),
		}
		o.leiosEBLog.append(leiosForgedEBEntry{
			point: &forwardedPoint,
			size:  uint64(len(current.blockRaw)),
		})
	}
	if transactionsOffer {
		forwardedPoint := ocommon.Point{
			Slot: point.Slot,
			Hash: slices.Clone(point.Hash),
		}
		o.leiosEBLog.append(leiosForgedEBEntry{txOffer: &forwardedPoint})
	}
	o.leiosMu.Unlock()
}

// markLeiosEndorserBlockRelayOffer records that a peer offered an occurrence.
// An intent-only entry preserves an offer that arrives before the manifest;
// it shares the cache's entry and TTL bounds and is merged by a later store.
func (o *Ouroboros) markLeiosEndorserBlockRelayOffer(
	point ocommon.Point,
) {
	o.markLeiosEndorserBlockRelayOfferFrom(point, "")
}

func (o *Ouroboros) markLeiosEndorserBlockRelayOfferFrom(
	point ocommon.Point,
	connectionID string,
) {
	if len(point.Hash) != lcommon.Blake2b256Size {
		return
	}
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosMu.Lock()
	if o.leiosEndorserBlocks == nil {
		o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	}
	now := time.Now()
	o.pruneLeiosEndorserBlockCacheLocked(now)
	current := o.leiosEndorserBlocks[key]
	if current == nil {
		o.leiosEndorserBlockSeq++
		o.leiosEndorserBlocks[key] = &leiosEndorserBlockData{
			point: ocommon.Point{
				Slot: point.Slot,
				Hash: slices.Clone(point.Hash),
			},
			cacheKeys:           []string{key},
			insertedAt:          now,
			relayOfferRequested: true,
			relayOfferSource:    connectionID,
			seq:                 o.leiosEndorserBlockSeq,
		}
		o.pruneLeiosEndorserBlockCacheLocked(now)
		o.leiosMu.Unlock()
		return
	}
	updated := *current
	updated.relayOfferRequested = true
	if connectionID != "" {
		updated.relayOfferSource = connectionID
	}
	for _, cacheKey := range current.cacheKeys {
		if o.leiosEndorserBlocks[cacheKey] == current {
			o.leiosEndorserBlocks[cacheKey] = &updated
		}
	}
	o.leiosMu.Unlock()
	o.enqueueLeiosRelayOffers(point, &updated)
}

func (o *Ouroboros) scheduleLeiosEndorserBlockValidation(
	point ocommon.Point,
	data *leiosEndorserBlockData,
) {
	if data == nil || !data.slotVerified {
		return
	}
	blockHash := lcommon.Blake2b256Hash(data.blockRaw)
	if !o.config.EnableLeios {
		o.publishValidatedLeiosEndorserBlock(
			point.Slot,
			lcommon.Blake2b256{},
			blockHash,
		)
		return
	}
	if !data.completeTxCache() ||
		isNilInterface(o.leiosAnnouncementLedger) ||
		len(data.announcementHeaderRaw) == 0 {
		return
	}
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosValidationMu.Lock()
	if o.leiosValidationClosed || o.leiosValidationSlots == nil {
		o.leiosValidationMu.Unlock()
		return
	}
	select {
	case o.leiosValidationSlots <- struct{}{}:
	default:
		o.leiosValidationMu.Unlock()
		return
	}
	o.leiosMu.Lock()
	current := o.leiosEndorserBlocks[key]
	if current == nil || !current.completeTxCache() || !current.slotVerified ||
		len(current.announcementHeaderRaw) == 0 ||
		current.semanticValidationStatus != leiosEBValidationUnknown {
		o.leiosMu.Unlock()
		<-o.leiosValidationSlots
		o.leiosValidationMu.Unlock()
		return
	}
	header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(
		current.announcementHeaderRaw,
	)
	if err != nil {
		o.leiosMu.Unlock()
		<-o.leiosValidationSlots
		o.leiosValidationMu.Unlock()
		return
	}
	basis := ledger.LeiosEndorserBlockBasisHash(header)
	pending := *current
	pending.semanticValidationStatus = leiosEBValidationPending
	pending.semanticValidationParent = slices.Clone(basis)
	pending.semanticValidationDone = make(chan struct{})
	for _, cacheKey := range current.cacheKeys {
		if o.leiosEndorserBlocks[cacheKey] == current {
			o.leiosEndorserBlocks[cacheKey] = &pending
		}
	}
	o.leiosValidationWG.Add(1)
	o.leiosMu.Unlock()
	o.leiosValidationMu.Unlock()

	go o.validateLeiosEndorserBlock(key, pending)
}

// ValidateLeiosEndorserBlockForHeader waits through the configured diffusion
// window for the exact endorser block announced by header, then validates it
// synchronously while basis is still the applied ledger tip. The ledger's
// apply boundary passes ledger.LeiosEndorserBlockBasisHash, once the
// announcing block is the tip and before the block that may certify it
// applies. A forger checking its own announcement before adoption passes
// ledger.LeiosValidationParentHash (see
// ledger.LedgerState.ValidateLeiosEndorserBlockTransactions). Endorser-block
// bodies arrive independently on asynchronous Leios fetch workers.
func (o *Ouroboros) ValidateLeiosEndorserBlockForHeader(
	ctx context.Context,
	header lcommon.BlockHeader,
	basis []byte,
) error {
	if header == nil {
		return errors.New("nil Leios announcing header")
	}
	announcer, ok := header.(interface {
		LeiosAnnouncement() (lcommon.Blake2b256, uint64, bool)
	})
	if !ok {
		return nil
	}
	ebHash, _, announced := announcer.LeiosAnnouncement()
	if !announced {
		return nil
	}
	if isNilInterface(o.leiosAnnouncementLedger) {
		return errors.New("leios announcement ledger unavailable")
	}
	if o.ledgerState != nil && !slices.Equal(
		o.ledgerState.Tip().Point.Hash,
		basis,
	) {
		return ledger.ErrLeiosValidationParentUnavailable
	}
	releaseAuthority, err := o.retainSelectedLeiosHeader(header)
	if err != nil {
		return err
	}
	defer releaseAuthority()
	if data, ok := o.lookupLeiosEndorserBlock(
		header.SlotNumber(),
		ebHash.Bytes(),
	); ok && data != nil {
		if err := validateLeiosEndorserBlockBinding(header, data); err != nil {
			return err
		}
	}
	//nolint:contextcheck // Persistence GC is node-owned and must outlive header validation.
	if publish := o.bindLeiosEndorserBlockSlot(
		ebHash.Bytes(),
		header.SlotNumber(),
	); publish != nil {
		publish()
	}
	waitCtx := ctx
	cancel := func() {}
	timeout := o.config.LeiosTxFetchTailBudget
	if timeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	if timeout <= 0 {
		key := leiosBlockKey(header.SlotNumber(), ebHash.Bytes())
		o.leiosMu.RLock()
		complete := o.leiosClosureCompleteLocked(key)
		o.leiosMu.RUnlock()
		if !complete {
			return errors.New(
				"leios endorser closure unavailable with tail retry disabled",
			)
		}
	} else if !o.waitForLeiosEndorserClosure(
		waitCtx,
		header.SlotNumber(),
		ebHash.Bytes(),
	) {
		if err := waitCtx.Err(); err != nil {
			return fmt.Errorf("wait for Leios endorser closure: %w", err)
		}
		return errors.New("leios endorser closure unavailable")
	}
	key := leiosBlockKey(header.SlotNumber(), ebHash.Bytes())
	data, ok := o.lookupLeiosEndorserBlock(
		header.SlotNumber(),
		ebHash.Bytes(),
	)
	if !ok || data == nil || !data.completeTxCache() || !data.slotVerified {
		return errors.New("leios endorser closure unavailable after wakeup")
	}
	if err := validateLeiosEndorserBlockBinding(header, data); err != nil {
		return err
	}
	validationParent := basis
	var pending leiosEndorserBlockData
	for {
		o.leiosMu.Lock()
		current := o.leiosEndorserBlocks[key]
		if current == nil {
			o.leiosMu.Unlock()
			return errors.New("leios endorser closure changed before validation")
		}
		sameParent := slices.Equal(
			current.semanticValidationParent,
			validationParent,
		)
		switch {
		case current.semanticValidationStatus == leiosEBValidationValid &&
			sameParent:
			o.leiosMu.Unlock()
			return nil
		case current.semanticValidationStatus == leiosEBValidationInvalid &&
			sameParent:
			o.leiosMu.Unlock()
			return fmt.Errorf(
				"%w: leios endorser closure failed semantic validation",
				ledger.ErrInvalidLeiosEndorserBlockTransactions,
			)
		case current.semanticValidationStatus == leiosEBValidationPending &&
			sameParent:
			done := current.semanticValidationDone
			o.leiosMu.Unlock()
			select {
			case <-done:
				continue
			case <-waitCtx.Done():
				return fmt.Errorf(
					"wait for Leios endorser validation: %w",
					waitCtx.Err(),
				)
			}
		}
		pending = *current
		pending.semanticValidationStatus = leiosEBValidationPending
		pending.semanticValidationParent = slices.Clone(validationParent)
		pending.semanticValidationDone = make(chan struct{})
		for _, cacheKey := range current.cacheKeys {
			if o.leiosEndorserBlocks[cacheKey] == current {
				o.leiosEndorserBlocks[cacheKey] = &pending
			}
		}
		o.leiosMu.Unlock()
		break
	}

	txCbors := make([][]byte, len(pending.txsRaw))
	err = nil
	for i, raw := range pending.txsRaw {
		txCbors[i], err = leiosEndorserBlockTxCbor(raw)
		if err != nil {
			err = fmt.Errorf(
				"%w: decode endorser transaction %d: %w",
				ledger.ErrInvalidLeiosEndorserBlockTransactions,
				i,
				err,
			)
			break
		}
	}
	if err == nil {
		err = o.leiosAnnouncementLedger.ValidateLeiosEndorserBlockTransactions(
			ctx,
			header,
			validationParent,
			txCbors,
		)
	}
	status := leiosEndorserBlockValidationStatus(err)
	validated := o.updateLeiosEndorserBlockValidation(
		key,
		validationParent,
		pending.semanticValidationDone,
		status,
	)
	if err != nil {
		return err
	}
	if validated == nil {
		return errors.New("leios endorser closure changed during validation")
	}
	o.publishValidatedLeiosEndorserBlock(
		validated.point.Slot,
		header.Hash(),
		lcommon.Blake2b256Hash(validated.blockRaw),
	)
	return nil
}

func (o *Ouroboros) retainSelectedLeiosHeader(
	header lcommon.BlockHeader,
) (func(), error) {
	announcer := header.(interface {
		LeiosAnnouncement() (lcommon.Blake2b256, uint64, bool)
	})
	ebHash, _, announced := announcer.LeiosAnnouncement()
	if !announced || len(header.Cbor()) == 0 {
		return nil, errors.New("selected Leios header has no preserved CBOR")
	}
	key := leiosBlockKey(header.SlotNumber(), ebHash.Bytes())
	parentKey := string(ledger.LeiosValidationParentHash(header))
	raw := slices.Clone(header.Cbor())
	rawKey := string(raw)
	o.leiosMu.Lock()
	if o.leiosSelectedHeaders == nil {
		o.leiosSelectedHeaders = make(
			map[string]map[string]map[string]selectedLeiosHeader,
		)
	}
	byParent := o.leiosSelectedHeaders[key]
	if byParent == nil {
		byParent = make(map[string]map[string]selectedLeiosHeader)
		o.leiosSelectedHeaders[key] = byParent
	}
	byRaw := byParent[parentKey]
	if byRaw == nil {
		byRaw = make(map[string]selectedLeiosHeader)
		byParent[parentKey] = byRaw
	}
	selected := byRaw[rawKey]
	selected.raw = raw
	selected.refs++
	byRaw[rawKey] = selected
	o.leiosMu.Unlock()
	return func() {
		o.leiosMu.Lock()
		if current := o.leiosSelectedHeaders[key]; current != nil {
			currentByRaw := current[parentKey]
			selected := currentByRaw[rawKey]
			if selected.refs > 1 {
				selected.refs--
				currentByRaw[rawKey] = selected
			} else {
				delete(currentByRaw, rawKey)
				if len(currentByRaw) == 0 {
					delete(current, parentKey)
				}
			}
			if len(current) == 0 {
				delete(o.leiosSelectedHeaders, key)
			}
		}
		o.leiosMu.Unlock()
	}, nil
}

func (o *Ouroboros) selectedLeiosHeaderForDataLocked(
	data *leiosEndorserBlockData,
) []byte {
	for _, byRaw := range o.leiosSelectedHeaders[leiosBlockKey(
		data.point.Slot,
		data.point.Hash,
	)] {
		for _, selected := range byRaw {
			header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(selected.raw)
			if err == nil && validateLeiosEndorserBlockBinding(header, data) == nil {
				return slices.Clone(selected.raw)
			}
		}
	}
	return nil
}

func (o *Ouroboros) validateLeiosEndorserBlock(
	key string,
	data leiosEndorserBlockData,
) {
	defer o.leiosValidationWG.Done()
	defer func() { <-o.leiosValidationSlots }()
	header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(
		data.announcementHeaderRaw,
	)
	if err != nil {
		err = fmt.Errorf(
			"%w: decode Leios announcing header: %w",
			ledger.ErrInvalidLeiosEndorserBlockTransactions,
			err,
		)
	} else if header == nil {
		err = fmt.Errorf(
			"%w: decoded Leios announcing header is nil",
			ledger.ErrInvalidLeiosEndorserBlockTransactions,
		)
	}
	if err == nil {
		err = validateLeiosEndorserBlockBinding(header, &data)
	}
	var txCbors [][]byte
	if err == nil {
		txCbors = make([][]byte, len(data.txsRaw))
		for i, raw := range data.txsRaw {
			txCbors[i], err = leiosEndorserBlockTxCbor(raw)
			if err != nil {
				err = fmt.Errorf(
					"%w: decode endorser transaction %d: %w",
					ledger.ErrInvalidLeiosEndorserBlockTransactions,
					i,
					err,
				)
				break
			}
		}
	}
	if err == nil {
		err = o.leiosAnnouncementLedger.ValidateLeiosEndorserBlockTransactions(
			o.leiosValidationCtx,
			header,
			data.semanticValidationParent,
			txCbors,
		)
	}
	if o.leiosValidationCtx.Err() != nil {
		err = o.leiosValidationCtx.Err()
	}
	status := leiosEBValidationUnknown
	switch {
	case err == nil:
		status = leiosEBValidationValid
	case errors.Is(err, ledger.ErrInvalidLeiosEndorserBlockTransactions):
		status = leiosEBValidationInvalid
		o.config.Logger.Debug(
			"rejecting semantically invalid Leios endorser block",
			"slot", data.point.Slot,
			"hash", hex.EncodeToString(data.point.Hash),
			"error", err,
		)
	case errors.Is(err, ledger.ErrLeiosValidationParentUnavailable),
		errors.Is(err, ledger.ErrLeiosValidationParentSuperseded),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		status = leiosEBValidationUnknown
	default:
		o.config.Logger.Debug(
			"deferring Leios endorser block validation after local failure",
			"slot", data.point.Slot,
			"hash", hex.EncodeToString(data.point.Hash),
			"error", err,
		)
	}
	validated := o.updateLeiosEndorserBlockValidation(
		key,
		data.semanticValidationParent,
		data.semanticValidationDone,
		status,
	)
	if status == leiosEBValidationValid && validated != nil {
		o.publishValidatedLeiosEndorserBlock(
			validated.point.Slot,
			header.Hash(),
			lcommon.Blake2b256Hash(validated.blockRaw),
		)
	}
}

func validateLeiosEndorserBlockBinding(
	header lcommon.BlockHeader,
	data *leiosEndorserBlockData,
) error {
	dijkstraHeader, ok := header.(*gdijkstra.DijkstraBlockHeader)
	if !ok {
		return fmt.Errorf(
			"%w: header has no Leios announcement",
			ledger.ErrInvalidLeiosEndorserBlockTransactions,
		)
	}
	if dijkstraHeader == nil {
		return fmt.Errorf(
			"%w: Leios announcing header is nil",
			ledger.ErrInvalidLeiosEndorserBlockTransactions,
		)
	}
	announcedHash, announcedSize, announced := dijkstraHeader.LeiosAnnouncement()
	if !announced || dijkstraHeader.SlotNumber() != data.point.Slot ||
		!slices.Equal(announcedHash.Bytes(), data.point.Hash) {
		return fmt.Errorf(
			"%w: leios announcing header does not bind the cached endorser block",
			ledger.ErrInvalidLeiosEndorserBlockTransactions,
		)
	}
	if uint64(len(data.blockRaw)) != announcedSize {
		return fmt.Errorf(
			"%w: Leios endorser block size %d does not match announcement size %d",
			ledger.ErrInvalidLeiosEndorserBlockTransactions,
			len(data.blockRaw),
			announcedSize,
		)
	}
	return nil
}

func leiosEndorserBlockValidationStatus(err error) uint8 {
	switch {
	case err == nil:
		return leiosEBValidationValid
	case errors.Is(err, ledger.ErrLedgerViewStorageFault):
		return leiosEBValidationUnknown
	case errors.Is(err, ledger.ErrInvalidLeiosEndorserBlockTransactions):
		return leiosEBValidationInvalid
	default:
		return leiosEBValidationUnknown
	}
}

func (o *Ouroboros) updateLeiosEndorserBlockValidation(
	key string,
	parentHash []byte,
	done chan struct{},
	status uint8,
) *leiosEndorserBlockData {
	o.leiosMu.Lock()
	current := o.leiosEndorserBlocks[key]
	if current == nil ||
		current.semanticValidationStatus != leiosEBValidationPending ||
		!slices.Equal(current.semanticValidationParent, parentHash) ||
		current.semanticValidationDone != done {
		o.leiosMu.Unlock()
		close(done)
		return nil
	}
	updated := *current
	updated.semanticValidationStatus = status
	updated.semanticValidationDone = nil
	for _, cacheKey := range current.cacheKeys {
		if o.leiosEndorserBlocks[cacheKey] == current {
			o.leiosEndorserBlocks[cacheKey] = &updated
		}
	}
	o.leiosMu.Unlock()
	close(done)
	return &updated
}

func (o *Ouroboros) retryLeiosEndorserBlockValidations() {
	blocks := o.retryableLeiosEndorserBlocks()
	for _, data := range blocks {
		o.scheduleLeiosEndorserBlockValidation(data.point, data)
	}
}

func (o *Ouroboros) retryableLeiosEndorserBlocks() []*leiosEndorserBlockData {
	o.leiosMu.RLock()
	blocks := make([]*leiosEndorserBlockData, 0, len(o.leiosEndorserBlocks))
	seen := make(map[*leiosEndorserBlockData]struct{}, len(o.leiosEndorserBlocks))
	for _, data := range o.leiosEndorserBlocks {
		if _, ok := seen[data]; ok {
			continue
		}
		seen[data] = struct{}{}
		if data != nil && data.semanticValidationStatus == leiosEBValidationUnknown &&
			data.slotVerified && data.completeTxCache() &&
			len(data.announcementHeaderRaw) > 0 {
			blocks = append(blocks, data)
		}
	}
	o.leiosMu.RUnlock()
	slices.SortFunc(blocks, func(a, b *leiosEndorserBlockData) int {
		if order := cmp.Compare(b.point.Slot, a.point.Slot); order != 0 {
			return order
		}
		return slices.Compare(a.point.Hash, b.point.Hash)
	})
	return blocks
}

func (o *Ouroboros) publishValidatedLeiosEndorserBlock(
	slot uint64,
	rbHash lcommon.Blake2b256,
	blockHash lcommon.Blake2b256,
) {
	// Trigger local vote emission for the stored block, outside the
	// cache lock
	if o.leiosVotes != nil && rbHash != (lcommon.Blake2b256{}) {
		o.leiosVotes.HandleValidatedEndorserBlock(slot, rbHash, blockHash)
	}
	// Register the block into the Leios pipeline for stage/timing
	// tracking and EB equivocation detection
	if o.leiosPipeline != nil {
		o.leiosPipeline.ObserveEndorserBlock(slot, blockHash)
	}
}

// MaxVerifiedEndorserBlockSlot returns the highest slot for which this node
// has corroborated an endorser block, or 0 if none. Because an endorser block
// shares its announcing ranking block's slot, this is proof that a ranking
// block exists at that slot.
//
// It is a monotonic lower bound on chain progress, never a tip: it is not
// rolled back on a fork, because the question it answers -- "is there a block
// out there at least this recent?" -- stays true regardless of which fork
// wins. Callers must treat it as advisory and must not use it as a parent.
//
// LIFECYCLE. The watermark has no lifecycle beyond that monotonic advance, and
// the two ends fail in opposite directions, so a consumer must handle both:
//
//   - It is raised at startup, to the maximum slot over authoritative
//     persisted manifests (restoreLeiosVerifiedEbSlot, from newOuroboros). If
//     that read fails the watermark stays cold, which fails OPEN: it
//     under-reports chain progress, so a consumer that uses it as evidence
//     simply sees no evidence. Reloading an evicted manifest from the blob
//     store does not raise it, and cannot need to: the startup restore
//     already takes the maximum over every persisted manifest, and every
//     manifest persisted after that went through publishLeiosEndorserBlock,
//     which advances the watermark first -- so within a process the watermark
//     is always at least the maximum persisted slot.
//
//   - It is NEVER lowered. An endorser block corroborated for a chain this
//     node does not adopt leaves the watermark above the local tip
//     permanently. Consumed as a hard comparison against a local tip, that
//     fails CLOSED and stays closed for as long as the local chain sits below
//     that slot -- while every local indicator reads healthy.
//
// Consequently a consumer must either treat the value as purely advisory
// (logging, metrics, hints) or bound it with its own explicitly configured
// tolerance. It must not be turned into an unconditional gate.
//
// The one gating consumer, the forge staleness check in ledger/forging, does
// the latter: it is opt-in behind ForgeEndorserBlockStalenessSlots, which
// defaults to 0 (disabled), so the refusal path does not exist unless an
// operator sets a bound, and the bound is its own knob rather than the local
// primary-chain-tip tolerance.
func (o *Ouroboros) MaxVerifiedEndorserBlockSlot() uint64 {
	if o == nil {
		return 0
	}
	return o.leiosMaxVerifiedEbSlot.Load()
}

func (o *Ouroboros) advanceLeiosVerifiedEbSlot(slot uint64) {
	for {
		previous := o.leiosMaxVerifiedEbSlot.Load()
		if slot <= previous ||
			o.leiosMaxVerifiedEbSlot.CompareAndSwap(previous, slot) {
			return
		}
	}
}

// restoreLeiosVerifiedEbSlot restores the monotonic Leios evidence watermark
// from authoritative persisted manifests during startup, so the optional
// forge staleness gate is still effective after a restart rather than reading
// 0 until the next verification. It is the only path that raises the
// watermark from persisted state; a cache eviction needs none, because the
// watermark within a process is always at least the maximum persisted slot
// (see MaxVerifiedEndorserBlockSlot).
func (o *Ouroboros) restoreLeiosVerifiedEbSlot() {
	if !o.config.EnableLeios || o.ledgerState == nil {
		return
	}
	slot, err := o.ledgerState.Database().MaxLeiosEBSlot()
	if err != nil {
		o.config.Logger.Warn(
			"failed to restore persisted leios EB watermark",
			"error", err,
		)
		o.startLeiosPersistenceGC(0, false)
		return
	}
	o.advanceLeiosVerifiedEbSlot(slot)
	o.startLeiosPersistenceGC(slot, true)
}

// bindLeiosEndorserBlockSlot reconciles a cached (slot, hash) occurrence
// against an authoritative source vouching for that same slot -- a validated
// ranking-block announcement, or the point a ranking block the ledger is
// applying references. An entry cached before that authority arrived carries
// the offering connection's unverified claim, so it is promoted here before
// anything keyed on the slot is published.
//
// It returns a closure that performs the publication (vote emission, pipeline
// observation, blob persistence enqueue) when promotion happened, or nil when
// there is nothing to publish (already verified, superseded, or no matching
// cache entry for this exact occurrence). The publish step is returned rather
// than run inline so a
// caller that reconciles under an outer lock -- recordLeiosAnnouncement holds
// leiosAnnouncementsMu across this call for the atomicity reasons on that
// function -- can run it only after releasing that lock: publishing invokes
// external vote/pipeline/persistence handlers, and running those under a
// mutex shared by every concurrent announcement would stall them all for the
// duration of a slow handler, or deadlock one that re-enters announcement
// recording. Callers with nothing else held (the backfill
// paths in leios_backfill.go) can just call the closure immediately.
func (o *Ouroboros) bindLeiosEndorserBlockSlot(
	ebHash []byte,
	slot uint64,
	announcementHeaderRaw ...[]byte,
) func() {
	// lookupLeiosEndorserBlock, not a direct map read: an entry may exist
	// only in the blob store (evicted from memory by TTL, or never loaded
	// this run). A result found there is necessarily this exact occurrence.
	// When it is absent, the authoritative binding below may reuse complete
	// content cached for the same hash while keeping the occurrences separate.
	key := leiosBlockKey(slot, ebHash)
	_, _ = o.lookupLeiosEndorserBlock(slot, ebHash)
	o.leiosMu.Lock()
	now := time.Now()
	o.pruneLeiosEndorserBlockCacheLocked(now)
	data := o.leiosEndorserBlocks[key]
	wasClosureReady := data != nil && data.completeTxCache() &&
		data.slotVerified
	contentChanged := false
	if data == nil || !data.completeTxCache() {
		// The manifest is content-addressed, so a complete cached occurrence
		// supplies the same bytes and transaction bodies. The announcement is
		// the authority for the new slot. Keep any exact-slot intent and trust
		// state while filling its content from the other occurrence.
		for _, candidate := range o.leiosEndorserBlocks {
			if candidate == nil || candidate.expired(now) ||
				!candidate.completeTxCache() ||
				!slices.Equal(candidate.point.Hash, ebHash) {
				continue
			}
			if data == nil {
				o.leiosEndorserBlockSeq++
				data = &leiosEndorserBlockData{
					point: ocommon.Point{
						Slot: slot,
						Hash: slices.Clone(ebHash),
					},
					cacheKeys:  []string{key},
					insertedAt: now,
					seq:        o.leiosEndorserBlockSeq,
				}
			}
			completed := *data
			completed.blockRaw = slices.Clone(candidate.blockRaw)
			completed.txsRaw = cloneRawMessages(candidate.txsRaw)
			completed.partialTxs = nil
			completed.txCount = candidate.txCount
			completed.semanticValidationStatus = leiosEBValidationUnknown
			for _, cacheKey := range data.cacheKeys {
				if current := o.leiosEndorserBlocks[cacheKey]; current == data ||
					current == nil {
					o.leiosEndorserBlocks[cacheKey] = &completed
				}
			}
			data = &completed
			contentChanged = true
			break
		}
	}
	if data == nil {
		o.leiosMu.Unlock()
		return nil
	}
	// Cached entries are replaced, never mutated in place: lookupLeiosEndorserBlock
	// hands out this pointer and readers use it without leiosMu held, so
	// flipping slotVerified here directly would race a concurrent unlocked
	// read of the same field. Publish a copy instead, the same pattern
	// retainLeiosPartialTxs uses.
	verified := *data
	changed := contentChanged
	if !verified.slotVerified {
		verified.slotVerified = true
		changed = true
	}
	if len(verified.announcementHeaderRaw) == 0 &&
		len(announcementHeaderRaw) > 0 && len(announcementHeaderRaw[0]) > 0 {
		verified.announcementHeaderRaw = slices.Clone(announcementHeaderRaw[0])
		changed = true
	}
	if !changed {
		o.leiosMu.Unlock()
		return nil
	}
	for _, key := range data.cacheKeys {
		if o.leiosEndorserBlocks[key] == data {
			o.leiosEndorserBlocks[key] = &verified
		}
	}
	// A closure that was already complete when it was stored was left
	// unsignaled (storeLeiosEndorserBlock withholds the wakeup until
	// slotVerified) -- signal it now that this authority has corroborated the
	// slot, or a waiter parked on it would otherwise sit until its wait
	// window times out instead of waking on the closure it is already
	// holding.
	if verified.completeTxCache() && verified.slotVerified {
		for _, key := range data.cacheKeys {
			o.signalLeiosClosureWaitersLocked(key)
		}
	}
	o.pruneLeiosEndorserBlockCacheLocked(time.Now())
	o.leiosMu.Unlock()
	return func() {
		o.publishLeiosEndorserBlock(
			verified.point,
			verified.blockRaw,
			&verified,
		)
		// Recorded here, not above: the caller may hold
		// leiosAnnouncementsMu until it runs this closure, and recording
		// resolves the slot through the ledger.
		if !wasClosureReady && verified.completeTxCache() &&
			verified.slotVerified {
			o.recordLeiosEbArrival(verified.point, verified.txCount)
		}
	}
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
	return data != nil && data.hasManifest() && len(data.txsRaw) == data.txCount
}

func (data *leiosEndorserBlockData) hasManifest() bool {
	return data != nil && len(data.blockRaw) > 0
}

const rawMessageHeaderBytes = int(unsafe.Sizeof(cbor.RawMessage{}))

// approxBytes estimates the memory retained by one cache entry. It charges
// backing-array capacity for the manifest, transaction bodies, and the outer
// transaction slices so sparse partial fetches consume their actual cache
// budget even when most entries are nil. It does not include the fixed entry
// struct or small bookkeeping strings.
func (data *leiosEndorserBlockData) approxBytes() int {
	if data == nil {
		return 0
	}
	return cap(data.blockRaw) + rawMessagesRetainedBytes(data.txsRaw) +
		rawMessagesRetainedBytes(data.partialTxs)
}

func rawMessagesRetainedBytes(messages []cbor.RawMessage) int {
	total := cap(messages) * rawMessageHeaderBytes
	for _, raw := range messages {
		total += cap(raw)
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
	slot uint64,
	hash []byte,
	result []cbor.RawMessage,
	validate func(int, cbor.RawMessage) error,
) {
	if len(result) == 0 {
		return
	}
	o.leiosMu.RLock()
	defer o.leiosMu.RUnlock()
	data, ok := o.leiosEndorserBlocks[leiosBlockKey(slot, hash)]
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
	slot uint64,
	hash []byte,
	partial []cbor.RawMessage,
	validate func(int, cbor.RawMessage) error,
) {
	if len(partial) == 0 {
		return
	}
	key := leiosBlockKey(slot, hash)
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
	// Evict intent-only entries first, then peer-fetched manifests whose slots
	// have not been corroborated. Unauthenticated network input must not
	// displace content bound to an authoritative slot from the shared cache.
	// Within each class, sort by seq rather than insertedAt:
	// insertedAt is a wall-clock timestamp captured before leiosMu is acquired,
	// so a delayed goroutine can carry an earlier timestamp into the map after a
	// goroutine that actually won the lock and inserted first. seq is assigned
	// while the lock is held, so it reflects true insertion order.
	slices.SortFunc(blocks, func(a, b *leiosEndorserBlockData) int {
		if a.hasManifest() != b.hasManifest() {
			if a.hasManifest() {
				return 1
			}
			return -1
		}
		if a.slotVerified != b.slotVerified {
			if a.slotVerified {
				return 1
			}
			return -1
		}
		return cmp.Compare(a.seq, b.seq)
	})
	// Evict in the priority order above until both the entry-count and aggregate
	// byte budget are satisfied. totalBytes is updated as each victim is chosen
	// instead of being recomputed from scratch.
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
	slot uint64,
	hash []byte,
) (*leiosEndorserBlockData, bool) {
	key := leiosBlockKey(slot, hash)
	now := time.Now()
	o.leiosMu.RLock()
	data, ok := o.leiosEndorserBlocks[key]
	if !ok || data == nil {
		o.leiosMu.RUnlock()
		// Memory cache miss: try the persistent blob store so we can serve
		// historical EBs whose in-memory TTL has elapsed.
		return o.loadLeiosEBFromDB(slot, hash)
	}
	if !data.expired(now) {
		o.leiosMu.RUnlock()
		if !data.hasManifest() {
			return o.loadLeiosEBFromDB(slot, hash)
		}
		return data, true
	}
	o.leiosMu.RUnlock()

	o.leiosMu.Lock()
	data, ok = o.leiosEndorserBlocks[key]
	if !ok || data == nil {
		o.leiosMu.Unlock()
		return o.loadLeiosEBFromDB(slot, hash)
	}
	if data.expired(now) {
		o.deleteLeiosEndorserBlockDataLocked(data)
		o.leiosMu.Unlock()
		return o.loadLeiosEBFromDB(slot, hash)
	}
	o.leiosMu.Unlock()
	if !data.hasManifest() {
		return o.loadLeiosEBFromDB(slot, hash)
	}
	return data, true
}

// loadLeiosEBFromDB loads a Leios endorser block's manifest (and txs, if
// stored) from the persistent blob store for the exact (slot, hash)
// occurrence named, and caches the result in memory under the same key. The
// blob store is itself keyed by (slot, hash) (`types.LeiosEBManifestKey`),
// so a persisted occurrence for a different slot of the same hash is a
// distinct blob key and is never returned here.
// Returns (nil, false) when the blob store has no manifest for this exact
// occurrence.
func (o *Ouroboros) loadLeiosEBFromDB(
	slot uint64,
	hash []byte,
) (*leiosEndorserBlockData, bool) {
	db := o.leiosDatabase()
	if db == nil {
		return nil, false
	}
	manifestRaw, err := db.GetLeiosEBManifest(hash, slot)
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
	block, err := decodeLeiosEndorserBlock(manifestRaw)
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
	txsRaw, err := db.GetLeiosEBTxs(hash, slot)
	if err != nil && !errors.Is(err, types.ErrBlobKeyNotFound) {
		o.config.Logger.Debug(
			"failed to load leios EB txs from blob store",
			"component", "network",
			"error", err,
		)
		return nil, false
	}
	if len(txsRaw) > 0 {
		err := validateLeiosEndorserBlockTxs(manifestRaw, txsRaw)
		if err == nil {
			err = validateLeiosEndorserBlockTxStructures(txsRaw)
		}
		if err != nil {
			// Transaction blobs written before manifest binding was enforced (or
			// structural decoding was enforced), or corrupted afterward, are only
			// a historical-serving cache. Keep the content-addressed manifest, but
			// discard the untrusted bodies so the normal by-point path fetches and
			// persists a verified replacement.
			o.config.Logger.Debug(
				"discarding invalid persisted leios EB txs",
				"component", "network",
				"hash", hex.EncodeToString(hash),
				"error", err,
			)
			txsRaw = nil
		}
	}

	cacheKeys := []string{leiosBlockKey(slot, hash)}
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
		// window.
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
			"component",
			"network",
			"hash",
			hex.EncodeToString(hash),
			"size",
			n,
			"max_size",
			leiosEndorserBlockCacheMaxEntryBytes,
		)
		return data, true
	}
	// Populate the in-memory cache so subsequent lookups skip the DB.
	o.leiosMu.Lock()
	if o.leiosEndorserBlocks == nil {
		o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	}
	// Only cache if no fresher entry has appeared while we were loading.
	now := time.Now()
	if existing := o.leiosEndorserBlocks[cacheKeys[0]]; existing == nil ||
		existing.expired(now) || !existing.hasManifest() {
		if existing != nil && !existing.expired(now) {
			data.relayOfferRequested = existing.relayOfferRequested
			data.relayOfferSource = existing.relayOfferSource
		}
		o.pruneLeiosEndorserBlockCacheLocked(now)
		o.leiosEndorserBlockSeq++
		data.seq = o.leiosEndorserBlockSeq
		for _, key := range cacheKeys {
			o.leiosEndorserBlocks[key] = data
		}
		// Prune again after inserting so a reload that fits the per-entry
		// budget but pushes the aggregate over its budget is evicted
		// promptly, the same way storeLeiosEndorserBlock prunes both before
		// and after admitting an entry.
		o.pruneLeiosEndorserBlockCacheLocked(now)
	} else {
		data = existing
	}
	o.leiosMu.Unlock()
	o.enqueueLeiosRelayOffers(data.point, data)
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
		ret = append(ret, tx)
	}
	return txresponse.Clone(ret)
}

func leiosAvailableTxsFromBitmap(
	txs []cbor.RawMessage,
	requested map[uint16]uint64,
) (map[uint16]uint64, []cbor.RawMessage) {
	available := make(map[uint16]uint64)
	for idx, tx := range txs {
		if tx == nil {
			continue
		}
		bucket := idx / 64
		if bucket > math.MaxUint16 {
			break
		}
		mask := uint64(1) << uint(63-(idx%64))
		if requested[uint16(bucket)]&mask == 0 { // #nosec G115 -- checked above
			continue
		}
		available[uint16(bucket)] |= mask // #nosec G115 -- checked above
	}
	return available, leiosTxsFromBitmap(txs, available)
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
// ledger to apply when the referencing Dijkstra ranking block is processed.
// ebSlot is the slot the caller's own reference requires: the manifest is
// content-addressed, so the same hash can be a live, independently required
// occurrence at more than one slot at once, and this looks up exactly that
// occurrence rather than whichever one happens to be cached for the hash.
// ok is false when that exact (slot, hash) occurrence
// is not cached, its transactions are incomplete, or its slot binding is not
// yet verified -- the ledger keys the endorser blob it persists on this
// slot, so an unverified peer claim must not reach it. It
// satisfies ledger.EndorserBlockProviderFunc.
func (o *Ouroboros) EndorserBlockTxsByHash(
	ebHash []byte,
	ebSlot uint64,
) ([]cbor.RawMessage, bool) {
	data, ok := o.lookupLeiosEndorserBlock(ebSlot, ebHash)
	if !ok || !data.completeTxCache() || !data.slotVerified ||
		data.semanticValidationStatus == leiosEBValidationPending ||
		data.semanticValidationStatus == leiosEBValidationInvalid {
		return nil, false
	}
	return cloneRawMessages(data.txsRaw), true
}

// EndorserBlockDataByHash returns the verified manifest and complete
// transaction bodies for one cached endorser-block occurrence. It satisfies
// ledger.EndorserBlockDataProviderFunc for read paths that race persistence.
func (o *Ouroboros) EndorserBlockDataByHash(
	ebHash []byte,
	ebSlot uint64,
) ([]byte, []cbor.RawMessage, bool) {
	data, ok := o.lookupLeiosEndorserBlock(ebSlot, ebHash)
	if !ok || !data.completeTxCache() || !data.slotVerified ||
		data.semanticValidationStatus == leiosEBValidationPending ||
		data.semanticValidationStatus == leiosEBValidationInvalid {
		return nil, nil, false
	}
	return bytes.Clone(data.blockRaw), cloneRawMessages(data.txsRaw), true
}

// EndorserBlockValidation reports the cached semantic verdict for an endorser
// block occurrence.
func (o *Ouroboros) EndorserBlockValidation(
	ebHash []byte,
	ebSlot uint64,
) ledger.EndorserBlockValidationStatus {
	data, ok := o.lookupLeiosEndorserBlock(ebSlot, ebHash)
	if !ok {
		return ledger.EndorserBlockValidationUnknown
	}
	switch data.semanticValidationStatus {
	case leiosEBValidationPending:
		return ledger.EndorserBlockValidationPending
	case leiosEBValidationValid:
		return ledger.EndorserBlockValidationValid
	case leiosEBValidationInvalid:
		return ledger.EndorserBlockValidationInvalid
	default:
		return ledger.EndorserBlockValidationUnknown
	}
}

// EndorserBlockTxHashesByHash returns the manifest-order transaction hashes of
// a complete cached endorser block at ebSlot. The forge loop uses this to
// build the prototype-2026w29 post-certificate mempool view: transactions in
// the EB being certified are excluded from the new EB announced by the same
// ranking block. See EndorserBlockTxsByHash for why ebSlot is required.
func (o *Ouroboros) EndorserBlockTxHashesByHash(
	ebHash []byte,
	ebSlot uint64,
) ([]string, bool) {
	data, ok := o.lookupLeiosEndorserBlock(ebSlot, ebHash)
	if !ok || !data.completeTxCache() || !data.slotVerified {
		return nil, false
	}
	block, err := decodeLeiosEndorserBlock(data.blockRaw)
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
	top, err := safedecode.Guard(func() ([]cbor.RawMessage, error) {
		var top []cbor.RawMessage
		_, err := cbor.Decode(blockCbor, &top)
		return top, err
	})
	if err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, false
	}
	header, err := safedecode.Guard(func() (gdijkstra.DijkstraBlockHeader, error) {
		var header gdijkstra.DijkstraBlockHeader
		_, err := cbor.Decode(top[0], &header)
		return header, err
	})
	if err != nil {
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
	ctx context.Context,
	blockCbor []byte,
) (ebHash lcommon.Blake2b256, ebSlot uint64, certified bool, resolved bool) {
	top, err := safedecode.Guard(func() ([]cbor.RawMessage, error) {
		var top []cbor.RawMessage
		_, err := cbor.Decode(blockCbor, &top)
		return top, err
	})
	if err != nil || len(top) == 0 {
		return lcommon.Blake2b256{}, 0, false, false
	}
	header, err := safedecode.Guard(func() (gdijkstra.DijkstraBlockHeader, error) {
		var header gdijkstra.DijkstraBlockHeader
		_, err := cbor.Decode(top[0], &header)
		return header, err
	})
	if err != nil {
		return lcommon.Blake2b256{}, 0, false, false
	}
	if cert, present := header.LeiosCertified(); !present || !cert {
		return lcommon.Blake2b256{}, 0, false, false
	}
	// The header is certified from here on; a resolution failure below keeps
	// certified=true so the caller disconnects instead of serving raw.
	if o.ledgerState == nil {
		return lcommon.Blake2b256{}, 0, true, false
	}
	prevHash := header.PrevHash()
	parent, err := o.ledgerState.BlockByHash(ctx, prevHash.Bytes())
	if err != nil {
		return lcommon.Blake2b256{}, 0, true, false
	}
	hash, ok := leiosAnnouncementFromBlockCbor(parent.Cbor)
	if !ok {
		return lcommon.Blake2b256{}, 0, true, false
	}
	// The endorser block shares its announcing ranking block's slot -- here,
	// the parent's -- the same structural derivation
	// leiosEndorserBlockForApply uses on the ledger side, rather than the
	// CertRB's own slot (a different, later block).
	return hash, parent.Slot, true, true
}

// resolveCertifiedEndorserTxs returns the endorser-block transactions that a
// certifying ranking block (CertRB) inlines over node-to-client, or ok=false
// when the block is not a CertRB or its endorser block is not fully available.
func (o *Ouroboros) resolveCertifiedEndorserTxs(
	ctx context.Context,
	blockCbor []byte,
) ([]cbor.RawMessage, bool) {
	ebHash, ebSlot, _, resolved := o.certifiedEndorserBlockHash(ctx, blockCbor)
	if !resolved {
		return nil, false
	}
	data, found := o.lookupLeiosEndorserBlock(ebSlot, ebHash.Bytes())
	if !found || !data.completeTxCache() || !data.slotVerified {
		return nil, false
	}
	return cloneRawMessages(data.txsRaw), true
}

// leiosClosureCompleteLocked reports whether a complete, slot-verified
// transaction closure is cached in memory for the given cache key, using the
// same readiness predicate (completeTxCache and slotVerified) as the
// resolver. Gating on slotVerified here matters as much as it does in the
// resolver: without it, a waiter could be woken (or return immediately) on a
// closure that is complete but still carries an offering connection's
// unverified slot claim. The caller must hold leiosMu.
func (o *Ouroboros) leiosClosureCompleteLocked(key string) bool {
	data, ok := o.leiosEndorserBlocks[key]
	return ok && data != nil && data.completeTxCache() && data.slotVerified
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
// (ebSlot, ebHash) is cached in memory or ctx is done. It returns true once
// the closure is available.
func (o *Ouroboros) waitForLeiosEndorserClosure(
	ctx context.Context,
	ebSlot uint64,
	ebHash []byte,
) bool {
	key := leiosBlockKey(ebSlot, ebHash)
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
	ebSlot uint64,
) ([]byte, bool) {
	if !o.waitForLeiosEndorserClosure(ctx, ebSlot, ebHash.Bytes()) {
		return nil, false
	}
	merged, ok, err := o.mergedLeiosRankingBlockCbor(ctx, blockCbor)
	if err != nil || !ok {
		return nil, false
	}
	return merged, true
}

// spliceEndorserTxsIntoDijkstraBlock returns rankingBlockCbor with the endorser
// block's transactions inlined into the ranking block's (empty) transaction
// segment, matching the node-to-client "merged" block the prototype serves for
// a certifying ranking block. The Dijkstra block is [header, block_body] with
// block_body = [transactions, leios_certificate, peras_certificate]. The
// transactions element is replaced and the leios_certificate element is
// cleared, because CIP-0164 permits a certificate or transactions and not
// both. The header and peras element are preserved verbatim so the served
// block's hash (a hash of the header) is unchanged; the header's block_body_hash
// intentionally no longer matches, which is acceptable over node-to-client
// because local clients do not re-verify the body hash.
//
// It returns an error (and the caller serves the raw block) when the block is
// not a fillable CertRB shape: the top level must have two elements, the body
// three, and the existing transactions segment must be empty. Endorser
// transactions are already in block_transaction form, is_valid included, and
// are inlined unchanged, so a transaction marked invalid stays invalid.
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
	if len(body) != 3 {
		return nil, fmt.Errorf(
			"dijkstra block body has %d elements, expected 3",
			len(body),
		)
	}
	var existingTxs []cbor.RawMessage
	if _, err := cbor.Decode(body[0], &existingTxs); err != nil {
		return nil, fmt.Errorf("decode dijkstra transactions: %w", err)
	}
	if len(existingTxs) != 0 {
		return nil, fmt.Errorf(
			"ranking block already has %d transactions; not a fillable CertRB",
			len(existingTxs),
		)
	}
	newTxs := make([]cbor.RawMessage, len(ebTxsRaw))
	var err error
	for i, tx := range ebTxsRaw {
		var txCbor []byte
		txCbor, err = leiosEndorserBlockTxCbor(tx)
		newTxs[i] = txCbor
		if err != nil {
			return nil, fmt.Errorf("convert endorser transaction %d: %w", i, err)
		}
	}
	newTxsRaw, err := cbor.Encode(newTxs)
	if err != nil {
		return nil, fmt.Errorf("encode endorser transactions: %w", err)
	}
	// The leios_certificate segment is cleared rather than preserved. CIP-0164
	// permits a certificate or transactions, not both, and gouroboros enforces
	// that inside DijkstraBlockBody.UnmarshalCBOR, where no VerifyConfig Skip
	// field reaches it. Keeping the certificate alongside the inlined
	// transactions would make the served block undecodable by any client
	// rather than merely body-hash-stale.
	nilCert, err := cbor.Encode(nil)
	if err != nil {
		return nil, fmt.Errorf("encode cleared leios certificate: %w", err)
	}
	newBody, err := cbor.Encode([]cbor.RawMessage{
		cbor.RawMessage(newTxsRaw), cbor.RawMessage(nilCert), body[2],
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
	ctx context.Context,
	blockCbor []byte,
) ([]byte, bool, error) {
	ebTxsRaw, ok := o.resolveCertifiedEndorserTxs(ctx, blockCbor)
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
	return o.serveLeiosRankingBlockCbor(block, ctx.ConnectionId, ctx.Server)
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
	owner *ochainsync.Server,
) ([]byte, error) {
	ctx := context.Background()
	if owner != nil {
		connDone, cancelWaiter := o.registerLeiosServeWaiter(connId, owner)
		defer cancelWaiter()
		ctx = leiosConnDoneContext{done: connDone}
	}
	merged, ok, err := o.mergedLeiosRankingBlockCbor(
		ctx,
		block.Cbor,
	)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
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
	ebHash, ebSlot, certified, resolved := o.certifiedEndorserBlockHash(
		ctx,
		block.Cbor,
	)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	return o.serveLeiosCertRbWithWait(block, ebHash, ebSlot, connId, owner)
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
	ebSlot uint64,
	connId ouroboros.ConnectionId,
	owner *ochainsync.Server,
) ([]byte, error) {
	connDone, cancelWaiter := o.registerLeiosServeWaiter(connId, owner)
	defer cancelWaiter()
	ctx, cancel := context.WithTimeout(
		leiosConnDoneContext{done: connDone},
		o.leiosClosureWaitTimeout(),
	)
	defer cancel()
	start := time.Now()
	merged, ok := o.awaitMergedLeiosRankingBlock(
		ctx,
		block.Cbor,
		ebHash,
		ebSlot,
	)
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
