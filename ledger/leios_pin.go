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

package ledger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ImmutableTipSource reports the selected chain's immutable point. LedgerState
// is the production source: its point is k blocks behind the applied tip.
type ImmutableTipSource interface {
	ImmutablePoint(ctx context.Context) (ocommon.Point, bool, error)
}

// SetImmutableTipSource replaces the source AdvanceLeiosPins reads the
// immutable point from. A nil source restores the ledger's own.
func (ls *LedgerState) SetImmutableTipSource(src ImmutableTipSource) {
	ls.leiosPinMu.Lock()
	defer ls.leiosPinMu.Unlock()
	ls.leiosImmutableTip = src
}

func (ls *LedgerState) immutableTipSource() ImmutableTipSource {
	if ls.leiosImmutableTip != nil {
		return ls.leiosImmutableTip
	}
	return ls
}

// leiosPinRepairPerPass bounds how many content-pending pins one walk tries
// to repair, so a peerless node cannot stall collection behind fetch waits.
const leiosPinRepairPerPass = 4

// leiosPinRepairTimeout bounds one repair fetch.
const leiosPinRepairTimeout = 20 * time.Second

// AdvanceLeiosPins pins into the Leios store's immutable partition the
// endorser blocks certified by ranking blocks up to the immutable point. It
// walks the primary chain from the stored pin watermark, so certification is
// derived from the chain itself and a rollback leaves nothing to clear.
// Pinning a certification whose content is missing records a content-pending
// pin and is never an error: it must not hold up promotion, and the content
// is repaired from the in-memory cache or the backfiller on later passes.
//
// It is safe to call at any time and from several goroutines; each call
// resumes from the watermark. A crash after any pin and before the watermark
// advances repeats that pin, which is idempotent.
func (ls *LedgerState) AdvanceLeiosPins(ctx context.Context) error {
	ls.leiosPinMu.Lock()
	defer ls.leiosPinMu.Unlock()
	if ls.db == nil {
		return errors.New("advance Leios pins: database unavailable")
	}
	store, err := ls.db.LeiosDB()
	if err != nil {
		return fmt.Errorf("advance Leios pins: %w", err)
	}
	immutable, found, err := ls.immutableTipSource().ImmutablePoint(ctx)
	if err != nil {
		return fmt.Errorf("advance Leios pins: immutable point: %w", err)
	}
	if !found {
		return nil
	}
	immutableRef := leiosdb.Ref{Slot: immutable.Slot, Hash: immutable.Hash}
	wm, err := store.Watermarks()
	if err != nil {
		return fmt.Errorf("advance Leios pins: %w", err)
	}
	if wm.PinnedThroughSet && wm.PinnedThrough.Slot > immutable.Slot {
		// The immutable point is derived from the applied tip, so an
		// ordinary rollback lowers it while the pinned blocks are still on
		// the chain. Those pins stay: only a chain that no longer contains
		// the pinned-through block was truncated below the watermark, as when
		// a crash fell between the truncation's commit and the pin rollback.
		onChain, err := ls.leiosPinnedThroughOnChain(ctx, wm.PinnedThrough)
		if err != nil {
			return fmt.Errorf("advance Leios pins: %w", err)
		}
		if onChain {
			ls.repairPendingLeiosPins(ctx, store)
			return nil
		}
		if _, err := store.RollbackPins(immutableRef); err != nil {
			return fmt.Errorf("advance Leios pins: reconcile: %w", err)
		}
		if wm, err = store.Watermarks(); err != nil {
			return fmt.Errorf("advance Leios pins: %w", err)
		}
	}
	ls.repairPendingLeiosPins(ctx, store)
	if wm.PinnedThroughSet &&
		wm.PinnedThrough.Slot == immutable.Slot &&
		bytes.Equal(wm.PinnedThrough.Hash, immutable.Hash) {
		return nil
	}
	return ls.walkLeiosPins(ctx, store, wm, immutable)
}

// leiosPinnedThroughOnChain reports whether the pin watermark's ranking block
// is on the primary chain.
func (ls *LedgerState) leiosPinnedThroughOnChain(
	ctx context.Context,
	through leiosdb.Ref,
) (bool, error) {
	primary := ls.Chain()
	if primary == nil {
		return false, errors.New("primary chain is nil")
	}
	_, found, err := primary.PointAtDepthFrom(
		ctx, ocommon.NewPoint(through.Slot, through.Hash), 0,
	)
	if errors.Is(err, models.ErrBlockNotFound) {
		return false, nil
	}
	return found, err
}

func (ls *LedgerState) walkLeiosPins(
	ctx context.Context,
	store leiosdb.Store,
	wm leiosdb.Watermarks,
	immutable ocommon.Point,
) error {
	primary := ls.Chain()
	if primary == nil {
		return errors.New("advance Leios pins: primary chain is nil")
	}
	start := ocommon.NewPoint(0, nil)
	if wm.PinnedThroughSet {
		start = ocommon.NewPoint(wm.PinnedThrough.Slot, wm.PinnedThrough.Hash)
	}
	it, err := primary.FromPointContext(ctx, start, false)
	if err != nil {
		return fmt.Errorf(
			"advance Leios pins: iterate from %d: %w",
			start.Slot,
			err,
		)
	}
	defer it.Cancel()
	var last *leiosdb.Ref
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		res, err := it.Next(false)
		if errors.Is(err, chain.ErrIteratorChainTip) {
			break
		}
		if err != nil {
			return fmt.Errorf("advance Leios pins: next block: %w", err)
		}
		if res.Rollback {
			return errors.New(
				"advance Leios pins: chain rolled back during the walk",
			)
		}
		if res.Block.Slot > immutable.Slot {
			break
		}
		blk, err := res.Block.Decode()
		if err != nil {
			return fmt.Errorf(
				"advance Leios pins: decode block at slot %d: %w",
				res.Block.Slot, err,
			)
		}
		certifier := leiosdb.Ref{Slot: res.Block.Slot, Hash: res.Block.Hash}
		ebHash, ebSlot, referenced, err := ls.leiosEndorserBlockForApply(
			ctx,
			blk,
		)
		if err != nil {
			return fmt.Errorf(
				"advance Leios pins: resolve certification at slot %d: %w",
				res.Block.Slot, err,
			)
		}
		if referenced {
			if err := ls.pinLeiosEndorserBlock(
				ctx, store, ebHash, ebSlot, certifier,
			); err != nil {
				return err
			}
			if err := store.AdvancePinnedThrough(certifier); err != nil {
				return fmt.Errorf("advance Leios pins: %w", err)
			}
		}
		last = &certifier
		if res.Block.Slot == immutable.Slot &&
			bytes.Equal(res.Block.Hash, immutable.Hash) {
			break
		}
	}
	if last == nil {
		return nil
	}
	if err := store.AdvancePinnedThrough(*last); err != nil {
		return fmt.Errorf("advance Leios pins: %w", err)
	}
	return nil
}

func (ls *LedgerState) pinLeiosEndorserBlock(
	ctx context.Context,
	store leiosdb.Store,
	ebHash lcommon.Blake2b256,
	ebSlot uint64,
	certifier leiosdb.Ref,
) error {
	ref := leiosdb.Ref{Slot: ebSlot, Hash: ebHash.Bytes()}
	res, err := store.Pin(ref, certifier)
	if err != nil {
		return fmt.Errorf("pin endorser block at slot %d: %w", ebSlot, err)
	}
	if !res.ContentPending {
		return nil
	}
	if ls.metrics.leiosPinsContentPending != nil {
		ls.metrics.leiosPinsContentPending.Inc()
	}
	ls.config.Logger.Warn(
		"pinned certified Leios endorser block without its content",
		"component", "ledger",
		"eb_slot", ebSlot,
		"certifier_slot", certifier.Slot,
	)
	ls.repairLeiosPin(ctx, store, ref, false)
	return nil
}

// repairPendingLeiosPins tries to supply the content of pins recorded without
// it, a few per pass.
func (ls *LedgerState) repairPendingLeiosPins(
	ctx context.Context,
	store leiosdb.Store,
) {
	pending, err := store.PendingPins()
	if err != nil {
		ls.config.Logger.Warn(
			"failed to list content-pending Leios pins",
			"component", "ledger",
			"error", err,
		)
		return
	}
	for i, p := range pending {
		if i >= leiosPinRepairPerPass || ctx.Err() != nil {
			return
		}
		ls.repairLeiosPin(ctx, store, p.EB, true)
	}
}

// repairLeiosPin stores the content of a pinned endorser block from the
// in-memory cache and, when allowFetch is set, from the backfiller. Failure
// leaves the pin pending for a later pass.
func (ls *LedgerState) repairLeiosPin(
	ctx context.Context,
	store leiosdb.Store,
	ref leiosdb.Ref,
	allowFetch bool,
) {
	data := ls.config.EndorserBlockDataProvider
	if data == nil {
		return
	}
	manifest, txs, ok := data(ref.Hash, ref.Slot)
	if !ok && allowFetch && ls.config.EndorserBlockFetcher != nil {
		fetchCtx, cancel := context.WithTimeout(ctx, leiosPinRepairTimeout)
		err := ls.config.EndorserBlockFetcher(fetchCtx, ref.Slot, ref.Hash)
		cancel()
		if err == nil {
			manifest, txs, ok = data(ref.Hash, ref.Slot)
		}
	}
	if !ok || !bytes.Equal(lcommon.Blake2b256Hash(manifest).Bytes(), ref.Hash) {
		return
	}
	if txs == nil {
		txs = []cbor.RawMessage{}
	}
	if err := store.PutVolatile(
		ref, leiosdb.Content{Manifest: manifest, Txs: txs},
	); err != nil {
		ls.config.Logger.Warn(
			"failed to repair content-pending Leios pin",
			"component", "ledger",
			"eb_slot", ref.Slot,
			"error", err,
		)
	}
}

// MigrateLeiosStore moves the endorser-block records of the shared blob store
// into the Leios store, deriving certification of records no marker covers
// from the primary chain. It is a no-op once the migration has finished.
func (ls *LedgerState) MigrateLeiosStore(ctx context.Context) error {
	if ls.db == nil {
		return errors.New("migrate Leios store: database unavailable")
	}
	opts := database.LeiosMigrationOptions{
		CertifiedByChain: ls.leiosCertifiedByChain,
	}
	store, err := ls.db.LeiosDB()
	if err != nil {
		return fmt.Errorf("migrate Leios store: %w", err)
	}
	state, err := store.MigrationState()
	if err != nil {
		return fmt.Errorf("migrate Leios store: %w", err)
	}
	// A finished migration reads nothing from the chain. An unfinished one
	// treats an immutable point it cannot read as unknown, which keeps every
	// legacy record volatile for the pin walk to decide, rather than failing
	// startup.
	if state != leiosdb.MigrationDone {
		point, found, err := ls.immutableTipSource().ImmutablePoint(ctx)
		switch {
		case err != nil:
			ls.config.Logger.Warn(
				"Leios store migration continues without an immutable point",
				"component", "ledger",
				"error", err,
			)
		case found:
			opts.ImmutablePoint = leiosdb.Ref{
				Slot: point.Slot,
				Hash: point.Hash,
			}
			opts.ImmutableKnown = true
		}
	}
	res, err := ls.db.MigrateLeiosStore(ctx, opts)
	if err != nil {
		return fmt.Errorf("migrate Leios store: %w", err)
	}
	if !res.AlreadyDone {
		ls.config.Logger.Info(
			"migrated Leios endorser blocks to the Leios store",
			"component", "ledger",
			"volatile", res.Volatile,
			"pinned", res.Pinned,
			"dropped", res.Dropped,
			"malformed", res.Malformed,
			"deleted_keys", res.Deleted,
		)
	}
	return nil
}

// leiosCertifiedByChain reports whether the primary chain holds the ranking
// block that certifies the endorser block occurrence: the block announcing it
// at the occurrence's slot, followed by a certifying child.
func (ls *LedgerState) leiosCertifiedByChain(
	ctx context.Context,
	eb leiosdb.Ref,
) (leiosdb.Ref, bool, error) {
	primary := ls.Chain()
	if primary == nil {
		return leiosdb.Ref{}, false, errors.New("primary chain is nil")
	}
	announcer, err := primary.BlockBeforeSlot(ctx, eb.Slot+1)
	if errors.Is(err, chain.ErrNoBlockBeforeSlot) {
		return leiosdb.Ref{}, false, nil
	}
	if err != nil {
		return leiosdb.Ref{}, false, err
	}
	if announcer.Slot != eb.Slot {
		return leiosdb.Ref{}, false, nil
	}
	announced, ok := leiosAnnouncementFromBlockCbor(announcer.Cbor)
	if !ok || !bytes.Equal(announced.Bytes(), eb.Hash) {
		return leiosdb.Ref{}, false, nil
	}
	it, err := primary.FromPointContext(
		ctx, ocommon.NewPoint(announcer.Slot, announcer.Hash), false,
	)
	if err != nil {
		return leiosdb.Ref{}, false, err
	}
	defer it.Cancel()
	res, err := it.Next(false)
	if errors.Is(err, chain.ErrIteratorChainTip) {
		return leiosdb.Ref{}, false, nil
	}
	if err != nil {
		return leiosdb.Ref{}, false, err
	}
	child, err := res.Block.Decode()
	if err != nil {
		return leiosdb.Ref{}, false, err
	}
	_, _, referenced, err := ls.leiosEndorserBlockForApply(ctx, child)
	if err != nil || !referenced {
		return leiosdb.Ref{}, false, err
	}
	return leiosdb.Ref{Slot: res.Block.Slot, Hash: res.Block.Hash}, true, nil
}
