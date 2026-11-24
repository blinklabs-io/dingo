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

package historyexpiry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// LedgerWindow exposes the ledger-derived slot horizon used to decide when
// immutable block history can be expired locally.
type LedgerWindow interface {
	CurrentSlot() (uint64, error)
	StabilityWindow() uint64
}

type PrunerConfig struct {
	LedgerState LedgerWindow
	DB          *database.Database
	Logger      *slog.Logger
	Frequency   time.Duration
	// Ceiling, when set, bounds the highest slot a round may expire, for a
	// reader such as the IndexerDB writer that has yet to read older block
	// bodies. ok false means no block may be expired yet.
	Ceiling func(ctx context.Context) (slot uint64, ok bool, err error)
	// Held, when set, reports whether the block still holds a payload in use,
	// such as a datum or script a live output names. A held block is skipped
	// and recorded, and checked again at the start of every later round, once
	// what used it is gone. The scan cursor moves past it.
	Held func(ctx context.Context, slot uint64, hash []byte) (bool, error)
}

type Pruner struct {
	config      PrunerConfig
	logger      *slog.Logger
	ledgerState LedgerWindow
	db          *database.Database
	// expire is a seam so a test can inject a per-block failure.
	expire func(context.Context, *database.BlobBlockResult) error

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewPruner(cfg PrunerConfig) *Pruner {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	p := &Pruner{
		config:      cfg,
		logger:      cfg.Logger,
		ledgerState: cfg.LedgerState,
		db:          cfg.DB,
	}
	p.expire = p.pruneBlock
	return p
}

func (p *Pruner) pruneBlock(
	ctx context.Context,
	next *database.BlobBlockResult,
) error {
	if _, err := p.db.PruneBlock(ctx, next.Slot, next.Hash); err != nil {
		return fmt.Errorf("history expiry: %w", err)
	}
	return nil
}

// loadCursor returns the persisted resume slot, or 0 when none is usable.
func (p *Pruner) loadCursor() uint64 {
	raw, err := p.db.GetChainState(database.HistoryExpiryCursorKey, nil)
	if err != nil {
		p.logger.Error("history expiry: failed to read cursor", "error", err)
		return 0
	}
	if raw == "" {
		return 0
	}
	cursor, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		p.logger.Error(
			"history expiry: ignoring malformed cursor",
			"value", raw,
			"error", err,
		)
		return 0
	}
	return cursor
}

func (p *Pruner) saveCursor(cursor uint64) {
	if err := p.db.SetChainState(
		database.HistoryExpiryCursorKey,
		strconv.FormatUint(cursor, 10),
		nil,
	); err != nil {
		p.logger.Error("history expiry: failed to save cursor", "error", err)
	}
}

// refreshRetainedBound recomputes the start of retained history that
// expired-history errors name, so request paths read it without scanning.
func (p *Pruner) refreshRetainedBound(ctx context.Context) {
	if err := p.db.RefreshOldestRetainedSlot(ctx); err != nil &&
		ctx.Err() == nil {
		p.logger.Error(
			"history expiry: failed to refresh oldest retained slot",
			"error", err,
		)
	}
}

// loadHeld returns the blocks held by earlier rounds. A failure to read them
// ends the round: scanning on would move the cursor past blocks whose hold is
// then lost.
func (p *Pruner) loadHeld() ([]database.HeldBlock, bool) {
	held, err := p.db.HistoryExpiryHeld()
	if err != nil {
		p.logger.Error("history expiry: failed to read held blocks", "error", err)
		return nil, false
	}
	return held, true
}

// recheckHeld expires each held block whose payloads are no longer in use and
// returns the blocks that stay held. A block that is already expired, or that
// a rollback removed, needs no further expiry. It returns ok false when the
// round must end.
func (p *Pruner) recheckHeld(
	ctx context.Context,
	held []database.HeldBlock,
) (kept []database.HeldBlock, ok bool) {
	for i, block := range held {
		if ctx.Err() != nil {
			return append(kept, held[i:]...), false
		}
		inUse, err := p.config.Held(ctx, block.Slot, block.Hash)
		if err != nil {
			if ctx.Err() == nil {
				p.logger.Error(
					"history expiry: failed to check block payloads",
					"error", err,
				)
			}
			return append(kept, held[i:]...), false
		}
		if inUse {
			kept = append(kept, block)
			continue
		}
		err = p.expire(ctx, &database.BlobBlockResult{
			Slot: block.Slot, Hash: block.Hash,
		})
		if err == nil ||
			errors.Is(err, types.ErrHistoryExpired) ||
			errors.Is(err, models.ErrBlockNotFound) ||
			errors.Is(err, types.ErrBlobKeyNotFound) {
			continue
		}
		if ctx.Err() == nil {
			p.logger.Error(
				"history expiry: failed to expire block",
				"error", err,
			)
		}
		return append(kept, held[i:]...), false
	}
	return kept, true
}

func (p *Pruner) saveHeld(held []database.HeldBlock) {
	if err := p.db.SetHistoryExpiryHeld(held); err != nil {
		p.logger.Error("history expiry: failed to save held blocks", "error", err)
	}
}

func (p *Pruner) prune(ctx context.Context) {
	currentSlot, err := p.ledgerState.CurrentSlot()
	if err != nil {
		p.logger.Error(
			"history expiry: failed to get current slot",
			"error",
			err,
		)
		return
	}

	stabilityWindow := p.ledgerState.StabilityWindow()
	if currentSlot <= stabilityWindow {
		p.logger.Debug(
			"history expiry: skipped because current slot is not high enough")
		return
	}
	end := currentSlot - stabilityWindow - 1
	if p.config.Ceiling != nil {
		ceiling, ok, err := p.config.Ceiling(ctx)
		if err != nil {
			if ctx.Err() == nil {
				p.logger.Error(
					"history expiry: failed to read the expiry ceiling",
					"error", err,
				)
			}
			return
		}
		if !ok {
			p.logger.Debug(
				"history expiry: skipped because no block may be expired yet")
			return
		}
		end = min(end, ceiling)
	}
	// cursor is the slot every block before has been expired through or
	// recorded as held. It stops advancing at the first block whose expiry
	// fails so that block stays inside the next round's range. The scan
	// restarts at the cursor slot itself because only some of the blocks
	// sharing it may be expired.
	start := p.loadCursor()
	cursor := start
	// held is every block a payload in use keeps. The cursor moves past a held
	// block, so a block that stays held costs a check each round and not a
	// re-scan of the chain after it.
	var held []database.HeldBlock
	if p.config.Held != nil {
		var ok bool
		held, ok = p.loadHeld()
		if !ok {
			return
		}
	}
	heldBefore := len(held)
	heldChanged := false
	defer func() {
		// The held blocks are saved before the cursor passes them, so a failed
		// save leaves the cursor behind and the blocks inside the next scan.
		if heldChanged || len(held) != heldBefore {
			p.saveHeld(held)
		}
		if cursor > start {
			p.saveCursor(cursor)
		}
	}()
	if len(held) > 0 {
		kept, ok := p.recheckHeld(ctx, held)
		heldChanged = len(kept) != len(held)
		held = kept
		if !ok {
			return
		}
	}
	type blockKey struct {
		hash string
		slot uint64
	}
	heldKeys := make(map[blockKey]struct{}, len(held))
	for _, block := range held {
		heldKeys[blockKey{string(block.Hash), block.Slot}] = struct{}{}
	}
	iter := p.db.BlocksInRangeContext(ctx, start, end)
	defer iter.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			next, err := iter.NextRaw()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if errors.Is(err, types.ErrHistoryExpired) {
					cursor, _ = iter.Progress()
					continue
				}
				p.logger.Error(
					"history expiry: failed to expire block",
					"error",
					err,
				)
				return
			}
			if next == nil {
				p.logger.Debug("history expiry: completed round")
				return
			}

			if p.config.Held != nil {
				if _, ok := heldKeys[blockKey{string(next.Hash), next.Slot}]; ok {
					// Rechecked above, and kept: the cursor passes it.
					cursor = next.Slot
					continue
				}
				inUse, err := p.config.Held(ctx, next.Slot, next.Hash)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					p.logger.Error(
						"history expiry: failed to check block payloads",
						"error", err,
					)
					return
				}
				if inUse {
					held = append(held, database.HeldBlock{
						Slot: next.Slot, Hash: bytes.Clone(next.Hash),
					})
					heldKeys[blockKey{string(next.Hash), next.Slot}] = struct{}{}
					cursor = next.Slot
					continue
				}
			}
			if err := p.expire(ctx, next); err != nil {
				if ctx.Err() != nil {
					return
				}
				p.logger.Error(
					"history expiry: failed to expire block",
					"error",
					err,
				)
				return
			}
			cursor = next.Slot
		}
	}
}

func (p *Pruner) run(ctx context.Context) {
	ticker := time.NewTicker(p.config.Frequency)
	defer ticker.Stop()
	p.refreshRetainedBound(ctx)
	for {
		select {
		case <-ticker.C:
			p.prune(ctx)
			p.refreshRetainedBound(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (p *Pruner) Start(ctx context.Context) error {
	if p.config.Frequency <= 0 {
		return fmt.Errorf(
			"history expiry: invalid frequency %d (must be > 0)",
			p.config.Frequency,
		)
	}
	if p.ledgerState == nil {
		return errors.New("history expiry: ledger state must not be nil")
	}
	if p.db == nil {
		return errors.New("history expiry: database must not be nil")
	}

	ctx, p.cancel = context.WithCancel(ctx) //nolint:gosec

	p.wg.Go(func() {
		p.run(ctx)
	})
	return nil
}

func (p *Pruner) Stop(ctx context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf(
			"history expiry: failed to stop before context cancellation: %w",
			ctx.Err(),
		)
	}
}
