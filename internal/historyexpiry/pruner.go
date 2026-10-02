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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
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
}

type Pruner struct {
	config      PrunerConfig
	logger      *slog.Logger
	ledgerState LedgerWindow
	db          *database.Database
	// expire is a seam so a test can inject a per-block failure.
	expire func(*database.BlobBlockResult) error

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

func (p *Pruner) pruneBlock(next *database.BlobBlockResult) error {
	if _, err := p.db.PruneBlock(next.Slot, next.Hash); err != nil {
		return fmt.Errorf("history expiry: %w", err)
	}
	return nil
}

// loadCursor returns the persisted resume slot, or 0 when none is usable.
func (p *Pruner) loadCursor() uint64 {
	raw, err := p.db.GetSyncState(database.HistoryExpiryCursorSyncKey, nil)
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
	if err := p.db.SetSyncState(
		database.HistoryExpiryCursorSyncKey,
		strconv.FormatUint(cursor, 10),
		nil,
	); err != nil {
		p.logger.Error("history expiry: failed to save cursor", "error", err)
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
	// cursor is the slot every block before has been expired through. It
	// stops advancing at the first block whose expiry fails so that block
	// stays inside the next round's range. The scan restarts at the cursor
	// slot itself because only some of the blocks sharing it may be expired.
	start := p.loadCursor()
	cursor := start
	stalled := false
	defer func() {
		if cursor > start {
			p.saveCursor(cursor)
		}
	}()
	iter := p.db.BlocksInRange(
		start,
		currentSlot-stabilityWindow-1,
	)
	defer iter.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			next, err := iter.NextRaw()
			if err != nil {
				if errors.Is(err, types.ErrHistoryExpired) {
					if !stalled {
						cursor, _ = iter.Progress()
					}
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

			if err := p.expire(next); err != nil {
				p.logger.Error(
					"history expiry: failed to expire block",
					"error",
					err,
				)
				stalled = true
			} else if !stalled {
				cursor = next.Slot
			}
		}
	}
}

func (p *Pruner) run(ctx context.Context) {
	ticker := time.NewTicker(p.config.Frequency)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.prune(ctx)
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
