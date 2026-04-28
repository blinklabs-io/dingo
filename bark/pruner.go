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

package bark

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger"
)

type PrunerConfig struct {
	LedgerState    *ledger.LedgerState
	DB             *database.Database
	Logger         *slog.Logger
	SecurityWindow uint64
	Frequency      time.Duration
}

type Pruner struct {
	config      PrunerConfig
	logger      *slog.Logger
	ledgerState *ledger.LedgerState
	db          *database.Database

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewPruner(cfg PrunerConfig) *Pruner {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &Pruner{
		config:      cfg,
		logger:      cfg.Logger,
		ledgerState: cfg.LedgerState,
		db:          cfg.DB,
	}
}

func (p *Pruner) pruneBlock(next *database.BlobBlockResult) error {
	if _, err := p.db.PruneBlock(next.Slot, next.Hash); err != nil {
		return fmt.Errorf("pruner: %w", err)
	}
	return nil
}

func (p *Pruner) prune(ctx context.Context) {
	currentSlot, err := p.ledgerState.CurrentSlot()
	if err != nil {
		p.logger.Error(
			"pruner: failed to get current slot",
			"error",
			err,
		)
		return
	}

	if currentSlot <= p.config.SecurityWindow {
		p.logger.Debug(
			"pruner: skipped because current slot is not high enough")
		return
	}
	iter := p.db.BlocksInRange(
		0,
		currentSlot-p.config.SecurityWindow-1,
	)
	defer iter.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			next, err := iter.NextRaw()
			if err != nil {
				p.logger.Error(
					"pruner: failed to prune block",
					"error",
					err,
				)
				return
			}
			if next == nil {
				p.logger.Debug("pruner: completed round of pruning")
				return
			}

			if err := p.pruneBlock(next); err != nil {
				p.logger.Error(
					"pruner: failed to prune block",
					"error",
					err,
				)
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
			"pruner: invalid frequency %d (must be > 0)",
			p.config.Frequency,
		)
	}
	if p.ledgerState == nil {
		return errors.New("pruner: ledger state must not be nil")
	}
	if p.db == nil {
		return errors.New("pruner: database must not be nil")
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
			"pruner: failed to stop before context cancellation: %w",
			ctx.Err(),
		)
	}
}
