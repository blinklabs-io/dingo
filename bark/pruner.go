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

func NewPruner(config PrunerConfig) *Pruner {
	return &Pruner{
		config:      config,
		logger:      config.Logger,
		ledgerState: config.LedgerState,
		db:          config.DB,
	}
}

func (p *Pruner) prune() {
	currentSlot, err := p.ledgerState.CurrentSlot()
	if err != nil {
		p.logger.Error(
			"pruner: failed to get current slot",
			"error",
			err,
		)
		return
	}

	if currentSlot < p.config.SecurityWindow {
		p.logger.Debug(
			"pruner: skipped because current slot is not high enough")
		return
	}
	iter := p.db.BlocksInRange(0, currentSlot-p.config.SecurityWindow)
	defer iter.Close()

	for {
		next, err := iter.NextRaw()
		if err != nil {
			p.logger.Error(
				"pruner: failed to get next block",
				"error",
				err,
			)
			return
		}
		if next == nil {
			p.logger.Debug("pruner: completed round of pruning")
			return
		}

		txn := p.db.BlobTxn(true)
		_, metadata, err := p.db.Blob().GetBlock(txn, next.Slot, next.Hash)
		if err != nil {
			p.logger.Error(
				"pruner: failed to get block",
				"error",
				err,
			)
			return
		}
		if err := p.db.Blob().DeleteBlock(
			txn,
			next.Slot,
			next.Hash,
			metadata.ID,
		); err != nil {
			p.logger.Error(
				"pruner: failed to delete block",
				"error",
				err,
			)
			return
		}
	}
}

func (p *Pruner) run(ctx context.Context) {
	ticker := time.NewTicker(p.config.Frequency)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.prune()
		case <-ctx.Done():
			return
		}
	}
}

func (p *Pruner) Start(ctx context.Context) error {
	ctx, p.cancel = context.WithCancel(ctx)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.run(ctx)
	}()
	return nil
}

func (p *Pruner) Close(ctx context.Context) error {
	p.cancel()
	p.wg.Wait()
	return nil
}
