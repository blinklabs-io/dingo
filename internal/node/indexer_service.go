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

package node

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/ledger"
)

// IndexerService runs the split layout's writer: a DatabaseSource over the
// ledger database, the Backfill row writer over the IndexerDB, the ledger
// fact deriver, and the genesis seed.
type IndexerService struct {
	writer *indexer.Writer
	store  *indexerdb.Store

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	runErr  error
	started bool
}

var _ dingo.IndexerWriter = (*IndexerService)(nil)

// NewIndexerService builds the writer for deps. It is a dingo.IndexerWriterFactory.
func NewIndexerService(
	deps dingo.IndexerWriterDeps,
) (dingo.IndexerWriter, error) {
	if deps.Consensus == nil || deps.Indexer == nil {
		return nil, errors.New("indexer service needs both databases")
	}
	// An IndexerDB a release seeded from genesis holds the zero-coin Byron
	// outputs the reference translation drops. They go before the writer
	// reads the IndexerDB.
	zero, err := ledger.ZeroValueByronGenesisOutputs(deps.NodeConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"derive zero-value Byron genesis outputs: %w",
			err,
		)
	}
	if err := ledger.RepairZeroValueByronGenesis(
		context.Background(), deps.Indexer, zero, deps.Logger,
		func(*database.Txn, uint64) error { return nil },
	); err != nil {
		return nil, fmt.Errorf("repair indexer genesis outputs: %w", err)
	}
	w, store, err := newIndexerWriter(
		deps, indexer.NewDatabaseSource(deps.Consensus),
		indexer.DefaultBatchBlocks,
	)
	if err != nil {
		return nil, err
	}
	w.WithSparseBatches()
	return &IndexerService{writer: w, store: store}, nil
}

// newIndexerWriter assembles the writer over src. Tests substitute a source
// that wraps the database source and a small batch.
func newIndexerWriter(
	deps dingo.IndexerWriterDeps,
	src indexer.ChainSource,
	batchBlocks int,
) (*indexer.Writer, *indexerdb.Store, error) {
	store, err := indexerdb.NewStore(
		deps.Indexer, deps.Indexer, deps.MaxReadyLagBlocks,
	)
	if err != nil {
		return nil, nil, err
	}
	deriver, err := indexer.NewDeriver(deps.Indexer, indexer.DeriveConfig{
		Logger:                     deps.Logger,
		NodeConfig:                 deps.NodeConfig,
		MinPoolMargin:              deps.MinPoolMargin,
		PledgeLeverageEnabled:      deps.PledgeLeverageEnabled,
		PledgeLeverage:             deps.PledgeLeverage,
		FullPotRewardsEnabled:      deps.FullPotRewardsEnabled,
		DelegatorInactivityEnabled: deps.DelegatorInactivityEnabled,
		DelegatorInactivity:        deps.DelegatorInactivity,
		WarmGenesisStake:           deps.WarmGenesisStake,
	})
	if err != nil {
		return nil, nil, err
	}
	metrics, err := indexer.NewMetrics(deps.PromRegistry, deps.SlotTime)
	if err != nil {
		return nil, nil, fmt.Errorf("register indexer metrics: %w", err)
	}
	bf := NewBackfill(deps.Indexer, deps.NodeConfig, deps.Logger)
	bf.SetDelegatorInactivityEnabled(deps.DelegatorInactivityEnabled)
	bf.SetDelegatorInactivityWindow(deps.DelegatorInactivity)
	// The ledger stores the endorser-block data certified closures are read
	// from; the IndexerDB sees only its own offsets.
	bf.SetClosureSource(ledger.NewDatabaseClosureSource(deps.Consensus))
	w := indexer.NewWriter(
		store, src, bf, deps.Logger, batchBlocks,
	).
		WithDeriver(deriver).
		WithGenesisSeed(bf.SeedGenesis).
		WithProgress(metrics.Observe).
		WithRollbackAncestorLost(metrics.NoRollbackAncestor).
		WithFailureReport(metrics.RepeatedFailures).
		WithFatal(deps.Fatal)
	return w, store, nil
}

// Store returns the IndexerDB's tip and readiness store.
func (s *IndexerService) Store() *indexerdb.Store { return s.store }

// Start runs the writer until Stop. It catches up from the IndexerDB's own
// tip, which also covers blocks the ledger applied before the writer existed.
func (s *IndexerService) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return errors.New("indexer service already started")
	}
	s.started = true
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		err := s.writer.Run(runCtx)
		if !errors.Is(err, context.Canceled) {
			s.mu.Lock()
			s.runErr = err
			s.mu.Unlock()
		}
	}()
	return nil
}

// RollbackTo implements dingo.IndexerWriter.
func (s *IndexerService) RollbackTo(
	ctx context.Context,
	target indexerdb.Tip,
) error {
	return s.writer.RollbackTo(ctx, target)
}

// Wake implements dingo.IndexerWriter.
func (s *IndexerService) Wake() { s.writer.Wake() }

// Stop cancels the writer and waits for it to leave, or for ctx to end. A
// batch in flight is rolled back with its transaction, never half-committed.
func (s *IndexerService) Stop(ctx context.Context) error {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("indexer writer did not stop: %w", ctx.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runErr
}
