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
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// IndexerService runs the split layout's writer: a DatabaseSource over the
// ledger database, the Backfill row writer over the IndexerDB, the ledger
// facts copy, and the genesis seed.
type IndexerService struct {
	writer *indexer.Writer
	store  *indexerdb.Store
	// ledgerless is set for a service given no ledger facts: it can roll the
	// IndexerDB back and cannot follow the ledger.
	ledgerless bool

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
	// outputs the reference translation drops, which a replay toward a
	// Mithril anchor would then disagree with. They go before the writer
	// reads the IndexerDB. The seed counted their genesis transactions into
	// the replay's count, which the anchor check compares with the
	// applied_transaction rows, so the count loses them with the rows.
	zero, err := ledger.ZeroValueByronGenesisOutputs(deps.NodeConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"derive zero-value Byron genesis outputs: %w",
			err,
		)
	}
	replayState, err := indexerdb.NewStore(
		deps.Indexer, deps.Indexer, deps.MaxReadyLagBlocks,
	)
	if err != nil {
		return nil, err
	}
	if err := ledger.RepairZeroValueByronGenesis(
		context.Background(), deps.Indexer, zero, deps.Logger,
		replayState.RemoveReplayTransactions,
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
	// Recorded before the factory returns, so no reader finds the IndexerDB
	// ready for a chain whose anchor it has not been told about. A single
	// small write.
	if err := w.Prepare(context.Background()); err != nil {
		return nil, fmt.Errorf("record indexer anchor: %w", err)
	}
	return &IndexerService{
		writer: w, store: store, ledgerless: deps.FactRows == nil,
	}, nil
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
	facts, err := indexer.NewMetadataFacts(deps.FactRows, deps.Indexer)
	if err != nil {
		return nil, nil, err
	}
	metrics, err := indexer.NewMetrics(deps.PromRegistry, deps.SlotTime)
	if err != nil {
		return nil, nil, fmt.Errorf("register indexer metrics: %w", err)
	}
	bf := NewBackfill(deps.Indexer, deps.NodeConfig, deps.Logger)
	bf.SetDelegatorInactivityEnabled(deps.DelegatorInactivityEnabled)
	// The ledger stores the endorser-block data certified closures are read
	// from; the IndexerDB sees only its own offsets.
	bf.SetClosureSource(ledger.NewDatabaseClosureSource(deps.Consensus))
	w := indexer.NewWriter(
		store, src, bf, deps.Logger, batchBlocks,
	).
		WithFacts(facts).
		WithGenesisSeed(bf.SeedGenesis).
		WithProgress(func(tip indexerdb.Tip, ledgerTip ochainsync.Tip) {
			metrics.Observe(tip, ledgerTip)
			if deps.Moved != nil {
				deps.Moved()
			}
		}).
		WithRollbackAncestorLost(metrics.NoRollbackAncestor).
		WithFailureReport(metrics.RepeatedFailures).
		WithDeferredIndexes(
			func(ctx context.Context) error {
				NewDeferredIndexRebuilder(ctx, deps.Indexer, deps.Logger).Drop()
				return nil
			},
			func(ctx context.Context) error {
				return NewDeferredIndexRebuilder(
					ctx, deps.Indexer, deps.Logger,
				).BuildAll()
			},
		)
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
	if s.ledgerless {
		return indexer.ErrNoLedger
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
