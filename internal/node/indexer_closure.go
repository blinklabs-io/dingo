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
	"math"

	"github.com/blinklabs-io/dingo/database"
	dledger "github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// SetClosureSource sets where IndexBlocks reads the certified endorser blocks
// of certifying ranking blocks from. The default is the backfill's own
// database, which reads through to the ledger's blob store on the split
// layout.
func (b *Backfill) SetClosureSource(src dledger.CertifiedClosureSource) {
	b.closures = src
}

func (b *Backfill) closureSource() dledger.CertifiedClosureSource {
	if b.closures != nil {
		return b.closures
	}
	return dledger.NewDatabaseClosureSource(b.db)
}

// indexClosure writes the rows the ledger writes when it applies a certified
// closure with its ranking block: the transactions its manifest lists, in
// manifest order, except those already stored. Their offsets point into the
// endorser-block record the ledger stored, in the layout it chose; they are written here under the
// IndexerDB's own keys.
//
// contextSlot is the slot the ledger applies the closure at when it is
// applied ahead of the epoch boundary its ranking block starts, which is the
// ranking block's parent. epochId, eraId and pp are those of the same
// position.
func (b *Backfill) indexClosure(
	ctx context.Context,
	txn *database.Txn,
	acc database.BatchAccumulator,
	closure *dledger.CertifiedClosure,
	point ocommon.Point,
	epochId uint64,
	eraId uint,
	pp lcommon.ProtocolParameters,
	contextSlot *uint64,
) error {
	if len(closure.RawTransactions()) == 0 {
		return nil
	}
	txs, err := closure.Decode()
	if err != nil {
		return err
	}
	bodies, err := closure.BodyCbors()
	if err != nil {
		return err
	}
	// Transactions an earlier block of this batch stored are still queued in
	// acc, so they are flushed before the lookup.
	if err := b.db.FlushBatch(acc, txn); err != nil {
		return fmt.Errorf("flushing indexed rows: %w", err)
	}
	keep, err := b.closureTransactionsToApply(ctx, txn, txs)
	if err != nil {
		return err
	}
	// The ledger keeps a standalone closure blob for an occurrence that
	// already had one, so offsets for it must follow that blob's layout
	// rather than the transaction list's.
	transactionBacking, err := b.closureSource().UsesTransactionBacking(
		closure.EndorserBlockHash.Bytes(), closure.EndorserBlockSlot,
	)
	if err != nil {
		return fmt.Errorf("inspect endorser block backing: %w", err)
	}
	var offsets *database.BlockIngestionResult
	if transactionBacking {
		offsets, err = dledger.EndorserBlockTransactionOffsets(
			txs, bodies, closure.RawTransactions(), closure.EndorserBlockSlot,
			closure.EndorserBlockHash,
		)
	} else {
		offsets, err = dledger.EndorserBlockOffsets(
			txs, bodies, closure.EndorserBlockSlot,
			closure.EndorserBlockHash,
		)
	}
	if err != nil {
		return fmt.Errorf("compute endorser block offsets: %w", err)
	}
	selected := make([]lcommon.Transaction, len(keep))
	var levels []lcommon.Transaction
	for i, idx := range keep {
		selected[i] = txs[idx]
		levels = append(levels, dledger.TransactionLevelsForApply(txs[idx])...)
	}
	if err := b.db.PersistTransactionOffsets(
		txn, levels, offsets, nil,
	); err != nil {
		return fmt.Errorf("store endorser block offsets: %w", err)
	}
	closureAcc := b.db.NewLeiosClosureBatchAccumulator()
	if closureAcc == nil {
		return errors.New(
			"metadata store cannot batch certified closures",
		)
	}
	defer closureAcc.Reset()
	var (
		protocolMajor uint64
		expanded      uint64
		windowEnd     int
	)
	if versioned, ok := pp.(lcommon.PoolRuleProtocolParameters); ok {
		protocolMajor = uint64(versioned.ProtocolMajorVersion())
	}
	for position, idx := range keep {
		if position == windowEnd {
			n, err := b.db.PrepareLeiosClosureBatch(
				closureAcc, txn, selected[position:],
			)
			if err != nil {
				return fmt.Errorf("prepare endorser UTxO batch: %w", err)
			}
			windowEnd = position + n
		}
		tx := txs[idx]
		txLevels := dledger.TransactionLevelsForApply(tx)
		childCount := uint64(len(txLevels)) - 1
		base := uint64(idx) + expanded //nolint:gosec // idx indexes a slice
		if base+childCount > math.MaxUint32 {
			return fmt.Errorf(
				"expanded transaction index out of range: %d",
				base+childCount,
			)
		}
		expanded += childCount
		for levelIndex, level := range txLevels {
			b.indexedTxs++
			storageIndex := uint32(base) + uint32(levelIndex) //nolint:gosec
			updateEpoch, paramUpdates := level.ProtocolParameterUpdates()
			if err := b.db.SetTransactionBatchedWithOpts(
				ctx, level, point, storageIndex,
				updateEpoch, paramUpdates,
				b.calculateCertDeposits(level, eraId, pp),
				offsets, closureAcc, txn,
				database.BatchedTxIngestOpts{
					SkipConsumedInputRecovery:  true,
					OffsetsPersisted:           true,
					LedgerContextSlot:          contextSlot,
					SkipWithdrawalWitnessWrite: !b.delegatorInactivityEnabled,
					DebitWithdrawals:           b.debitWithdrawals,
					ProtocolMajor:              protocolMajor,
				},
			); err != nil {
				return fmt.Errorf(
					"storing endorser transaction %d body %d: %w",
					idx, levelIndex, err,
				)
			}
			if err := b.processBlockGovernanceLevel(
				ctx, level, point, storageIndex, epochId, pp, txn,
			); err != nil {
				return fmt.Errorf(
					"governance of endorser transaction %d body %d: %w",
					idx, levelIndex, err,
				)
			}
		}
	}
	if err := b.db.FlushBatch(closureAcc, txn); err != nil {
		return fmt.Errorf("flushing endorser rows: %w", err)
	}
	return nil
}

// closureTransactionsToApply returns the indexes of the closure's
// transactions that are applied: the first occurrence of each not already
// stored.
func (b *Backfill) closureTransactionsToApply(
	ctx context.Context,
	txn *database.Txn,
	txs []lcommon.Transaction,
) ([]int, error) {
	hashes := make([][]byte, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.Hash().Bytes()
	}
	existing, err := b.db.GetTransactionsByHashes(ctx, hashes, txn)
	if err != nil {
		return nil, fmt.Errorf("dedup endorser transactions: %w", err)
	}
	skip := make(map[string]struct{}, len(existing)+len(txs))
	for _, tx := range existing {
		if len(tx.Hash) != 0 {
			skip[string(tx.Hash)] = struct{}{}
		}
	}
	keep := make([]int, 0, len(txs))
	for i, h := range hashes {
		if _, dup := skip[string(h)]; dup {
			continue
		}
		skip[string(h)] = struct{}{}
		keep = append(keep, i)
	}
	return keep, nil
}
