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
	"encoding/hex"

	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
)

// captureEndorserUndo returns, for each ranking block in blocks that applied a
// certified endorser block, the endorser transactions it applied, in
// application order, by position in the endorser block. An entry already in
// prior is kept, because a block whose rollback was already recorded may be
// truncated, and the transaction rows that say which block applied each
// endorser transaction go with it.
//
// It reads the metadata store, so a rollback calls it before truncating. A
// block whose endorser block cannot be resolved is logged and skipped: the
// undo is a notification, and failing the rollback over it would leave the
// ledger wrong to keep a subscriber right.
func (ls *LedgerState) captureEndorserUndo(
	ctx context.Context,
	blocks []models.Block,
	prior endorserUndo,
) endorserUndo {
	ret := prior
	if ls.config.EventBus == nil ||
		ls.config.EndorserBlockProvider == nil ||
		!ls.config.EventBus.HasSubscribers(TransactionEventType) {
		return ret
	}
	for _, block := range blocks {
		if block.Type != gledger.BlockTypeDijkstra {
			continue
		}
		if _, ok := ret[string(block.Hash)]; ok {
			continue
		}
		applied, err := ls.endorserTxsAppliedBy(ctx, block)
		if err != nil {
			ls.config.Logger.Error(
				"failed to capture endorser transactions for rollback undo",
				"component", "ledger",
				"error", err,
				"slot", block.Slot,
				"hash", hex.EncodeToString(block.Hash),
			)
			continue
		}
		if len(applied.txs) == 0 {
			continue
		}
		if ret == nil {
			ret = make(endorserUndo)
		}
		ret[string(block.Hash)] = applied
	}
	return ret
}

// endorserTxsAppliedBy returns the transactions of the endorser block that
// block certifies which block itself applied. Apply skips a transaction an
// earlier block already applied, and records the rest under the ranking
// block's hash, so the rows decide.
func (ls *LedgerState) endorserTxsAppliedBy(
	ctx context.Context,
	block models.Block,
) (endorserApplied, error) {
	blk, err := block.Decode()
	if err != nil {
		return endorserApplied{}, err
	}
	ebHash, ebSlot, announced, err := ls.leiosEndorserBlockForApply(ctx, blk)
	if err != nil || !announced {
		return endorserApplied{}, err
	}
	raws, ok := ls.config.EndorserBlockProvider(ebHash.Bytes(), ebSlot)
	if !ok {
		return endorserApplied{}, errCertifiedEndorserBlockUnavailable
	}
	ids, err := endorserBlockTxIds(raws)
	if err != nil {
		return endorserApplied{}, err
	}
	rows, err := ls.ledgerDB().GetTransactionsByHashes(ctx, ids, nil)
	if err != nil {
		return endorserApplied{}, err
	}
	appliedHere := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if bytes.Equal(row.BlockHash, block.Hash) {
			appliedHere[string(row.Hash)] = struct{}{}
		}
	}
	ret := endorserApplied{ebHash: ebHash.Bytes(), ebSlot: ebSlot}
	for i, id := range ids {
		if _, ok := appliedHere[string(id)]; !ok {
			continue
		}
		// An endorser block can repeat a transaction; apply keeps the first.
		delete(appliedHere, string(id))
		ret.txs = append(ret.txs, durableEndorserTx{
			Index: uint32(i), //nolint:gosec // bounded by the endorser block
		})
	}
	return ret, nil
}
