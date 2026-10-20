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
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// CertifiedEndorserTransactions returns the endorser-block transactions a
// certifying ranking block applied, in endorser-block order. A certifying
// ranking block carries no transactions of its own; its transactions are
// those of the endorser block its parent announced, whose ledger rows are
// stored under the ranking block's hash (applyEndorserBlock). The raw
// transactions come from the persisted endorser-block transaction list
// (database.GetLeiosEBTxs) and are kept only when a transaction row with
// the same hash is stored under block, so a transaction the ledger skipped
// as already on chain is not reported twice. A row's block index is the
// transaction's endorser-block position, counting stored sub-transaction
// rows as for any block (LedgerDelta).
//
// ok is false when block does not certify an endorser block, or when its
// endorser transactions are unavailable: the persisted list is absent
// (pruned under a non-zero Leios persistence retention, or never stored) or
// the parent block is no longer held. Callers then use the block body. A
// block whose history was imported without transaction rows (a Mithril
// restore) reports none, as its body does.
func (ls *LedgerState) CertifiedEndorserTransactions(
	ctx context.Context,
	block ledger.Block,
) (txs []lcommon.Transaction, ok bool, err error) {
	if block == nil {
		return nil, false, nil
	}
	ebHash, ebSlot, announced, err := ls.leiosEndorserBlockForApply(
		ctx,
		block,
	)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !announced {
		return nil, false, nil
	}
	rawTxs, err := ls.db.GetLeiosEBTxs(ebHash.Bytes(), ebSlot)
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf(
			"get endorser block %x transactions: %w",
			ebHash.Bytes(),
			err,
		)
	}
	rows, err := ls.db.GetTransactionsByBlockHash(
		ctx,
		block.Hash().Bytes(),
		nil,
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get transactions for block %x: %w",
			block.Hash().Bytes(),
			err,
		)
	}
	stored := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		stored[string(row.Hash)] = struct{}{}
	}
	ret := make([]lcommon.Transaction, 0, len(rawTxs))
	for idx, raw := range rawTxs {
		txCbor, _, err := decodeEndorserTxEnvelope(raw)
		if err != nil {
			return nil, false, fmt.Errorf(
				"endorser block %x tx %d: %w",
				ebHash.Bytes(),
				idx,
				err,
			)
		}
		tx, err := safedecode.DijkstraBlockTransaction(txCbor)
		if err != nil {
			return nil, false, fmt.Errorf(
				"decode endorser block %x tx %d: %w",
				ebHash.Bytes(),
				idx,
				err,
			)
		}
		hashKey := string(tx.Hash().Bytes())
		if _, found := stored[hashKey]; !found {
			continue
		}
		// A repeated transaction is applied once, at its first position.
		delete(stored, hashKey)
		ret = append(ret, tx)
	}
	return ret, true, nil
}
