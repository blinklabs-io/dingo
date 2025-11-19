// Copyright 2025 Blink Labs Software
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
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type TransactionRecord struct {
	Tx       lcommon.Transaction
	Index    int
	Deposits map[int]uint64
}

type LedgerDelta struct {
	Point        ocommon.Point
	BlockEraId   uint
	Transactions []TransactionRecord
}

func (d *LedgerDelta) addTransactionWithDeposits(
	tx lcommon.Transaction,
	index int,
	deposits map[int]uint64,
) {
	// Collect transaction with deposits
	d.Transactions = append(
		d.Transactions,
		TransactionRecord{Tx: tx, Index: index, Deposits: deposits},
	)
}

func (d *LedgerDelta) apply(ls *LedgerState, txn *database.Txn) error {
	for _, tr := range d.Transactions {
		// Use consumeUtxo if tx is marked invalid
		// This allows us to capture collateral returns in the case of
		// phase 2 validation failure
		if !tr.Tx.IsValid() {
			// Process consumed UTxOs
			for _, consumed := range tr.Tx.Consumed() {
				if err := ls.consumeUtxo(txn, consumed, d.Point.Slot); err != nil {
					return fmt.Errorf("remove consumed UTxO: %w", err)
				}
			}
			// No deposit calculation for invalid transactions
			var deposits map[int]uint64
			err := ls.db.SetTransaction(
				d.Point,
				tr.Tx,
				uint32(tr.Index), //nolint:gosec
				deposits,
				txn,
			)
			if err != nil {
				return fmt.Errorf("record invalid transaction: %w", err)
			}
			// Stop processing this transaction
			continue
		}
		// Use pre-calculated deposits if available, otherwise calculate them
		var deposits map[int]uint64
		if tr.Deposits != nil {
			deposits = tr.Deposits
		} else if len(tr.Tx.Certificates()) > 0 {
			var err error
			deposits, err = ls.CalculateCertificateDeposits(
				tr.Tx.Certificates(),
			)
			if err != nil {
				return fmt.Errorf("calculate certificate deposits: %w", err)
			}
		}

		// Store transaction (with or without certificates)
		err := ls.db.SetTransaction(
			d.Point,
			tr.Tx,
			uint32(tr.Index), //nolint:gosec
			deposits,
			txn,
		)
		if err != nil {
			return fmt.Errorf("record transaction: %w", err)
		}

		// Protocol parameter updates
		if updateEpoch, paramUpdates := tr.Tx.ProtocolParameterUpdates(); updateEpoch > 0 {
			for genesisHash, update := range paramUpdates {
				err := ls.db.SetPParamUpdate(
					genesisHash.Bytes(),
					update.Cbor(),
					d.Point.Slot,
					updateEpoch,
					txn,
				)
				if err != nil {
					return fmt.Errorf("set pparam update: %w", err)
				}
			}
		}
	}
	return nil
}

type LedgerDeltaBatch struct {
	deltas []*LedgerDelta
}

func (b *LedgerDeltaBatch) addDelta(delta *LedgerDelta) {
	b.deltas = append(b.deltas, delta)
}

func (b *LedgerDeltaBatch) apply(ls *LedgerState, txn *database.Txn) error {
	for _, delta := range b.deltas {
		err := delta.apply(ls, txn)
		if err != nil {
			return err
		}
	}
	return nil
}
