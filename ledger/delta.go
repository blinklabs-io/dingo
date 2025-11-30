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
	"math"

	"github.com/blinklabs-io/dingo/database"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type TransactionRecord struct {
	Tx    lcommon.Transaction
	Index int
}

type LedgerDelta struct {
	Point        ocommon.Point
	BlockEraId   uint
	Transactions []TransactionRecord
}

func (d *LedgerDelta) addTransaction(
	tx lcommon.Transaction,
	index int,
) {
	// Collect transaction
	d.Transactions = append(
		d.Transactions,
		TransactionRecord{Tx: tx, Index: index},
	)
}

func (d *LedgerDelta) apply(ls *LedgerState, txn *database.Txn) error {
	for _, tr := range d.Transactions {
		if tr.Index < 0 || tr.Index > math.MaxUint32 {
			return fmt.Errorf("transaction index out of range: %d", tr.Index)
		}
		// Extract protocol parameter updates
		updateEpoch, paramUpdates := tr.Tx.ProtocolParameterUpdates()

		// Calculate certificate deposits
		certs := tr.Tx.Certificates()
		certDeposits := make(map[int]uint64)
		for i, cert := range certs {
			deposit, err := ls.calculateCertificateDeposit(cert, d.BlockEraId)
			if err != nil {
				return fmt.Errorf("calculate certificate deposit: %w", err)
			}
			certDeposits[i] = deposit
		}

		err := ls.db.SetTransaction(
			tr.Tx,
			d.Point,
			uint32(tr.Index), //nolint:gosec
			updateEpoch,
			paramUpdates,
			certDeposits,
			txn,
		)
		if err != nil {
			return fmt.Errorf("record transaction: %w", err)
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
