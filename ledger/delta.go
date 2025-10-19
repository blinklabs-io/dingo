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
	"maps"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type TransactionRecord struct {
	Tx    lcommon.Transaction
	Index uint32
}

type LedgerDelta struct {
	PParamUpdates     map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate
	Point             ocommon.Point
	Produced          []lcommon.Utxo
	Consumed          []lcommon.TransactionInput
	Certificates      []lcommon.Certificate
	Transactions      []TransactionRecord
	PParamUpdateEpoch uint64
}

//
//nolint:unparam
func (d *LedgerDelta) processTransaction(
	tx lcommon.Transaction,
	index uint32,
) error {
	// Consumed UTxOs
	d.Consumed = slices.Concat(d.Consumed, tx.Consumed())
	// Produced UTxOs
	d.Produced = slices.Concat(d.Produced, tx.Produced())
	// Collect transaction
	d.Transactions = append(
		d.Transactions,
		TransactionRecord{Tx: tx, Index: index},
	)
	// Stop processing transaction if it's marked as invalid
	// This allows us to capture collateral returns in the case of phase 2 validation failure
	if !tx.IsValid() {
		return nil
	}
	// Protocol parameter updates
	if updateEpoch, paramUpdates := tx.ProtocolParameterUpdates(); updateEpoch > 0 {
		d.PParamUpdateEpoch = updateEpoch
		if d.PParamUpdates == nil {
			d.PParamUpdates = make(
				map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
			)
		}
		maps.Copy(d.PParamUpdates, paramUpdates)
	}
	// Certificates
	d.Certificates = slices.Concat(d.Certificates, tx.Certificates())
	return nil
}

func (d *LedgerDelta) apply(ls *LedgerState, txn *database.Txn) error {
	for _, tr := range d.Transactions {
		inputsCbor, err := cbor.Encode(tr.Tx.Consumed())
		if err != nil {
			return fmt.Errorf("encode transaction inputs: %w", err)
		}
		outputsCbor, err := cbor.Encode(tr.Tx.Produced())
		if err != nil {
			return fmt.Errorf("encode transaction outputs: %w", err)
		}
		err = ls.db.NewTransaction(
			tr.Tx.Hash().Bytes(),
			strconv.Itoa(tr.Tx.Type()),
			d.Point.Hash,
			tr.Index,
			inputsCbor,
			outputsCbor,
			txn,
		)
		if err != nil {
			return fmt.Errorf("record transaction: %w", err)
		}
	}
	// Process produced UTxOs
	for _, produced := range d.Produced {
		outAddr := produced.Output.Address()
		err := ls.db.NewUtxo(
			produced.Id.Id().Bytes(),
			produced.Id.Index(),
			d.Point.Slot,
			outAddr.PaymentKeyHash().Bytes(),
			outAddr.StakeKeyHash().Bytes(),
			produced.Output.Cbor(),
			produced.Output.Amount(),
			produced.Output.Assets(),
			txn,
		)
		if err != nil {
			return fmt.Errorf("add produced UTxO: %w", err)
		}
	}
	// Process consumed UTxOs
	for _, consumed := range d.Consumed {
		if err := ls.consumeUtxo(txn, consumed, d.Point.Slot); err != nil {
			return fmt.Errorf("remove consumed UTxO: %w", err)
		}
	}
	// Protocol parameter updates
	for genesisHash, update := range d.PParamUpdates {
		err := ls.db.SetPParamUpdate(
			genesisHash.Bytes(),
			update.Cbor(),
			d.Point.Slot,
			d.PParamUpdateEpoch,
			txn,
		)
		if err != nil {
			return fmt.Errorf("set pparam update: %w", err)
		}
	}
	// Certificates
	if err := ls.processTransactionCertificates(txn, d.Point, d.Certificates); err != nil {
		return fmt.Errorf("process transaction certificates: %w", err)
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
		for _, tr := range delta.Transactions {
			inputsCbor, err := cbor.Encode(tr.Tx.Consumed())
			if err != nil {
				return fmt.Errorf("encode transaction inputs: %w", err)
			}
			outputsCbor, err := cbor.Encode(tr.Tx.Produced())
			if err != nil {
				return fmt.Errorf("encode transaction outputs: %w", err)
			}
			err = ls.db.NewTransaction(
				tr.Tx.Hash().Bytes(),
				strconv.Itoa(tr.Tx.Type()),
				delta.Point.Hash,
				tr.Index,
				inputsCbor,
				outputsCbor,
				txn,
			)
			if err != nil {
				return fmt.Errorf("record transaction: %w", err)
			}
		}
	}
	// Produced UTxOs with transaction IDs
	produced := make([]models.UtxoSlot, 0, 100)
	for _, delta := range b.deltas {
		for _, utxo := range delta.Produced {
			produced = append(
				produced,
				models.UtxoSlot{
					Slot: delta.Point.Slot,
					Utxo: utxo,
				},
			)
		}
	}
	if len(produced) > 0 {
		// Process in groups of 1000 to avoid hitting DB limits
		for i := 0; i < len(produced); i += 1000 {
			end := min(
				len(produced),
				i+1000,
			)
			batch := produced[i:end]
			if err := ls.db.AddUtxos(batch, txn); err != nil {
				return err
			}
		}
	}
	for _, delta := range b.deltas {
		// Process consumed UTxOs
		for _, consumed := range delta.Consumed {
			if err := ls.consumeUtxo(txn, consumed, delta.Point.Slot); err != nil {
				return fmt.Errorf("remove consumed UTxO: %w", err)
			}
		}
		// Protocol parameter updates
		for genesisHash, update := range delta.PParamUpdates {
			err := ls.db.SetPParamUpdate(
				genesisHash.Bytes(),
				update.Cbor(),
				delta.Point.Slot,
				delta.PParamUpdateEpoch,
				txn,
			)
			if err != nil {
				return fmt.Errorf("set pparam update: %w", err)
			}
		}
		// Certificates
		if err := ls.processTransactionCertificates(txn, delta.Point, delta.Certificates); err != nil {
			return fmt.Errorf("process transaction certificates: %w", err)
		}
	}
	return nil
}
