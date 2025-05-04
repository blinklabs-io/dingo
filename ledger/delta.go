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
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type LedgerDelta struct {
	Point             ocommon.Point
	Produced          []lcommon.Utxo
	Consumed          []lcommon.TransactionInput
	PParamUpdateEpoch uint64
	PParamUpdates     map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate
	Certificates      []lcommon.Certificate
}

// nolint:unparam
func (d *LedgerDelta) processTransaction(tx lcommon.Transaction) error {
	// Consumed UTxOs
	d.Consumed = slices.Concat(d.Consumed, tx.Consumed())
	// Produced UTxOs
	d.Produced = slices.Concat(d.Produced, tx.Produced())
	// Protocol parameter updates
	if updateEpoch, paramUpdates := tx.ProtocolParameterUpdates(); updateEpoch > 0 {
		d.PParamUpdateEpoch = updateEpoch
		if d.PParamUpdates == nil {
			d.PParamUpdates = make(
				map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
			)
		}
		for genesisHash, update := range paramUpdates {
			d.PParamUpdates[genesisHash] = update
		}
	}
	// Certificates
	d.Certificates = slices.Concat(d.Certificates, tx.Certificates())
	return nil
}

func (d *LedgerDelta) apply(ls *LedgerState, txn *database.Txn) error {
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
	// Produced
	produced := make([]types.UtxoSlot, 0, 100)
	for _, delta := range b.deltas {
		for _, utxo := range delta.Produced {
			produced = append(
				produced,
				types.UtxoSlot{
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
