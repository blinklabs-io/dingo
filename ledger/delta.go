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
			d.PParamUpdates = make(map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate)
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
	// Process consumed UTxOs
	for _, consumed := range d.Consumed {
		if err := ls.consumeUtxo(txn, consumed, d.Point.Slot); err != nil {
			return fmt.Errorf("remove consumed UTxO: %w", err)
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
			txn,
		)
		if err != nil {
			return fmt.Errorf("add produced UTxO: %w", err)
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
