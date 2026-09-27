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
	"math/big"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

type transactionBodyLevel struct {
	common.Transaction
	body             common.TransactionBody
	witnesses        common.TransactionWitnessSet
	metadata         common.TransactionMetadatum
	hasBodyWitnesses bool
	hasBodyMetadata  bool
}

func (t transactionBodyLevel) Cbor() []byte { return t.body.Cbor() }
func (t transactionBodyLevel) Hash() common.Blake2b256 {
	return t.body.Id()
}
func (t transactionBodyLevel) Id() common.Blake2b256 { return t.body.Id() }
func (t transactionBodyLevel) Metadata() common.TransactionMetadatum {
	if t.hasBodyMetadata {
		return t.metadata
	}
	return t.Transaction.Metadata()
}

func (t transactionBodyLevel) Witnesses() common.TransactionWitnessSet {
	if t.hasBodyWitnesses {
		return t.witnesses
	}
	return t.Transaction.Witnesses()
}
func (t transactionBodyLevel) Fee() *big.Int { return t.body.Fee() }
func (t transactionBodyLevel) Inputs() []common.TransactionInput {
	return t.body.Inputs()
}

func (t transactionBodyLevel) Outputs() []common.TransactionOutput {
	return t.body.Outputs()
}
func (t transactionBodyLevel) TTL() uint64 { return t.body.TTL() }
func (t transactionBodyLevel) ProtocolParameterUpdates() (
	uint64,
	map[common.Blake2b224]common.ProtocolParameterUpdate,
) {
	return t.body.ProtocolParameterUpdates()
}

func (t transactionBodyLevel) ValidityIntervalStart() uint64 {
	return t.body.ValidityIntervalStart()
}

func (t transactionBodyLevel) ReferenceInputs() []common.TransactionInput {
	return t.body.ReferenceInputs()
}

func (t transactionBodyLevel) Collateral() []common.TransactionInput {
	return t.body.Collateral()
}

func (t transactionBodyLevel) CollateralReturn() common.TransactionOutput {
	return t.body.CollateralReturn()
}

func (t transactionBodyLevel) TotalCollateral() *big.Int {
	return t.body.TotalCollateral()
}

func (t transactionBodyLevel) Certificates() []common.Certificate {
	return t.body.Certificates()
}

func (t transactionBodyLevel) Withdrawals() map[*common.Address]*big.Int {
	return t.body.Withdrawals()
}

func (t transactionBodyLevel) AuxDataHash() *common.Blake2b256 {
	return t.body.AuxDataHash()
}

func (t transactionBodyLevel) RequiredSigners() []common.Blake2b224 {
	return t.body.RequiredSigners()
}

func (t transactionBodyLevel) AssetMint() *common.MultiAsset[common.MultiAssetTypeMint] {
	return t.body.AssetMint()
}

func (t transactionBodyLevel) ScriptDataHash() *common.Blake2b256 {
	return t.body.ScriptDataHash()
}

func (t transactionBodyLevel) VotingProcedures() common.VotingProcedures {
	return t.body.VotingProcedures()
}

func (t transactionBodyLevel) ProposalProcedures() []common.ProposalProcedure {
	return t.body.ProposalProcedures()
}

func (t transactionBodyLevel) CurrentTreasuryValue() *big.Int {
	return t.body.CurrentTreasuryValue()
}
func (t transactionBodyLevel) Donation() *big.Int { return t.body.Donation() }
func (t transactionBodyLevel) Consumed() []common.TransactionInput {
	if !t.IsValid() {
		return t.Transaction.Consumed()
	}
	return t.body.Inputs()
}

func (t transactionBodyLevel) Produced() []common.Utxo {
	if !t.IsValid() {
		return t.Transaction.Produced()
	}
	outputs := t.body.Outputs()
	produced := make([]common.Utxo, 0, len(outputs))
	for idx, output := range outputs {
		produced = append(produced, common.Utxo{
			Id:     shelley.NewShelleyTransactionInput(t.body.Id().String(), idx),
			Output: output,
		})
	}
	return produced
}

func (t transactionBodyLevel) DijkstraDirectDeposits() dijkstra.DijkstraDirectDeposits {
	switch body := t.body.(type) {
	case *dijkstra.DijkstraTransactionBody:
		return body.TxDirectDeposits
	case *dijkstra.DijkstraSubTransactionBody:
		return body.TxDirectDeposits
	case *dijkstra.DijkstraTransaction:
		return body.Body.TxDirectDeposits
	default:
		return nil
	}
}

// TransactionLevels returns the Dijkstra sub-transaction bodies in encoded
// order followed by the enclosing transaction body. Each level exposes its
// own body hash, witnesses, metadata, and ledger effects while retaining the
// validity of the enclosing transaction.
func TransactionLevels(tx common.Transaction) []common.Transaction {
	if tx == nil {
		return nil
	}
	bodies := common.SubTransactionBodiesFromTransaction(tx)
	if len(bodies) == 0 {
		return []common.Transaction{tx}
	}
	witnessSets := common.SubTransactionWitnessSetsFromTransaction(tx)
	var subMetadata []common.TransactionMetadatum
	if dijkstraTx, ok := tx.(*dijkstra.DijkstraTransaction); ok {
		subTransactions := dijkstraTx.Body.TxSubTransactions.Items()
		subMetadata = make([]common.TransactionMetadatum, len(subTransactions))
		for idx := range subTransactions {
			subMetadata[idx] = subTransactions[idx].TxMetadata
		}
	}
	levels := make([]common.Transaction, 0, len(bodies)+1)
	for idx, body := range bodies {
		level := transactionBodyLevel{
			Transaction:     tx,
			body:            body,
			hasBodyMetadata: true,
		}
		if idx < len(witnessSets) {
			level.witnesses = witnessSets[idx]
			level.hasBodyWitnesses = true
		}
		if idx < len(subMetadata) {
			level.metadata = subMetadata[idx]
		}
		levels = append(levels, level)
	}
	return append(levels, transactionBodyLevel{Transaction: tx, body: tx})
}

// TransactionLevelsForApply excludes Dijkstra sub-transactions when the
// enclosing transaction is invalid; only its collateral effects are applied.
func TransactionLevelsForApply(tx common.Transaction) []common.Transaction {
	levels := TransactionLevels(tx)
	if tx != nil && !tx.IsValid() && len(levels) > 1 {
		return levels[len(levels)-1:]
	}
	return levels
}

var _ common.Transaction = transactionBodyLevel{}
