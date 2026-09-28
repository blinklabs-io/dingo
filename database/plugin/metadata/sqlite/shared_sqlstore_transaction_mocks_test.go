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

package sqlite

import (
	"math/big"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type mockTransaction struct {
	certificates []lcommon.Certificate
	hash         lcommon.Blake2b256
	isValid      bool
	metadata     lcommon.TransactionMetadatum
	produced     []lcommon.Utxo
	inputs       []lcommon.TransactionInput
	consumed     []lcommon.TransactionInput
	collateral   []lcommon.TransactionInput
	refInputs    []lcommon.TransactionInput
	outputs      []lcommon.TransactionOutput
	collReturn   lcommon.TransactionOutput
	withdrawals  map[*lcommon.Address]*big.Int
	mint         *lcommon.MultiAsset[lcommon.MultiAssetTypeMint]
}

func (m *mockTransaction) Hash() lcommon.Blake2b256 { return m.hash }
func (m *mockTransaction) Id() lcommon.Blake2b256   { return m.hash }
func (m *mockTransaction) Type() int                { return 0 }
func (m *mockTransaction) Fee() *big.Int            { return big.NewInt(1000) }
func (m *mockTransaction) TTL() uint64              { return 1000000 }
func (m *mockTransaction) IsValid() bool            { return m.isValid }
func (m *mockTransaction) Metadata() lcommon.TransactionMetadatum {
	return m.metadata
}
func (m *mockTransaction) AuxiliaryData() lcommon.AuxiliaryData { return nil }
func (m *mockTransaction) RawAuxiliaryData() []byte             { return nil }
func (m *mockTransaction) CollateralReturn() lcommon.TransactionOutput {
	return m.collReturn
}
func (m *mockTransaction) Produced() []lcommon.Utxo { return m.produced }
func (m *mockTransaction) Outputs() []lcommon.TransactionOutput {
	return m.outputs
}

func (m *mockTransaction) Inputs() []lcommon.TransactionInput { return m.inputs }
func (m *mockTransaction) Collateral() []lcommon.TransactionInput {
	return m.collateral
}
func (m *mockTransaction) Certificates() []lcommon.Certificate {
	return m.certificates
}
func (m *mockTransaction) ProtocolParameterUpdates() (
	uint64,
	map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
) {
	return 0, nil
}

func (m *mockTransaction) AssetMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	return m.mint
}
func (m *mockTransaction) AuxDataHash() *lcommon.Blake2b256 { return nil }

func (m *mockTransaction) Cbor() []byte { return []byte("mock_cbor") }
func (m *mockTransaction) Consumed() []lcommon.TransactionInput {
	return m.consumed
}

func (m *mockTransaction) Witnesses() lcommon.TransactionWitnessSet { return nil }
func (m *mockTransaction) ValidityIntervalStart() uint64            { return 0 }
func (m *mockTransaction) ReferenceInputs() []lcommon.TransactionInput {
	return m.refInputs
}
func (m *mockTransaction) TotalCollateral() *big.Int {
	return big.NewInt(0)
}
func (m *mockTransaction) Withdrawals() map[*lcommon.Address]*big.Int {
	return m.withdrawals
}
func (m *mockTransaction) RequiredSigners() []lcommon.Blake2b224 { return nil }
func (m *mockTransaction) ScriptDataHash() *lcommon.Blake2b256   { return nil }
func (m *mockTransaction) VotingProcedures() lcommon.VotingProcedures {
	return lcommon.VotingProcedures{}
}
func (m *mockTransaction) ProposalProcedures() []lcommon.ProposalProcedure {
	return nil
}

func (m *mockTransaction) CurrentTreasuryValue() *big.Int { return big.NewInt(0) }

func (m *mockTransaction) Donation() *big.Int            { return big.NewInt(0) }
func (m *mockTransaction) Utxorpc() (*cardano.Tx, error) { return nil, nil }
func (m *mockTransaction) LeiosHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

type mockTransactionInput struct {
	hash  lcommon.Blake2b256
	index uint32
}

func (m mockTransactionInput) Id() lcommon.Blake2b256 { return m.hash }
func (m mockTransactionInput) Index() uint32          { return m.index }
func (m mockTransactionInput) String() string         { return m.hash.String() }
func (m mockTransactionInput) Utxorpc() (*cardano.TxInput, error) {
	return nil, nil
}
func (m mockTransactionInput) ToPlutusData() data.PlutusData { return nil }

type mockTransactionOutput struct {
	amount *big.Int
	// address defaults to the zero Address, which carries neither a payment
	// nor a staking credential. Set it to exercise the address-derived
	// columns of the produced utxo row -- payment/staking credential,
	// script-locked classification, and the pointer position of a pointer
	// address.
	address lcommon.Address
}

func (m *mockTransactionOutput) Address() lcommon.Address { return m.address }
func (m *mockTransactionOutput) Amount() *big.Int         { return m.amount }

func (m *mockTransactionOutput) Assets() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] {
	return nil
}
func (m *mockTransactionOutput) Datum() *lcommon.Datum          { return nil }
func (m *mockTransactionOutput) DatumHash() *lcommon.Blake2b256 { return nil }
func (m *mockTransactionOutput) Cbor() []byte                   { return nil }
func (m *mockTransactionOutput) Utxorpc() (*cardano.TxOutput, error) {
	return nil, nil
}
func (m *mockTransactionOutput) ScriptRef() lcommon.Script     { return nil }
func (m *mockTransactionOutput) ToPlutusData() data.PlutusData { return nil }
func (m *mockTransactionOutput) String() string                { return "" }

func newTestWitnessTransaction(hashSeed string) *ledger.MockTransaction {
	ws := ledger.NewMockTransactionWitnessSet().
		WithVkeyWitnesses(lcommon.VkeyWitness{
			Vkey:      make([]byte, 32),
			Signature: make([]byte, 64),
		})
	tx := ledger.NewTransactionBuilder().WithWitnesses(ws)
	tx.WithId([]byte(hashSeed))
	return tx
}
