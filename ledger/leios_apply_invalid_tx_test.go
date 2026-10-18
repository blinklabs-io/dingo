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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// endorserBlockTxWithValidity returns the fixture transaction as an endorser
// block entry, [body, witnesses, auxiliary_data, is_valid].
func endorserBlockTxWithValidity(
	t *testing.T,
	fx *dijkstraCollateralReturnFixture,
	isValid bool,
) cbor.RawMessage {
	t.Helper()
	var parts []cbor.RawMessage
	_, err := cbor.Decode(fx.txCbor, &parts)
	require.NoError(t, err)
	require.Len(t, parts, 3)
	validity, err := cbor.Encode(isValid)
	require.NoError(t, err)
	raw, err := cbor.Encode(append(parts, cbor.RawMessage(validity)))
	require.NoError(t, err)
	return raw
}

// withoutTxValidation drops per-transaction rule checks. The rules reject an
// invalid transaction that carries no Plutus script, and evaluating a failing
// script is not what these tests exercise: they cover how the apply path treats
// the validity flag once a transaction is certified.
func withoutTxValidation(fx *dijkstraCollateralReturnFixture) {
	era := fx.ls.currentEra
	era.ValidateTxFunc = nil
	fx.ls.currentEra = era
	fx.ls.activeEras = []eras.EraDesc{era}
	fx.ls.publishSnapshotsLocked()
}

func applyFixtureEndorserBlock(
	t *testing.T,
	fx *dijkstraCollateralReturnFixture,
	raw cbor.RawMessage,
) {
	t.Helper()
	ebHash := lcommon.Blake2b256Hash([]byte("endorser block")).Bytes()
	rbPoint := ocommon.NewPoint(dijkstraCollateralReturnTestSlot, ebHash)
	require.NoError(
		t,
		fx.db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
			_, _, err := fx.ls.applyEndorserBlockInContext(
				t.Context(),
				txn,
				rbPoint,
				1,
				dijkstraCollateralReturnTestSlot-1,
				ebHash,
				[]cbor.RawMessage{raw},
				nil,
			)
			return err
		}),
	)
}

func requireFixtureUtxo(
	t *testing.T,
	fx *dijkstraCollateralReturnFixture,
	txId []byte,
	index uint32,
	want bool,
	msg string,
) {
	t.Helper()
	utxo, err := fx.db.Metadata().GetUtxo(txId, index, nil)
	require.NoError(t, err)
	if want {
		require.NotNil(t, utxo, msg)
	} else {
		require.Nil(t, utxo, msg)
	}
}

// A certified endorser transaction marked invalid spends its collateral and
// creates only its collateral return; its regular inputs and outputs are not
// touched.
func TestApplyEndorserBlockInvalidTransactionAppliesCollateralOnly(
	t *testing.T,
) {
	t.Parallel()

	fx := newDijkstraCollateralReturnFixture(t, 0)
	withoutTxValidation(fx)
	txId := fx.tx.Hash().Bytes()
	applyFixtureEndorserBlock(
		t, fx, endorserBlockTxWithValidity(t, fx, false),
	)

	requireFixtureUtxo(t, fx, fx.inputIds[0], 0, true,
		"regular input of an invalid transaction must stay unspent")
	requireFixtureUtxo(t, fx, fx.inputIds[1], 0, false,
		"collateral input of an invalid transaction must be spent")
	requireFixtureUtxo(t, fx, txId, 0, false,
		"regular output of an invalid transaction must not exist")
	requireFixtureUtxo(t, fx, txId, 1, true,
		"collateral return must be created after the regular outputs")
}

func TestApplyEndorserBlockValidTransactionAppliesRegularEffects(t *testing.T) {
	t.Parallel()

	fx := newDijkstraCollateralReturnFixture(t, 0)
	txId := fx.tx.Hash().Bytes()
	applyFixtureEndorserBlock(
		t, fx, endorserBlockTxWithValidity(t, fx, true),
	)

	requireFixtureUtxo(t, fx, fx.inputIds[0], 0, false,
		"regular input of a valid transaction must be spent")
	requireFixtureUtxo(t, fx, fx.inputIds[1], 0, true,
		"collateral input of a valid transaction must stay unspent")
	requireFixtureUtxo(t, fx, txId, 0, true,
		"regular output of a valid transaction must exist")
	requireFixtureUtxo(t, fx, txId, 1, false,
		"collateral return must not exist for a valid transaction")
}
