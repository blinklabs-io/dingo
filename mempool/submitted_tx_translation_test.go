// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mempool

import (
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

var errUntranslatedSubmission = errors.New("submission was not translated")

type submissionTranslatorValidator struct{}

type rejectingSubmissionTranslatorValidator struct {
	submissionTranslatorValidator
}

func (*submissionTranslatorValidator) TranslateSubmittedTx(
	tx gledger.Transaction,
) (gledger.Transaction, error) {
	translated, err := conway.NewConwayTransactionFromCbor(tx.Cbor())
	if err != nil {
		return nil, err
	}
	translated.SetCbor(nil)
	return translated, nil
}

func (*submissionTranslatorValidator) ValidateTx(tx gledger.Transaction) error {
	if tx.Type() != conway.EraIdConway {
		return errUntranslatedSubmission
	}
	return nil
}

func (v *submissionTranslatorValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
	_ *utxoref.StateOverlay,
) error {
	return v.ValidateTx(tx)
}

func (*rejectingSubmissionTranslatorValidator) ValidateTx(
	_ gledger.Transaction,
) error {
	return errUntranslatedSubmission
}

func (v *rejectingSubmissionTranslatorValidator) ValidateTxWithOverlay(
	tx gledger.Transaction,
	_ map[utxoref.Key]struct{},
	_ map[utxoref.Key]lcommon.Utxo,
	_ *utxoref.StateOverlay,
) error {
	return v.ValidateTx(tx)
}

func TestAddTransactionStoresTranslatedCurrentEraTx(t *testing.T) {
	t.Parallel()

	raw, err := cbor.Encode([]any{
		map[uint]any{
			0: []any{},
			1: []any{},
			2: uint64(0),
		},
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	previous, err := babbage.NewBabbageTransactionFromCbor(raw)
	require.NoError(t, err)

	m := newTestMempoolWithValidator(t, &submissionTranslatorValidator{})
	t.Cleanup(func() { require.NoError(t, m.Stop(context.Background())) })
	require.NoError(t, m.AddTransaction(
		context.Background(), uint(babbage.EraIdBabbage), raw,
	))

	stored, ok := m.GetTransaction(previous.Hash().String())
	require.True(t, ok)
	require.Equal(t, uint(conway.EraIdConway), stored.Type)
	decoded, err := conway.NewConwayTransactionFromCbor(stored.Cbor)
	require.NoError(t, err)
	require.Equal(t, previous.Hash(), decoded.Hash())

	consumer := m.AddConsumer(newTestConnectionId(6458))
	require.NotNil(t, consumer)
	relayed := consumer.NextTx(false)
	require.NotNil(t, relayed)
	require.Equal(t, uint(conway.EraIdConway), relayed.Type)
}

func TestTranslatedValidationErrorCarriesCurrentEra(t *testing.T) {
	t.Parallel()

	raw, err := cbor.Encode([]any{
		map[uint]any{0: []any{}, 1: []any{}, 2: uint64(0)},
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	m := newTestMempoolWithValidator(
		t,
		&rejectingSubmissionTranslatorValidator{},
	)
	t.Cleanup(func() { require.NoError(t, m.Stop(context.Background())) })

	err = m.AddTransaction(
		context.Background(), uint(babbage.EraIdBabbage), raw,
	)
	require.ErrorIs(t, err, errUntranslatedSubmission)
	var submittedEra interface{ SubmissionEra() uint }
	require.ErrorAs(t, err, &submittedEra)
	require.Equal(t, uint(conway.EraIdConway), submittedEra.SubmissionEra())
}
