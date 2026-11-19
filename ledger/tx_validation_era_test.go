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
	"time"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func testBabbageSubmission(t *testing.T) gledger.Transaction {
	t.Helper()
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
	tx, err := babbage.NewBabbageTransactionFromCbor(raw)
	require.NoError(t, err)
	return tx
}

func testMarySubmission(t *testing.T) gledger.Transaction {
	t.Helper()
	raw, err := cbor.Encode([]any{
		map[uint]any{
			0: []any{},
			1: []any{},
			2: uint64(0),
		},
		map[uint]any{},
		nil,
	})
	require.NoError(t, err)
	tx, err := mary.NewMaryTransactionFromCbor(raw)
	require.NoError(t, err)
	return tx
}

func TestTranslateSubmittedTxToCurrentEra(t *testing.T) {
	t.Parallel()

	current := eraDesc(t, eras.Eras, conway.EraIdConway)
	current.ValidateTxFunc = nil
	current.EvaluateTxFunc = nil
	ls := &LedgerState{
		activeEras: eras.Eras,
		currentEra: current,
		slotClock: NewSlotClock(
			newMockSlotTimeProvider(time.Now(), time.Second, 100),
			DefaultSlotClockConfig(),
		),
	}
	ls.publishSnapshotsLocked()

	for _, submitted := range []gledger.Transaction{
		testBabbageSubmission(t),
		testMarySubmission(t),
	} {
		translated, err := ls.TranslateSubmittedTx(submitted)
		require.NoError(t, err)
		require.Equal(t, conway.EraIdConway, translated.Type())
		require.Equal(t, submitted.Hash(), translated.Hash())
		require.IsType(t, &conway.ConwayTransaction{}, translated)
		require.NoError(t, ls.ValidateTx(submitted))
		_, _, _, err = ls.EvaluateTx(submitted)
		require.NoError(t, err)
	}
}

func TestTranslateSubmittedTxRejectsUnsupportedEra(t *testing.T) {
	t.Parallel()

	previous := eraDesc(t, eras.Eras, babbage.EraIdBabbage)
	current := eraDesc(t, eras.Eras, conway.EraIdConway)
	previous.EvaluateTxFunc = nil
	current.EvaluateTxFunc = nil
	ls := &LedgerState{
		activeEras: []eras.EraDesc{previous, current},
		currentEra: current,
		slotClock: NewSlotClock(
			newMockSlotTimeProvider(time.Now(), time.Second, 100),
			DefaultSlotClockConfig(),
		),
	}
	ls.publishSnapshotsLocked()

	byronTx := mockledger.NewTransactionBuilder().WithType(0)
	_, err := ls.TranslateSubmittedTx(byronTx)
	var mismatch *gledger.EraMismatch
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, "Byron", mismatch.OtherEra.Name)
	require.Equal(t, "Conway", mismatch.LedgerEra.Name)
}

func TestTranslateSubmittedTxReportsConversionFailure(t *testing.T) {
	t.Parallel()

	current := eraDesc(t, eras.Eras, conway.EraIdConway)
	ls := &LedgerState{
		activeEras: []eras.EraDesc{
			eraDesc(t, eras.Eras, babbage.EraIdBabbage),
			current,
		},
		currentEra: current,
	}
	ls.publishSnapshotsLocked()

	malformed := mockledger.NewTransactionBuilder().
		WithType(int(babbage.EraIdBabbage))
	_, err := ls.TranslateSubmittedTx(malformed)
	require.Error(t, err)
	var mismatch *gledger.EraMismatch
	require.NotErrorAs(t, err, &mismatch)
	var submittedEra interface{ SubmissionEra() uint }
	require.ErrorAs(t, err, &submittedEra)
	require.Equal(t, uint(conway.EraIdConway), submittedEra.SubmissionEra())
}
