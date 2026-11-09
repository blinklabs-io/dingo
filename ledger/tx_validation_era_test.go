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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/eras"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestValidateTxRejectsPreviousEra(t *testing.T) {
	t.Parallel()

	previous := eraDesc(t, eras.Eras, babbage.EraIdBabbage)
	current := eraDesc(t, eras.Eras, conway.EraIdConway)
	previous.ValidateTxFunc = nil
	current.ValidateTxFunc = nil
	ls := &LedgerState{
		activeEras: []eras.EraDesc{previous, current},
		currentEra: current,
		slotClock: NewSlotClock(
			newMockSlotTimeProvider(time.Now(), time.Second, 100),
			DefaultSlotClockConfig(),
		),
	}
	ls.publishSnapshotsLocked()

	currentTx := mockledger.NewTransactionBuilder().
		WithType(int(conway.EraIdConway))
	require.NoError(t, ls.ValidateTx(currentTx), "current-era transaction")

	previousTx := mockledger.NewTransactionBuilder().
		WithType(int(babbage.EraIdBabbage))
	err := ls.ValidateTx(previousTx)
	var mismatch *gledger.EraMismatch
	if !errors.As(err, &mismatch) {
		t.Fatalf(
			"previous-era submission must return a typed era mismatch; got %v",
			err,
		)
	}
	require.Equal(t, "Babbage", mismatch.OtherEra.Name)
	require.Equal(t, "Conway", mismatch.LedgerEra.Name)
}

func TestEvaluateTxRejectsPreviousEra(t *testing.T) {
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

	currentTx := mockledger.NewTransactionBuilder().
		WithType(int(conway.EraIdConway))
	_, _, _, err := ls.EvaluateTx(currentTx)
	require.NoError(t, err, "current-era transaction")

	previousTx := mockledger.NewTransactionBuilder().
		WithType(int(babbage.EraIdBabbage))
	_, _, _, err = ls.EvaluateTx(previousTx)
	var mismatch *gledger.EraMismatch
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, "Babbage", mismatch.OtherEra.Name)
	require.Equal(t, "Conway", mismatch.LedgerEra.Name)
}
