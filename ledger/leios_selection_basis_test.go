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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// Endorser-block selection is pinned to the ranking block's parent: a ledger
// whose tip is elsewhere (typically trailing the primary chain by an
// unapplied block that may apply a certified closure) must not validate.
func TestWithTxValidationSessionAtParentRequiresParentTip(t *testing.T) {
	t.Parallel()
	ls, _, _ := newLeiosApplyTestLedger(t)
	tipHash := bytes.Repeat([]byte{0x5a}, lcommon.Blake2b256Size)
	ls.currentTip = ochainsync.Tip{
		Point:       ocommon.NewPoint(500, tipHash),
		BlockNumber: 7,
	}
	ls.publishSnapshotsLocked()

	called := 0
	session := func(
		_ func(ledger.Transaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo, *utxoref.StateOverlay) error,
		_ func() bool,
		_ func(func() error) (bool, error),
	) error {
		called++
		return nil
	}

	other := bytes.Repeat([]byte{0x5b}, lcommon.Blake2b256Size)
	err := ls.WithTxValidationSessionAtParent(t.Context(), other, session)
	require.ErrorIs(t, err, forging.ErrTxValidationParentMismatch)
	require.Zero(t, called)

	require.NoError(
		t,
		ls.WithTxValidationSessionAtParent(t.Context(), tipHash, session),
	)
	require.Equal(t, 1, called)
}

// After a certifying ranking block is applied, the certified closure's
// transactions have spent their inputs: a validation session must reject
// re-selecting them, so the view the mempool and the forger validate against
// reflects the closure.
func TestValidationSessionRejectsAppliedClosureTransaction(t *testing.T) {
	t.Parallel()
	ls, db, _ := newLeiosApplyTestLedger(t)
	enableLeiosApplyInputValidation(ls)
	rawProducer, producerTx := leiosApplyTestProducerTx(t, 0x61)
	rawSpend, spendTx := leiosApplyTestSpendingTx(
		t, 0x62, producerTx.Hash().Bytes(), 0,
	)
	_, err := leiosApplyTestApplyEndorserBlock(
		t, ls, db, leiosApplyTestRankingPoint(0x41), 1, 940,
		leiosApplyTestEbHash(0x63), rawProducer,
	)
	require.NoError(t, err)

	validateSpend := func() error {
		var verdict error
		require.NoError(t, ls.WithTxValidationSession(t.Context(), func(
			validate func(ledger.Transaction, map[utxoref.Key]struct{}, map[utxoref.Key]lcommon.Utxo, *utxoref.StateOverlay) error,
			_ func() bool,
			_ func(func() error) (bool, error),
		) error {
			verdict = validate(
				spendTx,
				map[utxoref.Key]struct{}{},
				map[utxoref.Key]lcommon.Utxo{},
				utxoref.NewStateOverlay(),
			)
			return nil
		}))
		return verdict
	}
	require.NoError(t, validateSpend())

	_, err = leiosApplyTestApplyEndorserBlock(
		t, ls, db, leiosApplyTestRankingPoint(0x42), 2, 941,
		leiosApplyTestEbHash(0x64), rawSpend,
	)
	require.NoError(t, err)
	require.Error(t, validateSpend())
}
