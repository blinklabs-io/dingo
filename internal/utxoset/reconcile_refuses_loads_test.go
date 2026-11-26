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

package utxoset_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// Reconcile ends the loads from the metadata store: a load over a set that
// holds rows must fail rather than replace them with rows from a store that
// is no longer the ledger's copy.
func TestReconcileRefusesALoadOverHeldRows(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	for slot := uint64(1); slot <= 3; slot++ {
		tip := slotPoint(slot)
		require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
			Nonces: []models.BlockNonce{{
				Slot: slot, Hash: tip.Hash,
				Nonce: bytes.Repeat([]byte{byte(slot)}, 32),
			}},
		}))
	}
	db := releasedAt(t, slotPoint(3))

	// Reconcile fails here because the set lacks other families; it has
	// already ended the loads.
	err := set.Reconcile(t.Context(), dbadapter.New(db), nil)
	require.ErrorContains(t, err, "does not hold")
	err = set.LoadLedgerState(t.Context(), dbadapter.New(db))
	require.ErrorIs(t, err, badgerutxo.ErrLoadsRefused)
}
