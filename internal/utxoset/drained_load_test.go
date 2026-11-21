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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// A set whose metadata store has drained a family never reloads one from it:
// a load that would replace rows of a family from the store's tables fails
// before it drops any, and loads resume when the store holds the rows.
func TestLoadLedgerStateRefusesToReloadFromADrainedStore(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 2)
	tip := slotPoint(3)
	require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
		Nonces: []models.BlockNonce{{
			Hash: tip.Hash, Slot: tip.Slot, Nonce: []byte{1},
		}},
	}))
	db := ledgerAt(t, tip)

	set.SetDrainedFamilies(nodesettings.FamilyMask(0).With(nodesettings.FamilyNonces))
	err := set.LoadLedgerState(t.Context(), dbadapter.New(db))
	require.ErrorIs(t, err, badgerutxo.ErrLoadsRefused)
	_, held, err := set.BlockNonce(tip)
	require.NoError(t, err)
	require.True(t, held, "a refused load dropped the set's only nonce")

	set.SetDrainedFamilies(0)
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	loaded, err := set.NoncesLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
}

// A drained family does not stop the reload of one the metadata store still
// holds: every family but the nonces is drained, and the nonce rows the set
// holds still reload from their intact tables.
func TestLoadLedgerStateReloadsFamiliesTheStoreStillHolds(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	applySlots(t, set, 2)
	tip := slotPoint(3)
	require.NoError(t, set.ApplyLedgerChanges(&tip, ledger.LedgerChanges{
		Nonces: []models.BlockNonce{{
			Hash: tip.Hash, Slot: tip.Slot, Nonce: []byte{1},
		}},
	}))
	db := ledgerAt(t, tip)

	set.SetDrainedFamilies(
		nodesettings.AllFamilies() &^
			nodesettings.FamilyMask(0).With(nodesettings.FamilyNonces),
	)
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	loaded, err := set.NoncesLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
}
