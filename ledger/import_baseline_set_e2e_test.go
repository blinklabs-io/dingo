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

package ledger_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

type baselineHarness struct {
	set      *utxoset.Set
	scenario ledger.BoundaryEpochStateScenario
	// withDeposit was imported with a deposit, deactivated was imported and
	// then deactivated by a newer snapshot, and unknown has no account.
	withDeposit, deactivated, unknown models.StakeCredentialRef
	// imported are the credentials the scenario's own import baselines are
	// for.
	imported []models.StakeCredentialRef
}

const (
	baselineDepositSlot    = 5
	baselineDeactivateSlot = 6
	baselineDeposit        = 2_000_000
)

func (h *baselineHarness) importAround(
	t *testing.T,
	key []byte,
	slot uint64,
	deposit *uint64,
) models.StakeCredentialRef {
	t.Helper()
	ref := models.NewStakeCredentialRef(0, key)
	pool := bytes.Repeat([]byte{0xf1}, 28)
	acc := models.Account{
		StakingKey: key, Pool: pool, Active: true,
		AddedSlot: slot, CreatedSlot: slot,
	}
	if deposit != nil {
		d := types.Uint64(*deposit)
		acc.ImportDeposit = &d
	}
	require.NoError(t, h.set.ApplyLedgerChanges(nil, ledger.LedgerChanges{
		Accounts: []ledger.AccountChange{{Slot: slot, Account: acc}},
		ImportBaselines: []ledger.ImportBaselineChange{{
			Slot: slot,
			Baseline: ledger.ImportBaseline{
				Ref: ref, Active: true, AddedSlot: slot, Deposit: deposit,
				Pool: pool,
			},
		}},
	}))
	return ref
}

// newBaselineHarness imports accounts with baselines into the set, one with
// a deposit and one deactivated by a newer snapshot.
func newBaselineHarness(t *testing.T) *baselineHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	h := &baselineHarness{set: set}
	h.scenario = ledger.NewBoundaryEpochStateScenarioForTest(t, set)
	h.imported, err = h.scenario.LedgerDB().GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	require.NotEmpty(t, h.imported)
	deposit := uint64(baselineDeposit)
	h.withDeposit = h.importAround(
		t,
		bytes.Repeat([]byte{0xf2}, 28),
		baselineDepositSlot,
		&deposit,
	)
	h.deactivated = h.importAround(
		t,
		bytes.Repeat([]byte{0xf3}, 28),
		baselineDepositSlot,
		&deposit,
	)
	require.NoError(t, set.ApplyLedgerChanges(nil, ledger.LedgerChanges{
		ImportBaselines: []ledger.ImportBaselineChange{{
			Slot: baselineDeactivateSlot,
			Baseline: ledger.ImportBaseline{
				Ref: h.deactivated, AddedSlot: baselineDepositSlot,
			},
		}},
	}))
	h.unknown = models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0xf4}, 28))
	return h
}

func (h *baselineHarness) refs() []models.StakeCredentialRef {
	return append(append([]models.StakeCredentialRef{}, h.imported...),
		h.withDeposit, h.deactivated, h.unknown)
}

func (h *baselineHarness) requireBaselines(t *testing.T, db ledgerdb.LedgerDB) {
	t.Helper()
	read := func(ref models.StakeCredentialRef) *models.AccountImportRegistration {
		got, err := db.GetAccountImportRegistrationByCredential(
			t.Context(), ref.Tag, ref.Key, nil,
		)
		require.NoError(t, err)
		return got
	}
	require.Equal(t,
		&models.AccountImportRegistration{
			AddedSlot: baselineDepositSlot, Deposit: ptr(baselineDeposit),
		},
		read(h.withDeposit), "an import with a deposit")
	require.Nil(
		t,
		read(h.deactivated),
		"an import a newer snapshot deactivated",
	)
	require.Nil(t, read(h.unknown), "a credential with no baseline")
	for _, ref := range h.imported {
		got := read(ref)
		require.NotNil(t, got, "the scenario's import of %x", ref.Key)
		require.Nil(t, got.Deposit, "an import that preserved no deposit")
	}
}

// TestImportBaselineReadsFromTheLiveUtxoSet reads the import baselines
// through the ledger's LedgerDB from the live set with the store's read
// failing, for accounts imported with and without a deposit and one a newer
// snapshot deactivated, before and after a rollback.
func TestImportBaselineReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newBaselineHarness(t)
	reached, _ := h.scenario.HideStoreReads(
		"GetAccountImportRegistrationByCredential",
	)
	ldb := h.scenario.LedgerDB()
	h.requireBaselines(t, ldb)
	h.scenario.RollbackBeforeBoundary()
	h.requireBaselines(t, ldb)
	require.Zero(t, reached(), "a baseline read reached the metadata store")
}
