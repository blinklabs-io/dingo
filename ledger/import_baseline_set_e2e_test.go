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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

type baselineHarness struct {
	set      *utxoset.Set
	scenario ledger.BoundaryEpochStateScenario
	store    ledgerdb.LedgerDB
	// withDeposit was imported with a deposit, deactivated was imported and
	// then deactivated by a newer snapshot, and unknown has no account.
	withDeposit, deactivated, unknown models.StakeCredentialRef
	// imported are the credentials the scenario's own import baselines are
	// for.
	imported []models.StakeCredentialRef
}

const (
	baselineDepositSlot    = 5
	baselineDeactivateSlot = 20
	baselineDeposit        = 2_000_000
)

func (h *baselineHarness) importAround(
	t *testing.T,
	key []byte,
	slot uint64,
	deposit *uint64,
) models.StakeCredentialRef {
	t.Helper()
	acc := models.Account{
		StakingKey: key, Pool: bytes.Repeat([]byte{0xf1}, 28), Active: true,
		AddedSlot: slot, CreatedSlot: slot,
	}
	if deposit != nil {
		d := types.Uint64(*deposit)
		acc.ImportDeposit = &d
	}
	require.NoError(t, h.scenario.DB.Metadata().ImportAccount(&acc, nil))
	return models.NewStakeCredentialRef(0, key)
}

// newBaselineHarness imports accounts with baselines, one with a deposit and
// one deactivated by a newer snapshot, straight into the metadata store,
// before the set loads from it.
func newBaselineHarness(t *testing.T, reads string) *baselineHarness {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	h := &baselineHarness{set: set}
	h.scenario = ledger.NewBoundaryEpochStateScenarioForTest(t, set, reads)
	h.store = dbadapter.New(h.scenario.DB)
	require.NoError(
		t,
		h.scenario.DB.Metadata().
			IterateAccounts(nil, func(a *models.Account) error {
				h.imported = append(
					h.imported,
					models.NewStakeCredentialRef(a.CredentialTag, a.StakingKey),
				)
				return nil
			}),
	)
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
	require.NoError(t, h.scenario.DB.Metadata().DeactivateAccounts(
		nil, []models.StakeCredentialRef{h.deactivated}, baselineDeactivateSlot,
	))
	h.unknown = models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0xf4}, 28))
	require.NoError(t, set.LoadLedgerState(t.Context(), h.store))
	return h
}

func (h *baselineHarness) refs() []models.StakeCredentialRef {
	return append(append([]models.StakeCredentialRef{}, h.imported...),
		h.withDeposit, h.deactivated, h.unknown)
}

func baselineText(r *models.AccountImportRegistration) string {
	if r == nil {
		return "none"
	}
	return fmt.Sprintf("%d/%s", r.AddedSlot, depositText(r.Deposit))
}

func (h *baselineHarness) readsOf(t *testing.T, db ledgerdb.LedgerDB) []string {
	t.Helper()
	var out []string
	for _, ref := range h.refs() {
		got, err := db.GetAccountImportRegistrationByCredential(
			t.Context(), ref.Tag, ref.Key, nil,
		)
		require.NoError(t, err, "baseline of %x", ref.Key)
		out = append(out, fmt.Sprintf("%x %s", ref.Key[:2], baselineText(got)))
	}
	return out
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
// through the ledger's LedgerDB in set mode with the store's read failing,
// for accounts a snapshot imported with and without a deposit and one a newer
// snapshot deactivated, before and after a rollback, and requires every
// answer to be the store's.
func TestImportBaselineReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newBaselineHarness(t, ledger.UtxoSetReadsSet)
	reached, _ := h.scenario.HideStoreReads(
		"GetAccountImportRegistrationByCredential",
	)
	ldb := h.scenario.LedgerDB()
	require.Equal(t, h.readsOf(t, h.store), h.readsOf(t, ldb))
	h.requireBaselines(t, ldb)
	h.scenario.RollbackBeforeBoundary()
	require.Equal(
		t,
		h.readsOf(t, h.store),
		h.readsOf(t, ldb),
		"after the rollback",
	)
	h.requireBaselines(t, ldb)
	require.Zero(t, reached(), "a baseline read reached the metadata store")
	verified, mismatches := h.scenario.RegistrationReads()
	require.Zero(t, verified, "set mode compares nothing")
	require.Zero(t, mismatches)
}

// TestImportBaselineReadsAgreeInVerifyMode requires every verified baseline
// read to agree with the metadata store.
func TestImportBaselineReadsAgreeInVerifyMode(t *testing.T) {
	t.Parallel()
	h := newBaselineHarness(t, ledger.UtxoSetReadsVerify)
	ldb := h.scenario.LedgerDB()
	h.requireBaselines(t, ldb)
	verified, mismatches := h.scenario.RegistrationReads()
	require.Equal(t, float64(len(h.refs())), verified)
	require.Zero(t, mismatches)
}

// TestImportBaselineVerificationCountsADivergence requires a verify-mode read
// to count a mismatch when the metadata store holds a baseline the set does
// not, and to answer with the store's.
func TestImportBaselineVerificationCountsADivergence(t *testing.T) {
	t.Parallel()
	h := newBaselineHarness(t, ledger.UtxoSetReadsVerify)
	// Imported around the ledger's LedgerDB after the set loaded, so the set
	// never sees it.
	ref := h.importAround(t, bytes.Repeat([]byte{0xf5}, 28), 7, nil)
	got, err := h.scenario.LedgerDB().GetAccountImportRegistrationByCredential(
		t.Context(), ref.Tag, ref.Key, nil,
	)
	require.NoError(t, err)
	require.Equal(t, &models.AccountImportRegistration{AddedSlot: 7}, got,
		"the read answers from the metadata store")
	verified, mismatches := h.scenario.RegistrationReads()
	require.Equal(t, float64(1), verified)
	require.Equal(t, float64(1), mismatches)
}
