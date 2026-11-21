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
	"fmt"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

var registrationReadNames = []string{
	"GetAccountRegistrationHistoryByCredential",
	"GetLatestAccountRegistrationAtOrBefore",
	"GetStakeRegistrationsByCredential", "GetAccountsActiveAtSlot",
	"GetDrepLastRegistrationDeposit", "GetDrepLastRegistrationDeposits",
}

// unknownTableSet is a set whose loaded registrations name no table, as a
// set loaded from the metadata store holds them.
type unknownTableSet struct{ *utxoset.Set }

func (u unknownTableSet) StakeHistory(
	ref models.StakeCredentialRef,
) ([]ledger.StakeEvent, error) {
	events, err := u.Set.StakeHistory(ref)
	for i := range events {
		events[i].Class = ledger.StakeClassUnknown
	}
	return events, err
}

func depositText(d *uint64) string {
	if d == nil {
		return "nil"
	}
	return fmt.Sprint(*d)
}

func historyRowText(r *models.AccountRegistrationHistoryRow) string {
	if r == nil {
		return "nil"
	}
	return fmt.Sprintf("%d/%d/%d/%x/%s/%s/%d/%x", r.AddedSlot, r.BlockIndex,
		r.CertIndex, r.TxHash, r.Action, depositText(r.Deposit), r.TxSlot,
		r.BlockHash)
}

func (h *registrationHarness) readsOf(
	t *testing.T,
	db ledgerdb.LedgerDB,
) []string {
	t.Helper()
	s := h.slots
	ctx := t.Context()
	var out []string
	keys := [][]byte{s.K, s.L, s.M, s.Imported.Key}
	for _, key := range keys {
		for _, q := range []struct {
			limit, offset int
			order         string
		}{{0, 0, "asc"}, {0, 0, "desc"}, {2, 1, "desc"}, {1, 0, "asc"}, {0, 9, "asc"}} {
			rows, err := db.GetAccountRegistrationHistoryByCredential(
				ctx, 0, key, q.limit, q.offset, q.order, nil,
			)
			require.NoError(t, err)
			var texts []string
			for i := range rows {
				texts = append(texts, historyRowText(&rows[i]))
			}
			out = append(out, fmt.Sprintf("history %x %v %v", key, q, texts))
		}
		for _, slot := range []uint64{s.S1 - 1, s.S1, s.S2, s.S3, s.S4, s.S5 + 100} {
			row, err := db.GetLatestAccountRegistrationAtOrBefore(
				ctx,
				0,
				key,
				slot,
				nil,
			)
			require.NoError(t, err)
			out = append(
				out,
				fmt.Sprintf("latest %x %d %s", key, slot, historyRowText(row)),
			)
		}
		certs, err := db.GetStakeRegistrationsByCredential(0, key, nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("certs %x %+v", key, certs))
	}
	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(
			0,
			s.K,
		), models.NewStakeCredentialRef(0, s.L),
		models.NewStakeCredentialRef(0, s.M), s.Imported,
	}
	for _, slot := range []uint64{s.S1 - 1, s.S1, s.S2, s.S3, s.S4, s.S5, s.S5 + 100} {
		active, err := db.GetAccountsActiveAtSlot(refs, slot, nil)
		require.NoError(t, err)
		var keys []string
		for k := range active {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		out = append(out, fmt.Sprintf("active %d %x", slot, keys))
	}
	for _, key := range [][]byte{s.D, s.E, s.K} {
		dep, err := db.GetDrepLastRegistrationDeposit(ctx, 0, key, nil)
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("deposit %x %s", key, depositText(dep)))
	}
	all, err := db.GetDrepLastRegistrationDeposits(ctx, nil)
	require.NoError(t, err)
	out = append(out, fmt.Sprintf("deposits %v", all))
	return out
}

// TestRegistrationReadsFromTheLiveUtxoSet reads the registration history and
// DRep deposits through the ledger's LedgerDB from the live set with the store's
// reads failing, for a credential that registers, deregisters and
// re-registers, and DReps that register, update and retire, before and after
// a rollback, and requires every answer to be the store's.
func TestRegistrationReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarnessWith(t, "set", nil)
	reached, _ := h.scenario.HideStoreReads(registrationReadNames...)
	ldb := h.scenario.LedgerDB()
	want := h.readsOf(t, h.store)
	require.Equal(t, want, h.readsOf(t, ldb))
	require.NotEmpty(t, stakeHistory(t, ldb, h.slots.K, 0, 0, "asc"))
	h.scenario.RollbackBeforeBoundary()
	want = h.readsOf(t, h.store)
	require.Equal(t, want, h.readsOf(t, ldb), "after the rollback")
	require.Empty(t, stakeHistory(t, ldb, h.slots.K, 0, 0, "asc"))
	require.Zero(t, reached(), "a registration read reached the metadata store")
}

// TestStakeRegistrationReadFailsForAnUnknownTable requires an unavailable
// registration history table to fail without mixing in metadata reads.
func TestStakeRegistrationReadFailsForAnUnknownTable(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarnessWith(t, "set",
		func(s *utxoset.Set) ledger.UtxoSetWriter { return unknownTableSet{s} })
	reached, _ := h.scenario.HideStoreReads(registrationReadNames...)
	ldb := h.scenario.LedgerDB()
	_, err := ldb.GetStakeRegistrationsByCredential(0, h.slots.K, nil)
	require.Error(t, err)
	require.Zero(t, reached(), "the read did not use the metadata store")
	rows := stakeHistory(t, ldb, h.slots.K, 0, 0, "asc")
	require.Len(t, rows, 5)
	require.Zero(t, reached(), "the history is served by the set")
}
