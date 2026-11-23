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
	"testing"

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
