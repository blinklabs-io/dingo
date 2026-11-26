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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// A credit to an account that is not registered reports
// models.ErrAccountNotFound: POOLREAP, governance refunds and MIR route the
// amount to the treasury on that answer.
func TestCreditToUnregisteredAccountReportsNotFound(t *testing.T) {
	t.Parallel()
	missing := bytes.Repeat([]byte{0x5e}, 28)
	h := newSetHarness(t)
	ldb := h.scenario.LedgerDB()
	for _, c := range []struct {
		kind   string
		credit func() error
	}{
		{"single", func() error {
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			return ldb.AddAccountRewardByCredential(
				t.Context(), 0, missing, 500, 100, []byte{0x01}, txn,
			)
		}},
		{"post snapshot", func() error {
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			return ldb.AddPostSnapshotAccountRewardByCredential(
				t.Context(), 0, missing, 500, 100, []byte{0x02}, txn,
			)
		}},
		{"batch", func() error {
			txn := ldb.Begin(t.Context(), true)
			defer txn.Release()
			return ldb.AddAccountRewardsByCredential(
				t.Context(), []models.AccountRewardCredit{{
					StakingKey: missing, Amount: 500, Slot: 100,
					SourceHash: []byte{0x03},
				}}, txn,
			)
		}},
	} {
		require.ErrorIs(
			t, c.credit(), models.ErrAccountNotFound, "%s", c.kind,
		)
	}
	txn := ldb.Begin(t.Context(), true)
	defer txn.Release()
	require.NoError(t, ldb.AddAccountRewardByCredential(
		t.Context(), 0, missing, 0, 100, []byte{0x04}, txn,
	), "a zero credit is not applied and finds no account")
	require.False(
		t, h.scenario.SetFailed(),
		"a credit the caller handles must not stop the set",
	)
}
