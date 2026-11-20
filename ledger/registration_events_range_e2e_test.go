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
	"math"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

const registrationEventsRead = "GetStakeCredentialsWithRegistrationEvents"

func sortedCredentialKeys(refs []models.StakeCredentialRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.MapKey())
	}
	slices.Sort(out)
	return out
}

func registrationEventCredentials(
	t *testing.T,
	l ledgerdb.LedgerDB,
	from, to uint64,
) []string {
	t.Helper()
	refs, err := l.GetStakeCredentialsWithRegistrationEvents(from, to, nil)
	require.NoError(t, err)
	return sortedCredentialKeys(refs)
}

// The set answers the credentials with a registration or deregistration in a
// slot range, with the store's read failing, as the store's UNION over its
// certificate tables does, before and after a rollback.
func TestRegistrationEventCredentialsAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarnessWith(t, "set", nil)
	ldb := h.scenario.LedgerDB()
	s := h.slots
	ranges := [][2]uint64{
		{0, math.MaxInt64},
		{s.S1, s.S1},
		{s.S2, s.S2},
		{s.S3, s.S5},
		{s.S1 - 1, s.S1 - 1},
		{s.S5 + 1, math.MaxInt64},
		{s.DRegister, math.MaxInt64},
		{s.S4, s.S3},
	}
	want := make([][]string, len(ranges))
	for i, r := range ranges {
		want[i] = registrationEventCredentials(t, h.store, r[0], r[1])
	}
	require.Len(t, want[1], 3, "K, L and M register at S1")
	require.Len(t, want[2], 1, "K deregisters at S2")
	require.Empty(t, want[6], "DRep certificates are not stake events")
	require.Empty(t, want[7], "an empty range lists nothing")
	reached, _ := h.scenario.HideStoreReads(registrationEventsRead)
	for i, r := range ranges {
		require.Equal(t, want[i], registrationEventCredentials(t, ldb, r[0], r[1]),
			"range %v", r)
	}
	h.scenario.RollbackBeforeBoundary()
	for _, r := range ranges {
		require.Empty(t, registrationEventCredentials(t, ldb, r[0], r[1]),
			"range %v after the rollback", r)
	}
	require.Zero(t, reached(), "the read reached the metadata store")
}

// Verify mode answers from the store and counts a difference from the set.
func TestRegistrationEventCredentialsMatchTheStoreInVerifyMode(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarness(t)
	got := registrationEventCredentials(
		t, h.scenario.LedgerDB(), 0, math.MaxInt64,
	)
	require.Equal(
		t, registrationEventCredentials(t, h.store, 0, math.MaxInt64), got,
	)
	// Written around the ledger, so the set never sees it.
	cred := bytes.Repeat([]byte{0x7e}, 28)
	raw, err := dbtest.RawSQLiteMetadata(t, h.scenario.DB)
	require.NoError(t, err)
	_, err = raw.Exec(`
INSERT INTO stake_registration (staking_key, credential_tag, added_slot)
VALUES (?, 0, ?)`, cred, 5)
	require.NoError(t, err)
	_ = registrationEventCredentials(
		t, h.scenario.LedgerDB(), 0, math.MaxInt64,
	)
}

// A write transaction sees the registrations it staged and none the staged
// rollback drops.
func TestRegistrationEventCredentialsSeeStagedRegistrationsAndRollbacks(
	t *testing.T,
) {
	t.Parallel()
	h := newRegistrationHarnessWith(t, "set", nil)
	s := h.slots
	h.scenario.HideStoreReads(registrationEventsRead)
	fresh := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x9a}, 28))
	inside := h.scenario.RegistrationCredentialsInsideTxn(
		s.DReReg+1, s.DReReg+1, s.DReReg+1,
	)
	require.Equal(t, []string{fresh.MapKey()}, sortedCredentialKeys(inside))
	committed := registrationEventCredentials(
		t, h.scenario.LedgerDB(), s.DReReg+1, s.DReReg+1,
	)
	require.Empty(t, committed, "the abandoned transaction left nothing")
	all := h.scenario.RegistrationCredentialsInsideTxn(
		s.DReReg+1, 0, math.MaxInt64,
	)
	require.Len(t, all, 4, "K, L, M and the staged credential")
	rolled := h.scenario.RegistrationCredentialsInsideRollback(
		0, math.MaxInt64,
	)
	require.Empty(t, rolled, "the staged rollback drops every event")
}
