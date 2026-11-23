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
// slot range, with the store's read failing, before and after a rollback.
func TestRegistrationEventCredentialsAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	h := newRegistrationHarnessWith(t, "set", nil)
	ldb := h.scenario.LedgerDB()
	s := h.slots
	reached, _ := h.scenario.HideStoreReads(registrationEventsRead)
	in := func(from, to uint64) []string {
		t.Helper()
		return registrationEventCredentials(t, ldb, from, to)
	}
	key := func(k []byte) string {
		return models.NewStakeCredentialRef(0, k).MapKey()
	}
	atS1 := []string{key(s.K), key(s.L), key(s.M)}
	slices.Sort(atS1)
	require.Equal(t, atS1, in(s.S1, s.S1), "K, L and M register at S1")
	require.Equal(t, []string{key(s.K)}, in(s.S2, s.S2), "K deregisters at S2")
	require.Equal(t, atS1, in(0, math.MaxInt64))
	require.NotEmpty(t, in(s.S3, s.S5))
	require.Empty(t, in(s.S1-1, s.S1-1), "nothing registers before S1")
	require.Empty(t, in(s.S5+1, math.MaxInt64), "nothing registers after S5")
	require.Empty(t, in(s.DRegister, math.MaxInt64),
		"DRep certificates are not stake events")
	require.Empty(t, in(s.S4, s.S3), "an empty range lists nothing")
	h.scenario.RollbackBeforeBoundary()
	for _, r := range [][2]uint64{
		{0, math.MaxInt64}, {s.S1, s.S1}, {s.S2, s.S2}, {s.S3, s.S5},
	} {
		require.Empty(t, in(r[0], r[1]), "range %v after the rollback", r)
	}
	require.Zero(t, reached(), "the read reached the metadata store")
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
