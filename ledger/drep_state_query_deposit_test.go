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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// drepStateQueryDeposits seeds two DReps whose registrations recorded
// different deposits, publishes a current dRepDeposit parameter equal to
// neither, and returns the GetDRepState result.
//
// The three values are deliberately distinct. dRepDeposit is governable, so a
// DRep holds the value the parameter had when its own registration
// certificate was applied; a result built from the current parameter reports
// one number for every DRep and the wrong number for any DRep that
// registered before the last change to it.
func drepStateQueryDeposits(
	t *testing.T,
	creds []lcommon.Credential,
) olocalstatequery.DRepStateResult {
	t.Helper()
	db := newTestDB(t)
	const (
		earlyDeposit   = uint64(400_000_000)
		lateDeposit    = uint64(500_000_000)
		currentDeposit = uint64(600_000_000)
	)
	early := drepRefundTestCredential(0xe1)
	late := drepRefundTestCredential(0xe2)
	seedImportedDrep(t, db, early, earlyDeposit, 100, true)
	seedImportedDrep(t, db, late, lateDeposit, 200, true)

	ls := &LedgerState{db: db}
	ls.currentPParams = &conway.ConwayProtocolParameters{
		DRepDeposit: currentDeposit,
	}
	ls.publishSnapshotsLocked()

	result, err := ls.queryShelleyDRepState(creds)
	require.NoError(t, err)
	outer, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, outer, 1)
	dreps, ok := outer[0].(olocalstatequery.DRepStateResult)
	require.True(t, ok)
	require.Len(t, dreps, 2)

	for _, want := range []struct {
		cred    lcommon.Credential
		deposit uint64
	}{
		{early, earlyDeposit},
		{late, lateDeposit},
	} {
		cred := want.cred
		tag, err := models.CredentialTagFromUint(uint(cred.CredType))
		require.NoError(t, err)
		key := olocalstatequery.StakeCredential{
			Tag:   uint64(tag),
			Bytes: gledger.NewBlake2b224(cred.Credential[:]),
		}
		entry, found := dreps[key]
		require.True(t, found, "DRep missing from GetDRepState result")
		require.Equal(
			t,
			want.deposit,
			entry.Deposit,
			"GetDRepState must report the deposit recorded at this DRep's registration, not the current dRepDeposit parameter",
		)
		require.NotEqual(t, currentDeposit, entry.Deposit)
	}
	return dreps
}

// TestQueryDRepStateReportsRecordedDepositUnrestricted covers the
// empty-filter form, which reads the deposits in one batched query.
func TestQueryDRepStateReportsRecordedDepositUnrestricted(t *testing.T) {
	t.Parallel()

	drepStateQueryDeposits(t, nil)
}

// TestQueryDRepStateReportsRecordedDepositFiltered covers the
// credential-filtered form, which reads each deposit on its own. Both forms
// are covered because they source the deposit through different queries, and
// a fix applied to one leaves the other reporting the parameter.
func TestQueryDRepStateReportsRecordedDepositFiltered(t *testing.T) {
	t.Parallel()

	drepStateQueryDeposits(t, []lcommon.Credential{
		drepRefundTestCredential(0xe1),
		drepRefundTestCredential(0xe2),
	})
}
