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

package ledger

import (
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runActivate applies activateDelegatorInactivityIfNeeded inside a real
// write transaction, mirroring how processEpochRollover calls it (and how
// runRenew in account_expiry_renew_test.go drives
// renewWitnessedAccountExpirations).
func runActivate(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	currentEpoch uint64,
) error {
	t.Helper()
	txn := db.Transaction(true)
	return txn.Do(func(txn *database.Txn) error {
		return ls.activateDelegatorInactivityIfNeeded(txn, currentEpoch)
	})
}

// TestActivateDelegatorInactivityIfNeeded_GateOn verifies that, with the gate
// on, activating at epoch E stamps every pre-existing active account
// to E + DelegatorInactivity and durably records the activation marker.
func TestActivateDelegatorInactivityIfNeeded_GateOn(t *testing.T) {
	const (
		epoch      = uint64(200)
		inactivity = uint64(90)
	)
	ls, db := newRenewTestLedger(t, true, inactivity)

	credA := renewTestCred(0x11)
	credB := renewTestCred(0x12)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    credA,
		CredentialTag: 0,
		Active:        true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    credB,
		CredentialTag: 0,
		Active:        true,
	}))

	require.NoError(t, runActivate(t, ls, db, epoch))

	acctA, err := db.GetAccountByCredential(0, credA, false, nil)
	require.NoError(t, err)
	assert.Equal(t, epoch+inactivity, acctA.ExpirationEpoch)

	acctB, err := db.GetAccountByCredential(0, credB, false, nil)
	require.NoError(t, err)
	assert.Equal(t, epoch+inactivity, acctB.ExpirationEpoch)

	marker, err := db.GetSyncState(delegatorInactivityActivatedSyncKey, nil)
	require.NoError(t, err)
	assert.Equal(
		t,
		strconv.FormatUint(epoch, 10),
		marker,
		"durable marker must record the activation epoch",
	)
}

// TestActivateDelegatorInactivityIfNeeded_RunsOnce verifies the durable
// marker guards a second activation attempt (e.g. a later epoch boundary)
// from re-stamping an account whose expiration has since moved on (either
// through ordinary CIP-0163 renewal or a manual operator adjustment).
func TestActivateDelegatorInactivityIfNeeded_RunsOnce(t *testing.T) {
	const (
		epoch      = uint64(200)
		inactivity = uint64(90)
	)
	ls, db := newRenewTestLedger(t, true, inactivity)

	cred := renewTestCred(0x21)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    cred,
		CredentialTag: 0,
		Active:        true,
	}))

	require.NoError(t, runActivate(t, ls, db, epoch))

	// Simulate the account's expiration having since moved on (a normal
	// renewal or a manual operator set) to a value the second activation
	// pass must not clobber.
	const manualExpiration = uint64(9999)
	require.NoError(t, db.RenewAccountExpirations(
		[]models.StakeCredentialRef{models.NewStakeCredentialRef(0, cred)},
		manualExpiration,
		nil,
	))

	// A later epoch boundary reaching the activation step again must be a
	// no-op: the marker is already set.
	require.NoError(t, runActivate(t, ls, db, epoch+inactivity+5))

	acct, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	assert.Equal(
		t,
		manualExpiration,
		acct.ExpirationEpoch,
		"second activation must not re-stamp an already-activated account",
	)
}

// TestActivateDelegatorInactivityIfNeeded_GateOff verifies that, with the
// gate off, activation neither stamps any account nor writes the durable
// marker (so turning the gate on later still triggers a real activation on
// the next boundary).
func TestActivateDelegatorInactivityIfNeeded_GateOff(t *testing.T) {
	const epoch = uint64(200)
	ls, db := newRenewTestLedger(t, false, 90)

	cred := renewTestCred(0x31)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    cred,
		CredentialTag: 0,
		Active:        true,
	}))

	require.NoError(t, runActivate(t, ls, db, epoch))

	acct, err := db.GetAccountByCredential(0, cred, false, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), acct.ExpirationEpoch, "gate off must not stamp")

	marker, err := db.GetSyncState(delegatorInactivityActivatedSyncKey, nil)
	require.NoError(t, err)
	assert.Equal(t, "", marker, "gate off must not write the activation marker")
}
