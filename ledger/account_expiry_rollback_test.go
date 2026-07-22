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
	"encoding/binary"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// newExpiryRollbackTestLedger builds a DB-backed LedgerState with the CIP-0163
// delegator-inactivity gate configured and a fixed epoch schedule so
// SlotToEpoch resolves the seeded witness slots. Epochs are 100 slots long:
// E0 = [0,100), E1 = [100,200), E2 = [200,300), ... Epochs after E2 are
// projected from the current era, as SlotToEpoch documents.
func newExpiryRollbackTestLedger(
	t *testing.T,
	enabled bool,
	inactivity uint64,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
			DelegatorInactivityEnabled: enabled,
			DelegatorInactivity:        inactivity,
		},
		currentEra: eras.ShelleyEraDesc,
		epochCache: []models.Epoch{
			{EpochId: 0, StartSlot: 0, SlotLength: 1000, LengthInSlots: 100, EraId: 1},
			{EpochId: 1, StartSlot: 100, SlotLength: 1000, LengthInSlots: 100, EraId: 1},
			{EpochId: 2, StartSlot: 200, SlotLength: 1000, LengthInSlots: 100, EraId: 1},
		},
	}
	ls.publishSnapshotsLocked()
	return ls, db
}

// seedRollbackCertificate records a certificate through the public database
// ingestion boundary. This keeps the ledger test independent of a concrete
// metadata plugin and its GORM schema.
func seedRollbackCertificate(
	t *testing.T,
	db *database.Database,
	slot uint64,
	cert lcommon.Certificate,
) {
	t.Helper()
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[len(txID)-8:], slot)
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(txID)
	tx.WithCertificates(cert)
	require.NoError(t, db.SetTransactionMetadataOnly(
		tx,
		ocommon.NewPoint(slot, txID),
		0,
		map[int]uint64{0: 0},
		nil,
	))
}

func rollbackStakeDelegationCertificate(
	cred []byte,
) lcommon.Certificate {
	return &lcommon.StakeDelegationCertificate{
		StakeCredential: &lcommon.Credential{
			CredType:   0,
			Credential: lcommon.NewBlake2b224(cred),
		},
		PoolKeyHash: lcommon.NewBlake2b224(renewTestCred(0x0A)),
	}
}

func rollbackStakeRegistrationCertificate(
	cred []byte,
) lcommon.Certificate {
	return &lcommon.StakeRegistrationCertificate{
		StakeCredential: lcommon.Credential{
			CredType:   0,
			Credential: lcommon.NewBlake2b224(cred),
		},
	}
}

func rollbackStakeDeregistrationCertificate(
	cred []byte,
) lcommon.Certificate {
	return &lcommon.StakeDeregistrationCertificate{
		StakeCredential: lcommon.Credential{
			CredType:   0,
			Credential: lcommon.NewBlake2b224(cred),
		},
	}
}

// runRollbackRecompute mirrors the rollback path: it gathers the affected set
// (witnessed after rollbackSlot) before any deletes, then recomputes
// expirations, all inside one write transaction.
func runRollbackRecompute(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	rollbackSlot uint64,
) {
	t.Helper()
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		affected, err := db.AccountsWitnessedAfterSlot(rollbackSlot, txn)
		if err != nil {
			return err
		}
		return ls.recomputeAccountExpirationsAfterRollback(
			txn,
			rollbackSlot,
			affected,
		)
	}))
}

// TestRecomputeAccountExpirationsAfterRollbackDropsOrphanedRenewal is the brief
// scenario: a credential witnessed in E1 (slot 150) then E2 (slot 250) has its
// expiration stamped to E2+W. Rolling back into E1 (slot 199) must drop the
// orphaned E2 renewal and restore the surviving E1 renewal: ExpirationEpoch ==
// E1 + DelegatorInactivity.
func TestRecomputeAccountExpirationsAfterRollbackDropsOrphanedRenewal(t *testing.T) {
	const inactivity = uint64(90)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)

	cred := renewTestCred(0x01)
	// Account currently reflects the E2 witness: expiration = 2 + 90 = 92.
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      cred,
		CredentialTag:   0,
		Active:          true,
		AddedSlot:       150,
		ExpirationEpoch: 2 + inactivity,
	}))
	// Witness in E1 (slot 150) and E2 (slot 250).
	seedRollbackCertificate(
		t, db, 150, rollbackStakeDelegationCertificate(cred),
	)
	seedRollbackCertificate(
		t, db, 250, rollbackStakeDelegationCertificate(cred),
	)

	runRollbackRecompute(t, ls, db, 199)

	acct, err := db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1)+inactivity, acct.ExpirationEpoch)
}

// TestRecomputeAccountExpirationsAfterRollbackGateOff verifies the recomputation
// is a no-op when the gate is off: the E2-derived expiration is left untouched.
func TestRecomputeAccountExpirationsAfterRollbackGateOff(t *testing.T) {
	const inactivity = uint64(90)
	ls, db := newExpiryRollbackTestLedger(t, false, inactivity)

	cred := renewTestCred(0x02)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      cred,
		CredentialTag:   0,
		Active:          true,
		AddedSlot:       150,
		ExpirationEpoch: 2 + inactivity,
	}))
	seedRollbackCertificate(
		t, db, 150, rollbackStakeDelegationCertificate(cred),
	)
	seedRollbackCertificate(
		t, db, 250, rollbackStakeDelegationCertificate(cred),
	)

	runRollbackRecompute(t, ls, db, 199)

	acct, err := db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2)+inactivity, acct.ExpirationEpoch)
}

// TestRecomputeAccountExpirationsAfterRollbackClampsToActivationFloor is the
// review-fix scenario: the one-time CIP-0163 activation stamp gives every
// pre-existing account expiration = A + W without leaving a witness, so a pure
// witness-history recompute would mis-restore an account whose only
// post-activation witness is orphaned. Here W=90, activation runs at epoch
// A=500 for an account created in epoch 5 (registration cert at slot 550,
// stamped to 590 by activation). A post-activation delegation at slot 50550
// (epoch 505) renews expiration to 595, then a rollback to slot 50150 (epoch
// 501) orphans it. The only surviving witness is the epoch-5 registration, so
// an unclamped recompute would produce 5+90=95. The activation floor (A=500,
// account was stamped at activation) must clamp it back up to 500+90=590 — NOT 95,
// NOT 0.
func TestRecomputeAccountExpirationsAfterRollbackClampsToActivationFloor(t *testing.T) {
	const (
		inactivity      = uint64(90)
		activationEpoch = uint64(500)
	)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)

	cred := renewTestCred(0x04)
	// Account created in epoch 5 (slot 550): pre-activation registration only.
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      cred,
		CredentialTag:   0,
		Active:          true,
		AddedSlot:       550,
		CreatedSlot:     550,
		ExpirationEpoch: 0,
	}))
	// Pre-activation registration witness at slot 550 (epoch 5).
	seedRollbackCertificate(
		t, db, 550, rollbackStakeRegistrationCertificate(cred),
	)

	// One-time activation at epoch A=500 stamps expiration = 500 + 90 = 590 and
	// durably records the activation epoch in the marker.
	require.NoError(t, runActivate(t, ls, db, activationEpoch))
	acct, err := db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Equal(t, activationEpoch+inactivity, acct.ExpirationEpoch,
		"activation must stamp A+W")

	// Post-activation delegation witness at slot 50550 (epoch 505) renews the
	// expiration to 505 + 90 = 595 (as block application would).
	seedRollbackCertificate(
		t, db, 50550, rollbackStakeDelegationCertificate(cred),
	)
	require.NoError(t, db.RenewAccountExpirations(
		[]models.StakeCredentialRef{models.NewStakeCredentialRef(0, cred)},
		505+inactivity,
		nil,
	))

	// Roll back into epoch 501 (slot 50150): the epoch-505 delegation is
	// orphaned, leaving only the epoch-5 registration surviving.
	runRollbackRecompute(t, ls, db, 50150)

	acct, err = db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Equal(t, activationEpoch+inactivity, acct.ExpirationEpoch,
		"must clamp to activation floor A+W (590), not cert+W (95) or 0")
}

// TestRecomputeAccountExpirationsAfterRollbackResetsOrphanOnly verifies that a
// credential whose only witness was rolled away (no surviving witness <=
// rollbackSlot) has its expiration reset to 0.
func TestRecomputeAccountExpirationsAfterRollbackResetsOrphanOnly(t *testing.T) {
	const inactivity = uint64(90)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)

	cred := renewTestCred(0x03)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      cred,
		CredentialTag:   0,
		Active:          true,
		AddedSlot:       150,
		ExpirationEpoch: 2 + inactivity,
	}))
	// Only witness is at slot 250 (E2), which is rolled away by a rollback to
	// slot 199.
	seedRollbackCertificate(
		t, db, 250, rollbackStakeDelegationCertificate(cred),
	)

	runRollbackRecompute(t, ls, db, 199)

	acct, err := db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), acct.ExpirationEpoch)
}

// TestRecomputeAccountExpirationsAfterRollbackActivationMembership verifies
// that every account stamped at activation is reconstructed when rollback
// crosses before A. The account with no surviving witness resets to 0, while
// the account with an epoch-A witness restores its earlier epoch-1 witness.
func TestRecomputeAccountExpirationsAfterRollbackActivationMembership(t *testing.T) {
	const (
		inactivity      = uint64(90)
		activationEpoch = uint64(2)
	)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)

	// Activation-stamped account: expiration = A+W = 92, created before
	// activation, no witness rows.
	stamped := renewTestCred(0x51)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      stamped,
		CredentialTag:   0,
		Active:          true,
		AddedSlot:       50,
		CreatedSlot:     50,
		ExpirationEpoch: activationEpoch + inactivity,
	}))

	// Coincidentally-92 account: value came from an epoch-2 witness (slot 250),
	// with a surviving epoch-1 witness (slot 150).
	witnessed := renewTestCred(0x52)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      witnessed,
		CredentialTag:   0,
		Active:          true,
		AddedSlot:       150,
		CreatedSlot:     150,
		ExpirationEpoch: activationEpoch + inactivity,
	}))
	seedRollbackCertificate(
		t, db, 150, rollbackStakeDelegationCertificate(witnessed),
	)
	seedRollbackCertificate(
		t, db, 250, rollbackStakeDelegationCertificate(witnessed),
	)
	require.NoError(t, runActivate(t, ls, db, activationEpoch))

	// Roll back into epoch 1 (before activation epoch 2): the reset targets both
	// 92-valued rows, then the affected-set recompute corrects the witnessed one.
	runRollbackRecompute(t, ls, db, 199)

	stampedAcct, err := db.GetAccountByCredential(0, stamped, true, nil)
	require.NoError(t, err)
	require.Zero(t, stampedAcct.ExpirationEpoch,
		"activation-stamped account resets to 0 (its pre-activation value)")

	witnessedAcct, err := db.GetAccountByCredential(0, witnessed, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1)+inactivity, witnessedAcct.ExpirationEpoch,
		"epoch-2-witnessed account is recomputed from its surviving epoch-1 witness, not left at 0")
}

// TestRecomputeAccountExpirationsAfterRollbackRestoresPreActivationWitness
// verifies that crossing back before activation restores the expiration that
// activation overwrote. The credential has only an epoch-(A-1) witness, so it
// is absent from the ordinary post-rollback affected set; the activation-reset
// credential set must bring it into the witness-history recomputation.
func TestRecomputeAccountExpirationsAfterRollbackRestoresPreActivationWitness(
	t *testing.T,
) {
	const (
		inactivity      = uint64(90)
		activationEpoch = uint64(2)
	)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)
	cred := renewTestCred(0x53)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:      cred,
		CredentialTag:   0,
		Active:          true,
		AddedSlot:       150,
		CreatedSlot:     150,
		ExpirationEpoch: uint64(1) + inactivity,
	}))
	seedRollbackCertificate(
		t, db, 150, rollbackStakeDelegationCertificate(cred),
	)

	require.NoError(t, runActivate(t, ls, db, activationEpoch))
	acct, err := db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Equal(t, activationEpoch+inactivity, acct.ExpirationEpoch,
		"activation must replace the shorter pre-activation witness expiration")

	runRollbackRecompute(t, ls, db, 199)

	acct, err = db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1)+inactivity, acct.ExpirationEpoch,
		"rollback before activation must restore the epoch-(A-1) witness expiration")
}

func TestRecomputeAccountExpirationsAfterRollbackDoesNotFloorAccountInactiveAtActivation(
	t *testing.T,
) {
	const (
		inactivity      = uint64(90)
		activationEpoch = uint64(2)
	)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)
	cred := renewTestCred(0x54)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    cred,
		CredentialTag: 0,
		Active:        false,
		AddedSlot:     50,
		CreatedSlot:   50,
	}))
	seedRollbackCertificate(
		t, db, 50, rollbackStakeRegistrationCertificate(cred),
	)
	seedRollbackCertificate(
		t, db, 150, rollbackStakeDeregistrationCertificate(cred),
	)

	// The account exists but is inactive at A, so activation must not record it
	// in the durable stamped-membership set.
	require.NoError(t, runActivate(t, ls, db, activationEpoch))

	// A later registration is rolled away. The surviving deregistration is a
	// witness in epoch 1, and must not be clamped to activation epoch 2.
	seedRollbackCertificate(
		t, db, 250, rollbackStakeRegistrationCertificate(cred),
	)
	require.NoError(t, db.RenewAccountExpirations(
		[]models.StakeCredentialRef{models.NewStakeCredentialRef(0, cred)},
		2+inactivity,
		nil,
	))

	runRollbackRecompute(t, ls, db, 225)

	acct, err := db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1)+inactivity, acct.ExpirationEpoch)
}

func TestRecomputeAccountExpirationsAfterRollbackBeforeActivation(t *testing.T) {
	const inactivity = uint64(90)
	ls, db := newExpiryRollbackTestLedger(t, true, inactivity)
	cred := renewTestCred(0x44)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: cred, Active: true, ExpirationEpoch: 2 + inactivity,
	}))
	require.NoError(t, runActivate(t, ls, db, 2))

	runRollbackRecompute(t, ls, db, 199) // epoch 1, before activation epoch 2

	acct, err := db.GetAccountByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.Zero(t, acct.ExpirationEpoch)
	marker, err := db.GetSyncState(delegatorInactivityActivatedSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, marker)
}
