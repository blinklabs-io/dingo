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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

// inconsistentDepositSubstring is the message
// conway.DRepDepositStateInconsistentError renders when a registration
// reports no recorded deposit. Matched on the message rather than on a rule
// index, which moves whenever gouroboros inserts or reorders a rule.
const inconsistentDepositSubstring = "registered DRep credential has no recorded deposit"

// newDrepFallbackTestView returns a *LedgerView whose published consensus
// snapshot is Conway with drepRefundTestPparams, so the unknown-deposit
// fallback resolves through the same era certificate-deposit function the
// certificate write path uses. newStakeRefundTestView leaves the snapshot
// unpublished and the era Shelley, which is the "current era charges no DRep
// deposit" case rather than this one.
func newDrepFallbackTestView(
	t *testing.T,
) (*LedgerView, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = drepRefundTestPparams()
	ls.publishSnapshotsLocked()
	return &LedgerView{ls: ls}, db
}

// seedActiveDrepWithoutRegistration reproduces the state the vote-replay
// recovery path leaves behind. ledger/governance/processing.go calls
// InsertDrepIfAbsent when a valid DRep vote proves the credential exists
// on-chain but the metadata row was lost during recovery or bootstrap; that
// writes an active drep row and no registration_drep row at all, so the
// credential is registered with no recorded deposit.
func seedActiveDrepWithoutRegistration(
	t *testing.T,
	db *database.Database,
	cred lcommon.Credential,
	slot uint64,
) {
	t.Helper()
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	require.NoError(t, db.InsertDrepIfAbsent(
		tag,
		cred.Credential[:],
		slot,
		"",
		nil,
		true,
		nil,
	))
	// The recovery path really does leave no registration row behind; if it
	// ever starts writing one this test is measuring the wrong thing.
	recorded, err := db.GetDrepLastRegistrationDeposit(
		tag,
		cred.Credential[:],
		nil,
	)
	require.NoError(t, err)
	require.Nil(
		t,
		recorded,
		"the recovery path must leave no recorded deposit for this test to exercise the fallback",
	)
}

// TestDrepDeregistrationFallsBackToCurrentDepositWhenUnrecorded is the
// regression test. An active DRep with no registration_drep row reported
// Deposit == nil, and gouroboros fails closed on that
// (DRepDepositStateInconsistentError), so the deregistration was rejected and
// a node reaching that block stopped making progress. The refund is now
// judged against the DRep deposit the current protocol parameters charge,
// which is what the certificate write path would have recorded.
func TestDrepDeregistrationFallsBackToCurrentDepositWhenUnrecorded(
	t *testing.T,
) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe1)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	pp := drepRefundTestPparams()
	tx := drepDeregistrationTx(cred, drepRefundTestPparamDeposit)
	// The rule is invoked directly so a nil error means the refund
	// comparison actually ran and matched, rather than that the substring
	// was absent because an unrelated rule failed first.
	require.NoError(
		t,
		conway.UtxoValidateCertificateDeposits(tx, 200, lv, pp),
	)
	if err := eras.ValidateTxConway(tx, 200, lv, pp); err != nil {
		require.NotContains(t, err.Error(), inconsistentDepositSubstring)
		require.NotContains(t, err.Error(), incorrectRefundSubstring)
	}

	// Supporting evidence for why the acceptance holds.
	reg, err := lv.DRepRegistration(cred.Credential)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.NotNil(
		t,
		reg.Deposit,
		"an unrecorded deposit must be reported as the current parameter, not as absence",
	)
	require.Equal(t, uint64(drepRefundTestPparamDeposit), *reg.Deposit)
}

// TestDrepDeregistrationRejectsWrongRefundWhenUnrecorded is the mandatory
// negative case: the fallback must not become a licence to accept any refund.
// A deregistration over the same unrecorded registration that supplies a
// different amount is still rejected, including the zero the absence would
// have been misread as.
func TestDrepDeregistrationRejectsWrongRefundWhenUnrecorded(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe2)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	pp := drepRefundTestPparams()
	for _, refund := range []int64{0, drepRefundTestPparamDeposit + 1, -1} {
		err := conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, refund),
			200,
			lv,
			pp,
		)
		require.ErrorContains(t, err, incorrectRefundSubstring)
	}
}

// TestDrepDeregistrationPrefersRecordedDepositOverFallback is the second
// mandatory negative case. The recorded deposit and the current parameter are
// deliberately different values, so the two possible answers give opposite
// outcomes and the test cannot pass by accident: a recorded deposit must win,
// and the fallback must not be reached.
func TestDrepDeregistrationPrefersRecordedDepositOverFallback(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe3)
	require.NotEqual(
		t,
		uint64(drepRefundTestPparamDeposit),
		uint64(drepRefundTestRecordedDeposit),
		"the recorded deposit must differ from the parameter to discriminate",
	)
	seedImportedDrep(t, db, cred, drepRefundTestRecordedDeposit, 100, true)

	pp := drepRefundTestPparams()
	require.NoError(t, conway.UtxoValidateCertificateDeposits(
		drepDeregistrationTx(cred, drepRefundTestRecordedDeposit),
		200,
		lv,
		pp,
	))
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)
}

// TestDrepDeregistrationKeepsRecordedZeroAuthoritative pins the distinction
// the fallback must preserve. A zero dRepDeposit is a legitimate
// configuration, so a recorded zero is a real value: folding it into the
// unrecorded case would refund the current parameter and reject a valid
// deregistration.
func TestDrepDeregistrationKeepsRecordedZeroAuthoritative(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	cred := drepRefundTestCredential(0xe4)
	seedImportedDrep(t, db, cred, 0, 100, true)

	reg, err := lv.DRepRegistration(cred.Credential)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.NotNil(
		t,
		reg.Deposit,
		"a recorded zero must stay a value, not become absence",
	)
	require.Equal(t, uint64(0), *reg.Deposit)

	pp := drepRefundTestPparams()
	require.NoError(t, conway.UtxoValidateCertificateDeposits(
		drepDeregistrationTx(cred, 0),
		200,
		lv,
		pp,
	))
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)
}

// TestDrepRegistrationsFallBackForUnrecordedDeposit covers the plural view,
// which builds its deposits from a batched map lookup and so has its own
// absence path.
func TestDrepRegistrationsFallBackForUnrecordedDeposit(t *testing.T) {
	lv, db := newDrepFallbackTestView(t)
	unrecorded := drepRefundTestCredential(0xe5)
	recorded := drepRefundTestCredential(0xe6)
	seedActiveDrepWithoutRegistration(t, db, unrecorded, 100)
	seedImportedDrep(t, db, recorded, drepRefundTestRecordedDeposit, 101, true)

	registrations, err := lv.DRepRegistrations()
	require.NoError(t, err)
	byCredential := map[string]*uint64{}
	for _, reg := range registrations {
		byCredential[string(reg.Credential[:])] = reg.Deposit
	}

	got := byCredential[string(unrecorded.Credential[:])]
	require.NotNil(t, got, "the plural view must not report absence either")
	require.Equal(t, uint64(drepRefundTestPparamDeposit), *got)

	got = byCredential[string(recorded.Credential[:])]
	require.NotNil(t, got)
	require.Equal(t, uint64(drepRefundTestRecordedDeposit), *got)
}

// TestDrepRegistrationReportsAbsenceInAPreConwayEra keeps the fallback from
// inventing a refund where the current era charges no DRep deposit.
//
// The era has to be published for this to mean anything. CertDepositShelley
// through CertDepositBabbage have no *RegistrationDrepCertificate case and
// fall through to "default: return 0, nil", so before the drepDepositParams
// guard this reported a non-nil zero and gouroboros accepted a zero refund
// instead of failing closed.
func TestDrepRegistrationReportsAbsenceInAPreConwayEra(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.BabbageEraDesc
	ls.currentPParams = &babbage.BabbageProtocolParameters{
		KeyDeposit: 2_000_000,
		MaxTxSize:  16_384,
	}
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}
	// The era's own deposit function really does answer zero-without-error
	// for a DRep registration, which is what makes the guard load-bearing
	// rather than defensive.
	deposit, err := eras.BabbageEraDesc.CertDepositFunc(
		&lcommon.RegistrationDrepCertificate{},
		ls.currentPParams,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(0), deposit)

	cred := drepRefundTestCredential(0xe7)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	reg, err := lv.DRepRegistration(cred.Credential)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Nil(
		t,
		reg.Deposit,
		"a pre-Conway era has no DRep deposit to fall back to; absence must be reported",
	)

	registrations, err := lv.DRepRegistrations()
	require.NoError(t, err)
	require.Len(t, registrations, 1)
	require.Nil(
		t,
		registrations[0].Deposit,
		"the plural view must report absence in a pre-Conway era too",
	)

	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			drepRefundTestPparams(),
		),
		inconsistentDepositSubstring,
	)
}

// TestDrepRegistrationReportsAbsenceWithoutAPublishedSnapshot covers the other
// way the fallback has nothing to report: no consensus snapshot has been
// published yet, so there are no current parameters to read. Kept separate
// from the pre-Conway case because newStakeRefundTestView reaches this branch
// and never the era one -- the era field it sets is never read.
func TestDrepRegistrationReportsAbsenceWithoutAPublishedSnapshot(t *testing.T) {
	lv, db := newStakeRefundTestView(t)
	require.Nil(
		t,
		lv.ls.loadConsensusSnapshot(),
		"this fixture must leave the snapshot unpublished for this test to mean what it says",
	)
	cred := drepRefundTestCredential(0xe8)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	reg, err := lv.DRepRegistration(cred.Credential)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Nil(t, reg.Deposit)

	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			drepRefundTestPparams(),
		),
		inconsistentDepositSubstring,
	)
}

// TestDrepRegistrationReportsAbsenceForTypedNilParams covers the other way the
// capability assertion can be satisfied without a usable deposit behind it.
//
// A typed-nil *DijkstraProtocolParameters implements drepDepositParams, so the
// call-site guard admits it; CertDepositDijkstra then asserted the type
// successfully and dereferenced nil, panicking inside DRep view construction.
// It now reports ErrIncompatibleProtocolParams, which currentDRepDeposit turns
// into absence, so gouroboros fails closed as it does for every other
// no-deposit-available case.
//
// The two guards are complementary and both are asserted here: the era helper
// is what stops the panic, and the call-site capability check is what keeps a
// pre-Conway era from reaching it at all.
func TestDrepRegistrationReportsAbsenceForTypedNilParams(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	ls.currentEra = eras.DijkstraEraDesc
	ls.currentPParams = (*dijkstra.DijkstraProtocolParameters)(nil)
	ls.publishSnapshotsLocked()
	lv := &LedgerView{ls: ls}

	// The typed nil really does satisfy the capability the call-site guard
	// tests, so this case reaches the era helper rather than stopping early.
	_, implements := ls.currentPParams.(drepDepositParams)
	require.True(
		t,
		implements,
		"a typed-nil pointer must still satisfy drepDepositParams for this test to exercise the era helper",
	)

	cred := drepRefundTestCredential(0xe9)
	seedActiveDrepWithoutRegistration(t, db, cred, 100)

	require.NotPanics(t, func() {
		reg, err := lv.DRepRegistration(cred.Credential)
		require.NoError(t, err)
		require.NotNil(t, reg)
		require.Nil(
			t,
			reg.Deposit,
			"unusable parameters must report absence, not a fabricated deposit",
		)
	})
	require.NotPanics(t, func() {
		registrations, err := lv.DRepRegistrations()
		require.NoError(t, err)
		require.Len(t, registrations, 1)
		require.Nil(t, registrations[0].Deposit)
	})

	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, drepRefundTestPparamDeposit),
			200,
			lv,
			drepRefundTestPparams(),
		),
		inconsistentDepositSubstring,
	)
}
