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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// incorrectRefundSubstring is the message
// conway.CertificateRefundIncorrectError renders. These tests match on the
// message rather than on a rule index, which is an offset into an upstream
// slice and moves whenever gouroboros inserts or reorders a rule.
const incorrectRefundSubstring = "incorrect refund for certificate type 17"

const (
	// Deliberately different values. drepRefundTestPparamDeposit is what a
	// *registration* certificate must supply, and is the value a refund
	// would be judged against if the deregistration path fell back to
	// protocol parameters; drepRefundTestRecordedDeposit is what the
	// registration row actually recorded. Keeping them apart is what stops
	// these tests passing for the wrong reason.
	drepRefundTestPparamDeposit   = 1_000_000
	drepRefundTestRecordedDeposit = 500_000_000
)

func drepRefundTestPparams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
		},
		KeyDeposit:           2_000_000,
		DRepDeposit:          drepRefundTestPparamDeposit,
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

func drepRefundTestCredential(seed byte) lcommon.Credential {
	return lcommon.Credential{
		CredType: lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{seed}, lcommon.AddressHashSize),
		),
	}
}

// drepDeregistrationTx builds a real *conway.ConwayTransaction carrying a
// single DRep deregistration, which is the certificate whose refund
// conway.UtxoValidateCertificateDeposits checks against the deposit the
// ledger state reports for the credential.
func drepDeregistrationTx(
	cred lcommon.Credential,
	refund int64,
) *conway.ConwayTransaction {
	cert := &lcommon.DeregistrationDrepCertificate{
		CertType:       uint(lcommon.CertificateTypeDeregistrationDrep),
		DrepCredential: cred,
		Amount:         refund,
	}
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{
				{
					Type: uint(
						lcommon.CertificateTypeDeregistrationDrep,
					),
					Certificate: cert,
				},
			},
		},
	}
}

// seedImportedDrep writes the DRep and registration rows the Mithril
// ledger-state import produces: a registration_drep row with no certificate
// behind it, so certificate_id stays 0 while deposit_amount carries the real
// amount owed. On a bootstrapped node this is frequently a DRep's only
// registration row.
func seedImportedDrep(
	t *testing.T,
	db *database.Database,
	cred lcommon.Credential,
	deposit uint64,
	slot uint64,
	active bool,
) {
	t.Helper()
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	hash := append([]byte(nil), cred.Credential[:]...)
	require.NoError(t, db.Metadata().ImportDrep(
		&models.Drep{
			CredentialTag: tag,
			Credential:    hash,
			AddedSlot:     slot,
			Active:        active,
		},
		&models.RegistrationDrep{
			CredentialTag:  tag,
			DrepCredential: hash,
			AddedSlot:      slot,
			DepositAmount:  types.Uint64(deposit),
		},
		nil,
	))
}

// TestDRepDeregistrationRefundsRecordedDeposit is the regression test for the
// live rejection this fix addresses. LedgerView.DRepRegistration is the
// common.DRepState gouroboros consults for a DRep deregistration's refund;
// it built a DRepRegistration without assigning Deposit, so every refund was
// judged against zero and a certificate supplying the real deposit was
// rejected with "incorrect refund for certificate type 17: supplied
// 500000000, expected 0".
//
// The assertion is on the validation outcome through the production Conway
// rule with a real *LedgerView, not on the helper's return value, because the
// defect was a plausible internal value becoming the wrong consensus
// decision.
func TestDRepDeregistrationRefundsRecordedDeposit(t *testing.T) {
	lv, db := newStakeRefundTestView(t)
	cred := drepRefundTestCredential(0xd1)
	seedImportedDrep(t, db, cred, drepRefundTestRecordedDeposit, 100, true)
	pp := drepRefundTestPparams()

	// Balanced at the recorded deposit: accepted.
	require.NoError(t, conway.UtxoValidateCertificateDeposits(
		drepDeregistrationTx(cred, drepRefundTestRecordedDeposit),
		200,
		lv,
		pp,
	))

	// A refund of zero is what the defect accepted, and it must now be
	// rejected. This is the assertion that cannot hold both before and
	// after the fix.
	require.ErrorContains(
		t,
		conway.UtxoValidateCertificateDeposits(
			drepDeregistrationTx(cred, 0),
			200,
			lv,
			pp,
		),
		incorrectRefundSubstring,
	)

	// And the recorded value must win over the protocol parameter, so the
	// acceptance above is not a fallback to DRepDeposit that happened to
	// balance.
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

	// Supporting evidence for why the acceptance holds.
	reg, err := lv.DRepRegistration(cred.Credential)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Equal(t, uint64(drepRefundTestRecordedDeposit), reg.Deposit)
}

// TestDRepRegistrationsReportRecordedDeposits covers the plural view, which
// gouroboros declares on common.DRepState alongside the singular form. It
// reads its deposits through the batched query, so this is what executes
// GetDrepLastRegistrationDeposits' derived-table join.
func TestDRepRegistrationsReportRecordedDeposits(t *testing.T) {
	lv, db := newStakeRefundTestView(t)
	active := drepRefundTestCredential(0xd2)
	inactive := drepRefundTestCredential(0xd3)
	seedImportedDrep(t, db, active, drepRefundTestRecordedDeposit, 100, true)
	// Registered once, since deregistered. Its registration history must
	// not appear in a listing of active DReps.
	seedImportedDrep(t, db, inactive, 900_000_000, 101, false)

	regs, err := lv.DRepRegistrations()
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Equal(t, active.Credential, regs[0].Credential)
	require.Equal(t, uint64(drepRefundTestRecordedDeposit), regs[0].Deposit)
}

// TestDRepRegistrationReportsZeroForUnregisteredCredential pins the absence
// case the batched map leaves out entirely: a DRep row with no
// registration_drep history reports a deposit of 0 rather than an error,
// through both views.
func TestDRepRegistrationReportsZeroForUnregisteredCredential(t *testing.T) {
	lv, db := newStakeRefundTestView(t)
	cred := drepRefundTestCredential(0xd4)
	tag, err := models.CredentialTagFromUint(uint(cred.CredType))
	require.NoError(t, err)
	require.NoError(t, db.CreateDrep(nil, &models.Drep{
		CredentialTag: tag,
		Credential:    append([]byte(nil), cred.Credential[:]...),
		AddedSlot:     100,
		Active:        true,
	}))

	reg, err := lv.DRepRegistration(cred.Credential)
	require.NoError(t, err)
	require.NotNil(t, reg)
	require.Zero(t, reg.Deposit)

	regs, err := lv.DRepRegistrations()
	require.NoError(t, err)
	require.Len(t, regs, 1)
	require.Zero(t, regs[0].Deposit)
}
