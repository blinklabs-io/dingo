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
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// valueNotConservedSubstring is the message
// shelley.ValueNotConservedUtxoError renders. These tests match on that
// message and never on the rule index: the index is an offset into the
// upstream gouroboros slice and moves whenever upstream inserts or reorders a
// rule. It printed as 32 on v0.202.5 and prints as 33 on the currently pinned
// v0.202.6, which inserted UtxoValidateCurrentTreasuryValue at index 0.
const valueNotConservedSubstring = "value not conserved"

const stakeRefundTestKeyDeposit = 2_000_000

// stakeRefundTestPparams returns Conway protocol parameters whose KeyDeposit
// is the value a legacy stake deregistration falls back to when the ledger
// state cannot report the deposit recorded at registration.
func stakeRefundTestPparams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 9,
		},
		KeyDeposit:           stakeRefundTestKeyDeposit,
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

// stakeDeregistrationTx builds a real *conway.ConwayTransaction carrying a
// single legacy stake deregistration and no inputs or outputs, so value
// conservation reduces to "refund must equal fee". The refund is the only
// consumed value and the fee is the only produced value, which isolates the
// recorded-deposit lookup from every other term in the equation.
func stakeDeregistrationTx(
	cred lcommon.Credential,
	fee uint64,
) *conway.ConwayTransaction {
	cert := &lcommon.StakeDeregistrationCertificate{
		CertType:        uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: cred,
	}
	return &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxFee: fee,
			TxCertificates: []lcommon.CertificateWrapper{
				{
					Type: uint(
						lcommon.CertificateTypeStakeDeregistration,
					),
					Certificate: cert,
				},
			},
		},
	}
}

// seedStakeRegistration drives a stake registration through the production
// certificate write path, which is what decides whether the recorded deposit
// lands in the database as a value or as NULL. Passing a nil deposit omits the
// certificate index from the certDeposits map exactly as
// ledger.calculateCertificateDeposit and backfill.calculateCertDeposits do
// when the deposit cannot be computed.
func seedStakeRegistration(
	t *testing.T,
	db *database.Database,
	cred lcommon.Credential,
	deposit *uint64,
	slot uint64,
	seed byte,
) {
	t.Helper()
	builder := mockledger.NewTransactionBuilder()
	builder.WithId(bytes.Repeat([]byte{seed}, 32))
	builder.WithValid(true)
	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{seed + 1}, 32),
		0,
	)
	require.NoError(t, err)
	builder.WithInputs(input)
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd").
		WithLovelace(1_000_000).
		Build()
	require.NoError(t, err)
	builder.WithOutputs(output)
	builder.WithCertificates(&lcommon.StakeRegistrationCertificate{
		StakeCredential: cred,
	})
	tx, err := builder.Build()
	require.NoError(t, err)
	certDeposits := map[int]uint64{}
	if deposit != nil {
		certDeposits[0] = *deposit
	}
	require.NoError(t, db.SetTransactionMetadataOnly(
		tx,
		ocommon.NewPoint(slot, bytes.Repeat([]byte{seed + 2}, 32)),
		0,
		certDeposits,
		nil,
	))
}

// newStakeRefundTestView returns a *LedgerView over a real database, built
// from the same *LedgerState the other end-to-end validation tests use so the
// Conway rules that read genesis configuration (network ids, slot
// conversion) run rather than panic.
func newStakeRefundTestView(
	t *testing.T,
) (*LedgerView, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	return &LedgerView{ls: ls}, db
}

func stakeRefundTestCredential(seed byte) lcommon.Credential {
	return lcommon.Credential{
		CredType: lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(
			bytes.Repeat([]byte{seed}, lcommon.AddressHashSize),
		),
	}
}

// requireValueConserved asserts the transaction clears value conservation
// through the production Conway validation path. Other rules error on these
// deliberately minimal transactions (the input set is empty by design), so the
// assertion is on the absence of the value-conservation failure specifically,
// which is what the recorded-deposit refund decides.
func requireValueConserved(
	t *testing.T,
	lv *LedgerView,
	tx *conway.ConwayTransaction,
) {
	t.Helper()
	err := eras.ValidateTxConway(tx, 200, lv, stakeRefundTestPparams())
	if err != nil {
		require.NotContains(t, err.Error(), valueNotConservedSubstring)
	}
}

func requireValueNotConserved(
	t *testing.T,
	lv *LedgerView,
	tx *conway.ConwayTransaction,
) {
	t.Helper()
	err := eras.ValidateTxConway(tx, 200, lv, stakeRefundTestPparams())
	require.Error(t, err)
	require.Contains(t, err.Error(), valueNotConservedSubstring)
}

// TestValueConservationRefundsUnknownStakeDepositAtKeyDeposit is the
// regression test for #3829. A registration ingested without a computable
// deposit records NULL, LedgerView.StakeCredentialDeposit reports absence, and
// gouroboros' UtxoValidateValueNotConservedUtxo falls back to the current
// KeyDeposit. Before the fix the three zero-reporting sites stored an
// authoritative 0, the rule refunded 0, and this otherwise valid transaction
// failed value conservation.
//
// The assertion is on acceptance through eras.ValidateTxConway with a real
// *LedgerView, not on the helper's return value, because the defect was that
// a plausible internal value became the wrong validation outcome.
func TestValueConservationRefundsUnknownStakeDepositAtKeyDeposit(
	t *testing.T,
) {
	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc1)
	seedStakeRegistration(t, db, cred, nil, 100, 0xc1)

	// The refund falls back to KeyDeposit, so a fee of exactly KeyDeposit
	// conserves value. This acceptance is the assertion that carries the
	// regression: it is the validation outcome, one layer above the recorded
	// value that produces it.
	requireValueConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit),
	)

	// Supporting evidence for why the acceptance holds: the recorded deposit
	// is genuinely absent rather than a zero that happened to balance.
	recorded, err := lv.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	assert.Nil(
		t,
		recorded,
		"an uncomputable registration deposit must be recorded as absent, not zero",
	)
}

// TestValueConservationRejectsUnbalancedUnknownStakeDeposit is the mandatory
// negative case: the KeyDeposit fallback must not become a licence to pass
// value conservation for any fee. A genuinely unbalanced transaction over the
// same absent-deposit registration is still rejected.
func TestValueConservationRejectsUnbalancedUnknownStakeDeposit(t *testing.T) {
	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc2)
	seedStakeRegistration(t, db, cred, nil, 100, 0xc2)

	requireValueNotConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit+1_000_000),
	)
}

// TestValueConservationRefundsRecordedStakeDepositNotKeyDeposit is the second
// mandatory negative case: a correctly recorded non-zero deposit must be
// refunded at its recorded value, never at the current KeyDeposit. The
// recorded 5 ADA deliberately differs from the 2 ADA KeyDeposit, so the two
// possible refunds give opposite outcomes and the test cannot pass by
// accident.
func TestValueConservationRefundsRecordedStakeDepositNotKeyDeposit(
	t *testing.T,
) {
	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc3)
	recordedDeposit := uint64(5_000_000)
	require.NotEqual(
		t,
		uint64(stakeRefundTestKeyDeposit),
		recordedDeposit,
		"the recorded deposit must differ from KeyDeposit for this test to discriminate",
	)
	seedStakeRegistration(t, db, cred, &recordedDeposit, 100, 0xc3)

	got, err := lv.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, recordedDeposit, *got)

	// Balanced at the recorded deposit: accepted.
	requireValueConserved(t, lv, stakeDeregistrationTx(cred, recordedDeposit))
	// Balanced at the current KeyDeposit instead: rejected, which is what
	// proves the recorded value won.
	requireValueNotConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit),
	)
}

// TestValueConservationRefundsRecordedZeroStakeDepositAsZero pins the
// distinction the fix must preserve. A recorded zero is reachable and
// authoritative: config/cardano/devnet/shelley-genesis.json sets
// "keyDeposit": 0, so every stake registration on dingo's own devnet records
// a real zero deposit. Folding zero into the unknown case would refund
// KeyDeposit there and break value conservation on the devnet, which is why
// only the uncomputable case reports absence.
func TestValueConservationRefundsRecordedZeroStakeDepositAsZero(t *testing.T) {
	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc4)
	recordedZero := uint64(0)
	seedStakeRegistration(t, db, cred, &recordedZero, 100, 0xc4)

	got, err := lv.StakeCredentialDeposit(cred)
	require.NoError(t, err)
	require.NotNil(
		t,
		got,
		"a recorded zero deposit must stay a value, not become absence",
	)
	require.Equal(t, uint64(0), *got)

	// Refunded as zero, so a zero fee conserves value.
	requireValueConserved(t, lv, stakeDeregistrationTx(cred, 0))
	// And the KeyDeposit fallback must not be taken.
	requireValueNotConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit),
	)
}
