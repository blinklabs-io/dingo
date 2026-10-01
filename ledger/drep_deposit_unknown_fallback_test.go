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
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

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
