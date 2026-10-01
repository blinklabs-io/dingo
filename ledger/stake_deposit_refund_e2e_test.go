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
)

// TestValueConservationRejectsUnbalancedUnknownStakeDeposit is the mandatory
// negative case: the KeyDeposit fallback must not become a licence to pass
// value conservation for any fee. A genuinely unbalanced transaction over the
// same absent-deposit registration is still rejected.
func TestValueConservationRejectsUnbalancedUnknownStakeDeposit(t *testing.T) {
	t.Parallel()

	lv, db := newStakeRefundTestView(t)
	cred := stakeRefundTestCredential(0xc2)
	seedStakeRegistration(t, db, cred, nil, 100, 0xc2)

	requireValueNotConserved(
		t,
		lv,
		stakeDeregistrationTx(cred, stakeRefundTestKeyDeposit+1_000_000),
	)
}
