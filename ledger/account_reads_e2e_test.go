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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestAccountTableReadsSeeTheirTransactionsStagedAccounts requires the
// delegator reads in a write transaction to list a script credential that
// transaction registered and delegated, first, as the store does.
func TestAccountTableReadsSeeTheirTransactionsStagedAccounts(t *testing.T) {
	t.Parallel()
	h := newLiveStakeHarness(t, false)
	script := bytes.Repeat([]byte{0x74}, 28)
	creds, count := h.scenario.RegisterCredentialInsideTxn(1, script, h.pool)
	require.Equal(t, uint64(3), count)
	require.Equal(t, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(1, script), h.owner, h.deleg,
	}, creds, "a script credential sorts before a key credential")
}
