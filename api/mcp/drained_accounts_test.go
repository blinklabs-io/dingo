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

package mcp

import (
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// get_account reads the account table, which the set serves only through the
// ledger, so a drained accounts family is refused and an undrained one is
// answered.
func TestAccountToolRefusesADrainedAccountsFamily(t *testing.T) {
	t.Parallel()
	args := map[string]any{
		"stake_address_or_credential": strings.Repeat("ab", 28),
		"credential_type":             "key",
	}
	cs := drainedToolSession(
		t, newSchemaDB(t), nodesettings.FamilyAccounts,
	)

	text := callTool(t, cs, "get_account", args, true)

	require.Contains(t, text, "no longer holds")
	require.Contains(t, text, nodesettings.FamilyAccounts.String())

	cs = drainedToolSession(t, newSchemaDB(t), nodesettings.FamilyNonces)
	text = callTool(t, cs, "get_account", args, true)
	require.NotContains(t, text, "no longer holds")
	require.Contains(t, text, "not found")
}

// Without a ledger state, get_governance_state counts and reads DReps from
// the drep table, so a drained DReps family is refused.
func TestGovernanceStateWithoutLedgerStateRefusesDrainedDReps(t *testing.T) {
	t.Parallel()
	cs := drainedToolSession(t, newSchemaDB(t), nodesettings.FamilyDReps)

	text := callTool(t, cs, "get_governance_state", map[string]any{}, true)

	require.Contains(t, text, "no longer holds")
	require.Contains(t, text, nodesettings.FamilyDReps.String())

	cs = drainedToolSession(t, newSchemaDB(t), nodesettings.FamilyNonces)
	text = callTool(t, cs, "get_governance_state", map[string]any{}, false)
	require.NotContains(t, text, "no longer holds")
}
