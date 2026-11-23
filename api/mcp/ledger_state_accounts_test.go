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
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// Without a ledger state, get_governance_state counts and reads DReps from
// the drep table, so it is refused when the ledger state is in the LedgerDB.
func TestGovernanceStateWithoutLedgerStateRefusesSQLDRepReads(t *testing.T) {
	t.Parallel()
	cs := ledgerStateToolSession(t, newSchemaDB(t), true)

	text := callTool(t, cs, "get_governance_state", map[string]any{}, true)

	require.Contains(t, text, "does not hold")
	require.Contains(t, text, nodesettings.FamilyDReps.String())

	cs = ledgerStateToolSession(t, newSchemaDB(t), false)
	text = callTool(t, cs, "get_governance_state", map[string]any{}, false)
	require.NotContains(t, text, "does not hold")
}
