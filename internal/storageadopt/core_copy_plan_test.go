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

package storageadopt

import (
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/stretchr/testify/require"
)

// TestCoreCopyPlanCarriesCoreModeTables plans the consensus copy of the full
// schema and requires the tables core-mode block and epoch processing read
// from SQL, and the UTxO rows and ledger-state families the node reverts
// below the adopted tip before it moves them into the LedgerDB.
func TestCoreCopyPlanCarriesCoreModeTables(t *testing.T) {
	t.Parallel()
	plan, err := sqlstore.PlanCoreCopy(t.Context())
	require.NoError(t, err)
	if os.Getenv("DINGO_PRINT_CORE_COPY_PLAN") != "" {
		t.Logf("copied: %v", plan.Copied)
		t.Logf("dropped by foreign keys: %v", plan.DroppedByForeignKey)
		t.Logf("copied whole: %v", plan.CopiedWhole)
	}
	for _, table := range []string{
		"certs", "pool_stake_snapshot", "leios_transaction_context",
		"transaction", "applied_transaction",
		"move_instantaneous_rewards", "move_instantaneous_rewards_reward",
		"node_settings", "node_settings_gate", "sync_state", "tip",
		"commit_timestamp", "utxo", "account", "pool",
	} {
		require.Contains(t, plan.Copied, table)
	}
	for _, table := range []string{
		"address_transaction", "redeemer_script",
		"asset_identity", "asset_holding",
		"utxo_offset", "transaction_offset",
	} {
		require.NotContains(t, plan.Copied, table)
	}
	// A table without a single-column key has no cursor to resume from and
	// is copied in one transaction; a new one is a deliberate addition.
	require.Equal(t, []string{
		"account_drep_clear", "account_import_baseline",
		"account_inactivity_activation", "imported_pool_block_count",
		"pool_current_event", "reward_seed_failure",
		"utxo_collateral_edge", "utxo_reference_edge",
	}, plan.CopiedWhole)
}
