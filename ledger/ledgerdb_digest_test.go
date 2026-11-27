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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// stripRowIDs removes the row IDs from a decoded row: they count writes, so
// two ledgers that reached one state by different routes number them
// differently.
func stripRowIDs(v any) any {
	switch x := v.(type) {
	case map[string]any:
		delete(x, "ID")
		for k, e := range x {
			x[k] = stripRowIDs(e)
		}
	case []any:
		for i, e := range x {
			x[i] = stripRowIDs(e)
		}
	}
	return v
}

// ledgerDBDigest reads the ledger state the LedgerDB holds through its own
// interface, as sorted text rows, so two ledgers' state can be compared by
// value: epochs and their summaries, pool stake snapshots, the reward inputs
// and outputs of each epoch, treasury and donations, protocol parameters,
// governance proposals, DReps and accounts, and the transaction index.
func ledgerDBDigest(t *testing.T, ls *LedgerState) string {
	t.Helper()
	ldb := ls.ledgerDB()
	txn := ldb.Begin(t.Context(), false)
	defer txn.Release()
	var out strings.Builder
	emit := func(name string, rows ...any) {
		lines := make([]string, 0, len(rows))
		for _, r := range rows {
			raw, err := json.Marshal(r)
			require.NoError(t, err, name)
			var decoded any
			require.NoError(t, json.Unmarshal(raw, &decoded), name)
			norm, err := json.Marshal(stripRowIDs(decoded))
			require.NoError(t, err, name)
			lines = append(lines, string(norm))
		}
		sort.Strings(lines)
		fmt.Fprintf(&out, "## %s\n%s\n", name, strings.Join(lines, "\n"))
	}
	epochs, err := ldb.GetEpochs(txn)
	require.NoError(t, err)
	emit("epoch", asAny(epochs)...)
	for _, e := range epochs {
		name := func(table string) string { return fmt.Sprintf("%s/%d", table, e.EpochId) }
		summary, err := ldb.GetEpochSummary(e.EpochId, txn)
		require.NoError(t, err)
		if summary != nil {
			emit(name("epoch_summary"), summary)
		}
		for _, typ := range []string{"mark", "set", "go"} {
			snaps, err := ldb.GetPoolStakeSnapshotsByEpoch(e.EpochId, typ, txn)
			require.NoError(t, err)
			emit(name("pool_stake_snapshot/"+typ), asAny(snaps)...)
			snap, err := ldb.GetRewardSnapshot(e.EpochId, typ, txn)
			require.NoError(t, err)
			if snap != nil {
				emit(name("reward_snapshot/"+typ), snap)
			}
		}
		pots, err := ldb.GetRewardAdaPots(e.EpochId, txn)
		require.NoError(t, err)
		if pots != nil {
			emit(name("reward_ada_pots"), pots)
		}
		poolInputs, err := ldb.GetRewardPoolInputs(e.EpochId, txn)
		require.NoError(t, err)
		emit(name("reward_pool_input"), asAny(poolInputs)...)
		stakeInputs, err := ldb.GetRewardStakeInputs(e.EpochId, txn)
		require.NoError(t, err)
		emit(name("reward_stake_input"), asAny(stakeInputs)...)
		poolOutputs, err := ldb.GetRewardPoolOutputs(e.EpochId, txn)
		require.NoError(t, err)
		emit(name("reward_pool_output"), asAny(poolOutputs)...)
		accountOutputs, err := ldb.GetRewardAccountOutputs(e.EpochId, txn)
		require.NoError(t, err)
		emit(name("reward_account_output"), asAny(accountOutputs)...)
		params, err := ldb.GetPParamsRows(e.EpochId, e.EraId, txn)
		require.NoError(t, err)
		emit(name("pparams"), asAny(params)...)
	}
	state, err := ldb.GetNetworkState(txn)
	require.NoError(t, err)
	if state != nil {
		emit("network_state", state)
	}
	donations, err := ldb.ListNetworkDonations(txn)
	require.NoError(t, err)
	emit("network_donation", asAny(donations)...)
	proposals, err := ldb.GetAllGovernanceProposals(t.Context(), txn)
	require.NoError(t, err)
	emit("governance_proposal", asAny(proposals)...)
	var dreps, accounts []any
	if set, ok := ls.config.UtxoSet.(drepLister); ok {
		require.NoError(t, set.Dreps(func(d *models.Drep) error {
			dreps = append(dreps, d)
			return nil
		}))
	}
	emit("drep", dreps...)
	if set, ok := ls.config.UtxoSet.(accountLister); ok {
		require.NoError(t, set.Accounts(func(a *models.Account) error {
			accounts = append(accounts, a)
			return nil
		}))
	}
	emit("account", accounts...)
	// The transaction index is no fact table: no IndexerDB copies it.
	if index, ok := ls.config.UtxoSet.(TxIndexReader); ok {
		var lines []string
		require.NoError(t, index.EachTxIndexEntry(func(e TxIndexEntry) error {
			lines = append(lines, fmt.Sprintf(
				"hash=%x|block=%x|slot=%d|index=%d|era=%d|valid=%t",
				e.Hash, e.BlockHash, e.Slot, e.BlockIndex, e.Type, e.Valid,
			))
			return nil
		}))
		fmt.Fprintf(&out, "## tx_index\n%s\n", strings.Join(lines, "\n"))
	}
	return out.String()
}

func asAny[T any](rows []T) []any {
	out := make([]any, len(rows))
	for i, r := range rows {
		out[i] = r
	}
	return out
}
