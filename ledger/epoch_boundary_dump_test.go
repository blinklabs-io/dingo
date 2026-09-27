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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// epochBoundaryDumpShape is small enough to seed in seconds and still covers
// the rounding and eligibility edges: margins 0 and 1, a zero-stake pool,
// owners, DRep, always-abstain and no-confidence delegators.
func epochBoundaryDumpShape() epochBoundaryBenchShape {
	shape := epochBoundaryBenchShape{
		pools:             23,
		delegators:        1_500,
		dreps:             17,
		utxosPerDelegator: 2,
		proposals:         6,
		drepVotes:         12,
		spoVotes:          9,
		ccMembers:         3,
	}
	if raw := os.Getenv("DINGO_BOUNDARY_DUMP_DELEGATORS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			shape.delegators = n
			shape.pools = max(shape.pools, n/400)
		}
	}
	return shape
}

// dumpEpochBoundaryState renders every table an epoch boundary writes, in a
// stable order and without surrogate keys, so the same boundary on two code
// versions can be compared byte for byte.
func dumpEpochBoundaryState(t *testing.T, raw *sql.DB) string {
	t.Helper()
	queries := []struct{ name, query string }{
		{"account", `SELECT credential_tag, hex(staking_key), reward, active,
    hex(pool), hex(drep), drep_type FROM account
ORDER BY credential_tag, staking_key`},
		{"account_reward_delta", `SELECT credential_tag, hex(staking_key),
    hex(tx_hash), amount, previous_reward, added_slot, withdrawal,
    post_snapshot FROM account_reward_delta
ORDER BY credential_tag, staking_key, tx_hash, added_slot, withdrawal`},
		{"reward_live_stake", `SELECT credential_tag, hex(staking_key),
    hex(pool_key_hash), utxo_stake, reward_stake, total_stake, registered,
    pool_delegation_slot, updated_slot, calculation_version
FROM reward_live_stake ORDER BY credential_tag, staking_key`},
		{"network_state", `SELECT slot, treasury, reserves FROM network_state
ORDER BY slot`},
		{"reward_ada_pots", `SELECT epoch, treasury, reserves, fees, rewards,
    captured_slot FROM reward_ada_pots ORDER BY epoch`},
		{"reward_pool_output", `SELECT epoch, hex(pool_key_hash),
    apparent_performance, optimal_reward, total_reward, leader_reward,
    member_reward_total, owner_stake, undistributed, unspendable,
    boundary_slot FROM reward_pool_output ORDER BY epoch, pool_key_hash`},
		{"reward_account_output", `SELECT epoch, credential_tag,
    hex(staking_key), hex(pool_key_hash), reward_type, amount, spendable,
    guarded, boundary_slot FROM reward_account_output
ORDER BY epoch, credential_tag, staking_key, pool_key_hash, reward_type`},
		{
			"pool_stake_snapshot",
			`SELECT epoch, snapshot_type, hex(pool_key_hash),
    total_stake, delegator_count, captured_slot, calculation_version,
    reward_account_auto_vote, reward_account_auto_vote_resolved
FROM pool_stake_snapshot ORDER BY epoch, snapshot_type, pool_key_hash`,
		},
		{"reward_snapshot", `SELECT epoch, snapshot_type, total_active_stake,
    total_pool_count, total_delegators, captured_slot, boundary_slot,
    hex(epoch_nonce), protocol_version, authoritative, calculation_version,
    excluded_active_stake FROM reward_snapshot
ORDER BY epoch, snapshot_type`},
		{"reward_pool_input", `SELECT epoch, hex(pool_key_hash), margin,
    hex(reward_account), pledge, delegated_stake, owner_stake, cost,
    delegator_count, captured_slot, boundary_slot FROM reward_pool_input
ORDER BY epoch, pool_key_hash`},
		{
			"reward_stake_input",
			`SELECT epoch, hex(pool_key_hash), credential_tag,
    hex(staking_key), stake, owner, registered, captured_slot, boundary_slot
FROM reward_stake_input
ORDER BY epoch, pool_key_hash, credential_tag, staking_key`,
		},
		{"epoch_summary", `SELECT epoch, total_active_stake, total_pool_count,
    total_delegators, hex(epoch_nonce), boundary_slot, snapshot_ready
FROM epoch_summary ORDER BY epoch`},
		{"governance_proposal", `SELECT hex(tx_hash), action_index,
    enacted_epoch, enacted_slot, ratified_epoch, ratified_slot, expired_epoch,
    expired_slot FROM governance_proposal ORDER BY tx_hash, action_index`},
		{"drep", `SELECT credential_tag, hex(credential), active,
    last_activity_epoch, expiry_epoch FROM drep
ORDER BY credential_tag, credential`},
		{"epoch", `SELECT epoch_id, start_slot, hex(nonce), hex(evolving_nonce),
    hex(candidate_nonce), era_id FROM epoch ORDER BY epoch_id`},
	}
	var sb strings.Builder
	for _, q := range queries {
		rows, err := raw.Query(q.query)
		require.NoError(t, err, q.name)
		cols, err := rows.Columns()
		require.NoError(t, err)
		fmt.Fprintf(&sb, "== %s\n", q.name)
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			for i, v := range values {
				if b, ok := v.([]byte); ok {
					v = string(b)
				}
				if i > 0 {
					sb.WriteString("|")
				}
				fmt.Fprintf(&sb, "%v", v)
			}
			sb.WriteString("\n")
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return sb.String()
}

// dumpDRepVotingPower renders every DRep's voting power as governance reads it
// at the end of the boundary.
func dumpDRepVotingPower(t *testing.T, f *epochBoundaryBenchFixture) string {
	t.Helper()
	dreps, err := f.db.GetActiveDreps(nil)
	require.NoError(t, err)
	refs := make([]models.StakeCredentialRef, 0, len(dreps))
	for _, drep := range dreps {
		refs = append(refs, models.NewStakeCredentialRef(
			drep.CredentialTag, drep.Credential,
		))
	}
	powers, err := f.db.GetDRepVotingPowerBatch(refs, 0, nil)
	require.NoError(t, err)
	byType, err := f.db.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain, models.DrepTypeAlwaysNoConfidence,
		}, 0, nil,
	)
	require.NoError(t, err)
	lines := make([]string, 0, len(powers)+2)
	for key, power := range powers {
		lines = append(lines, fmt.Sprintf("%x=%d", key, power))
	}
	sort.Strings(lines)
	lines = append(lines, fmt.Sprintf(
		"abstain=%d no_confidence=%d",
		byType[models.DrepTypeAlwaysAbstain],
		byType[models.DrepTypeAlwaysNoConfidence],
	))
	return strings.Join(lines, "\n") + "\n"
}

// deregisterEpochBoundaryDumpDelegators deregisters every 97th delegator
// during the ended epoch, after any precompute ran, the way a deregistration
// certificate does: the account, its live stake row and the certificate row.
// Their rewards become unspendable at the boundary.
func deregisterEpochBoundaryDumpDelegators(t *testing.T, raw *sql.DB) {
	t.Helper()
	shape := epochBoundaryDumpShape()
	for d := 0; d < shape.delegators; d += 97 {
		key := epochBoundaryBenchHash(0x30, uint64(d)+1)
		slot := epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch) + 1_000 +
			uint64(d)
		for _, stmt := range []string{
			`UPDATE account SET active = 0, pool = NULL, added_slot = ?
WHERE credential_tag = 0 AND staking_key = ?`,
			`UPDATE reward_live_stake SET registered = 0, pool_key_hash = NULL,
    updated_slot = ? WHERE credential_tag = 0 AND staking_key = ?`,
			`INSERT INTO deregistration (added_slot, staking_key, credential_tag,
    amount) VALUES (?, ?, 0, '2000000')`,
		} {
			_, err := raw.Exec(stmt, slot, key)
			require.NoError(t, err)
		}
	}
}

// TestEpochBoundaryDumpForDifferential runs one boundary on the dump fixture
// for each precompute state and writes the resulting state to
// $DINGO_BOUNDARY_DUMP_DIR, so the same test on two code versions produces
// files to diff. It is skipped unless that directory is set.
func TestEpochBoundaryDumpForDifferential(t *testing.T) {
	t.Parallel()
	dir := os.Getenv("DINGO_BOUNDARY_DUMP_DIR")
	if dir == "" {
		t.Skip("set DINGO_BOUNDARY_DUMP_DIR to write boundary dumps")
	}
	for _, state := range []string{"complete", "partial", "missing"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			f := newEpochBoundaryBenchFixture(t, epochBoundaryDumpShape(), "")
			switch state {
			case "complete":
				require.NoError(
					t,
					f.ls.precomputeStakeRewardsAfterEpochTransition(
						epochBoundaryBenchPrecomputeEvent(),
					),
				)
			case "partial":
				epochBoundaryBenchPartialPrecomputeT(t, f)
			}
			raw, err := dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			defer raw.Close()
			deregisterEpochBoundaryDumpDelegators(t, raw)
			f.rollover(t)
			f.ls.waitEpochBoundaryBenchBackground()
			dump := dumpEpochBoundaryState(t, raw) + "== drep_power\n" +
				dumpDRepVotingPower(t, f)
			require.NoError(t, os.WriteFile(
				filepath.Join(dir, "boundary-"+state+".txt"),
				[]byte(dump), 0o644,
			))
		})
	}
}
