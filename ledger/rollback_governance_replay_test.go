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
	"context"
	"database/sql"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// Rolling a Conway boundary back must leave every metadata table as it was
// before the boundary, whatever the boundary ratified, enacted, expired,
// refunded or paid out, and re-applying it must reproduce the first result.
// cardano-ledger restores the whole NewEpochState snapshot, so any row a
// rollback leaves behind is derived state the Haskell node does not have.
func TestRollbackAcrossGovernanceBoundariesRestoresEveryTable(t *testing.T) {
	const boundaries = 4
	for _, tc := range []struct {
		name     string
		deferred bool
	}{
		{name: "ratified at boundary"},
		{name: "ratification deferred", deferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newGovDiffScenario(t)
			settle := func(*LedgerState) {}
			if tc.deferred {
				wireDeferredBoundarySnapshot(s.ls, s.snapshotMgr)
				settle = func(ls *LedgerState) {
					require.NoError(t, ls.WaitEpochBoundaryJob(t.Context()))
				}
			} else {
				s.ls.ratifyAtBoundary = true
			}
			raw, err := dbtest.RawSQLiteMetadata(t, s.db)
			require.NoError(t, err)
			t.Cleanup(func() { _ = raw.Close() })
			seedAccountCertificates(t, s, raw)

			exercised := map[string]bool{}
			epoch, pparams := s.epoch, s.pparams
			for boundary := 1; boundary <= boundaries; boundary++ {
				s.prepareRollover(t, epoch, pparams)
				before := dumpComparableTables(t, raw)
				preState := s.dump(t, epoch.EpochId, pparams)

				firstEpoch, firstPParams := s.commitRollover(
					t, epoch, pparams, settle,
				)
				first := s.dump(t, firstEpoch.EpochId, firstPParams)
				firstTables := dumpComparableTables(t, raw)

				require.NoError(t, s.db.RollbackMetadataAfterSlot(
					context.Background(),
					ocommon.Point{Slot: firstEpoch.StartSlot - 1},
					0,
					nil,
				))
				after := dumpComparableTables(t, raw)
				require.Equal(t, preState,
					s.dump(t, epoch.EpochId, pparams),
					"boundary %d: the rollback must restore the pre-boundary "+
						"proposals, committee, treasury, rewards and DRep power",
					boundary,
				)
				for table := range changedTables(before, firstTables) {
					exercised[table] = true
				}
				diffs := diffMetadataTables(before, after)
				for _, d := range diffs {
					t.Logf("boundary %d: %s", boundary, d)
				}
				require.Empty(t, diffs, "boundary %d", boundary)

				s.ls.currentEpoch = epoch
				s.ls.currentPParams = pparams
				secondEpoch, secondPParams := s.commitRollover(
					t, epoch, pparams, settle,
				)
				second := s.dump(t, secondEpoch.EpochId, secondPParams)
				require.Equal(t, first, second, "boundary %d", boundary)
				require.Equal(t,
					normalizeReplayEpoch(firstEpoch),
					normalizeReplayEpoch(secondEpoch),
				)
				secondTables := dumpComparableTables(t, raw)
				require.Empty(t,
					diffMetadataTables(
						withoutRowIDs(firstTables),
						withoutRowIDs(secondTables),
					),
					"boundary %d: replay must reproduce the first application",
					boundary,
				)

				epoch, pparams = secondEpoch, secondPParams
				s.ls.currentEpoch = epoch
				s.ls.currentPParams = pparams
			}
			// The boundaries must have changed every kind of state this test
			// claims a rollback restores; an unchanged table proves nothing.
			for _, table := range []string{
				"governance_proposal",
				"governance_proposal_ratification_history",
				"governance_proposal_drop",
				"committee_member",
				"committee_quorum",
				"pparams",
				"network_state",
				"account_reward_delta",
				"drep",
				"drep_dormancy_state",
				"drep_expiry_history",
				"reward_ada_pots",
				"pool_stake_snapshot",
				"epoch_summary",
			} {
				require.True(t, exercised[table],
					"no boundary changed %s", table)
			}
		})
	}
}

// withoutRowIDs drops each row's own autoincrement id, which a re-applied
// boundary legitimately allocates anew.
func withoutRowIDs(tables map[string][]string) map[string][]string {
	out := make(map[string][]string, len(tables))
	for table, lines := range tables {
		stripped := make([]string, 0, len(lines))
		for _, line := range lines {
			fields := strings.Fields(line)
			kept := fields[:0]
			for _, f := range fields {
				// A history or reward row's vote_id or mir_id names its
				// parent row, whose own id a re-applied boundary allocates
				// anew. A pool_current_event row's event_id names the history
				// row it was derived from, which a replay re-inserts under a
				// new id.
				if !strings.HasPrefix(f, "id=") &&
					!strings.HasPrefix(f, "vote_id=") &&
					!strings.HasPrefix(f, "mir_id=") &&
					!strings.HasPrefix(f, "event_id=") {
					kept = append(kept, f)
				}
			}
			stripped = append(stripped, strings.Join(kept, " "))
		}
		sort.Strings(stripped)
		out[table] = stripped
	}
	return out
}

// seedAccountCertificates gives each account the registration and delegation
// certificates a chain that created it would hold. Rolling an account back
// rebuilds its state from those rows, so an account without them has nothing
// to rebuild from.
func seedAccountCertificates(t *testing.T, s *govDiffScenario, raw *sql.DB) {
	t.Helper()
	for _, cred := range s.credentials {
		account, err := s.db.GetAccountByCredential(
			context.Background(), 0, cred, true, nil,
		)
		require.NoError(t, err)
		require.NotNil(t, account)
		slot := account.AddedSlot
		_, err = raw.Exec(`
INSERT INTO stake_registration (staking_key, credential_tag, added_slot, deposit_amount)
VALUES (?, 0, ?, '0')`, cred, slot)
		require.NoError(t, err)
		if len(account.Pool) > 0 {
			_, err = raw.Exec(`
INSERT INTO stake_delegation (staking_key, credential_tag, pool_key_hash, added_slot)
VALUES (?, 0, ?, ?)`, cred, account.Pool, slot)
			require.NoError(t, err)
		}
		if len(account.Drep) > 0 {
			_, err = raw.Exec(`
INSERT INTO vote_delegation (staking_key, credential_tag, drep, drep_type, added_slot)
VALUES (?, 0, ?, ?, ?)`, cred, account.Drep, account.DrepType, slot)
			require.NoError(t, err)
		}
	}
}

var (
	voteUpdatedNil = regexp.MustCompile(
		`^(.*added_slot=(\d+).*vote_updated_slot=)<nil>(.*)$`,
	)
	liveStakeUpdatedSlot = regexp.MustCompile(` updated_slot=\d+`)
)

// dumpComparableTables is dumpAllMetadataTables with the representations that
// carry no state removed. A vote never updated has a NULL vote_updated_slot,
// which readers treat as its added_slot, and reward_live_stake.updated_slot is
// the slot the aggregate was last rebuilt at.
func dumpComparableTables(t *testing.T, raw *sql.DB) map[string][]string {
	t.Helper()
	tables := dumpAllMetadataTables(t, raw)
	delete(tables, "commit_timestamp")
	for i, line := range tables["governance_vote"] {
		if m := voteUpdatedNil.FindStringSubmatch(line); m != nil {
			tables["governance_vote"][i] = m[1] + m[2] + m[3]
		}
	}
	for i, line := range tables["reward_live_stake"] {
		tables["reward_live_stake"][i] = liveStakeUpdatedSlot.ReplaceAllString(line, "")
	}
	// A rollback that restores account registrations records the credentials
	// for the next reward application to re-read. It is an invalidation hint
	// for a precomputed round, not ledger state, and nothing consumes it
	// before that application.
	tables["sync_state"] = slices.DeleteFunc(
		tables["sync_state"],
		func(line string) bool {
			return strings.Contains(line, models.RewardEligibilityRecheckKey) ||
				// A reward precompute cursor is a resumable cache of a
				// round whose inputs are already stored and fingerprinted;
				// the rollback's own restart of the worker writes it for an
				// epoch the rollback did not touch.
				strings.Contains(line, "dingo:stake-reward:precompute-cursor:") ||
				// An empty pending list is the same state as no list.
				strings.HasSuffix(line, `value={"entries":[]}`)
		},
	)
	for _, lines := range tables {
		sort.Strings(lines)
	}
	return tables
}

func changedTables(before, after map[string][]string) map[string]bool {
	out := make(map[string]bool)
	for _, d := range diffMetadataTables(before, after) {
		table, _, _ := strings.Cut(d, " ")
		out[table] = true
	}
	return out
}
