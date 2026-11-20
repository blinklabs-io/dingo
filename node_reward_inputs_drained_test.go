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

package dingo

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// With the reward-inputs family drained the startup provenance gate reads the
// mark snapshots' calculation versions from the live UTxO set, not from the
// emptied reward_snapshot table: a stale snapshot still stops the node and a
// current one does not.
func TestBackfillRewardLiveStakeChecksDrainedSnapshotProvenanceInTheSet(
	t *testing.T,
) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		version uint
		stale   bool
	}{
		{"stale mark snapshot", models.RewardStakeCalculationVersion - 1, true},
		{"current mark snapshot", models.RewardStakeCalculationVersion, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			guard := statedrain.NewTripGuard()
			db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
				Config:         &database.Config{DataDir: t.TempDir()},
				StatementGuard: guard,
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			tip := ocommon.NewPoint(10, bytes.Repeat([]byte{10}, 32))
			require.NoError(t, set.ApplyLedgerChanges(
				&tip, ledger.LedgerChanges{
					RewardInputs: []ledger.RewardInputChange{{
						Kind: ledger.RewardInputSnapshot,
						Snapshot: &models.RewardSnapshot{
							Epoch: 4, SnapshotType: "mark",
							CalculationVersion: tc.version,
							CapturedSlot:       10, BoundarySlot: 10,
						},
					}},
				},
			))
			require.NoError(t, db.RecordDrainedFamily(
				t.Context(), nodesettings.FamilyRewardInputs,
			))
			guard.Arm(
				nodesettings.FamilyRewardInputs,
				statedrain.TableNames(nodesettings.FamilyRewardInputs),
			)
			n := &Node{
				db:      db,
				utxoSet: set,
				config: Config{
					logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
				},
			}

			n.utxoSetReconciled = true
			err = n.backfillRewardLiveStake(t.Context())

			require.Empty(t, guard.Trips())
			if tc.stale {
				require.ErrorContains(t, err, "older accounting")
				require.ErrorContains(t, err, "[4]")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
