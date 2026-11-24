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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// The set holds the live stake through blocks, a rollback and a resume from
// a set behind the ledger. A resume rolls the ledger back to the set's tip,
// so the stake it leaves is the stake of a run the ledger rolled back to that
// point itself.
func TestLiveStakeFollowsBlocksARollbackAndAResume(t *testing.T) {
	t.Parallel()
	run := func() (ledger.ShadowScenario, *utxoset.Set, func() map[string]string) {
		t.Helper()
		set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir(), Only: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = set.Close() })
		scenario := ledger.RunUtxoSetOnlyScenarioForTest(
			t, set, func(*database.Database) {},
		)
		return scenario, set, func() map[string]string {
			t.Helper()
			got := map[string]string{}
			require.NoError(t, set.EachCredentialStake(
				func(tag uint8, hash []byte, n uint64) error {
					got[fmt.Sprintf("%d/%X", tag, hash)] = fmt.Sprint(n)
					return nil
				},
			))
			return got
		}
	}
	scenario, set, setStake := run()
	atTip := setStake()
	require.NotEmpty(t, atTip)

	behindAt := scenario.Points[len(scenario.Points)-2]
	require.NoError(t, scenario.Rollback(behindAt))
	rolledBack := setStake()
	require.NotEmpty(t, rolledBack)
	require.NotEqual(t, atTip, rolledBack, "the rollback undoes the tip's stake")

	// The set falls behind the ledger and the ledger resumes from its tip.
	resumeAt := scenario.Points[1]
	_, err := set.Rollback(resumeAt)
	require.NoError(t, err)
	require.NotEqual(t, rolledBack, setStake(), "the set is behind the ledger")
	resumed, err := scenario.Resume(t.Context(), resumeAt)
	require.NoError(t, err)
	require.True(t, resumed)
	require.False(t, scenario.UtxoSetFailed())

	synced, _, syncedStake := run()
	require.Equal(t, resumeAt, synced.Points[1], "the runs apply the same chain")
	require.NoError(t, synced.Rollback(resumeAt))
	require.Equal(t, syncedStake(), setStake(),
		"the resumed set holds the stake of the resume point")
}
