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
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/stretchr/testify/require"
)

// latestCommitteeRows renders the latest quorum row, clear marker included,
// and each cold credential's latest resignation, in a fixed order.
func latestCommitteeRows(
	t *testing.T,
	l ledgerdb.LedgerDB,
	stage string,
) []string {
	t.Helper()
	q, err := l.LatestCommitteeQuorum(t.Context(), nil)
	require.NoError(t, err, stage)
	out := []string{"quorum nil"}
	if q != nil {
		quorum := "clear"
		if q.Quorum != nil {
			quorum = q.Quorum.String()
		}
		out = []string{fmt.Sprintf("quorum %d %s", q.AddedSlot, quorum)}
	}
	rs, err := l.LatestCommitteeResignations(t.Context(), nil)
	require.NoError(t, err, stage)
	var resigns []string
	for _, r := range rs {
		resigns = append(resigns, fmt.Sprintf("resign %d/%x@%d",
			r.ColdCredentialTag, r.ColdCredential, r.AddedSlot))
	}
	slices.Sort(resigns)
	return append(out, resigns...)
}

// The set answers the latest committee quorum and each cold credential's
// latest resignation with the store's reads failing, after the committee
// writes, after a quorum is cleared and after the rollback.
func TestLatestCommitteeReadsAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := setModeScenario(t)
	ldb := scenario.LedgerDB()
	slots := scenario.Committee()
	reached, _ := scenario.HideStoreReads(
		"LatestCommitteeQuorum", "LatestCommitteeResignations",
	)
	got := latestCommitteeRows(t, ldb, "after the writes")
	require.Equal(t, fmt.Sprintf("quorum %d clear", slots.NoConfidence), got[0])
	require.Len(t, got, 2, "B's resignation is the only one")
	require.Contains(t, got[1], "resign ")
	scenario.RollbackBeforeBoundary()
	require.Equal(t,
		[]string{fmt.Sprintf("quorum %d 1/2", slots.Base)},
		latestCommitteeRows(t, ldb, "after the rollback"),
		"the rollback leaves the quorum seated at the base slot")
	require.Zero(t, reached(), "a committee read reached the metadata store")
}

// Verify mode answers from the store and counts a difference from the set.
