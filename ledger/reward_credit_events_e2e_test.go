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
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

func creditEventRows(
	t *testing.T,
	l ledgerdb.LedgerDB,
	slots ...uint64,
) []string {
	t.Helper()
	var out []string
	for _, slot := range slots {
		events, err := l.GetPostSnapshotRewardCreditEvents(slot, nil)
		require.NoError(t, err)
		for _, e := range events {
			out = append(out, fmt.Sprintf("%d %d/%x %x %d %t", e.AddedSlot,
				e.CredentialTag, e.StakingKey, e.TxHash, e.Amount, e.PostSnapshot))
		}
	}
	slices.Sort(out)
	return out
}

// With the reward-outputs family drained the credit events of a slot come
// from the set, each with the source hash of its credit, as the store lists
// them, before and after a rollback that removes the later slot's credit.
func TestPostSnapshotCreditEventsAreServedFromTheSetOnceDrained(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, "set")
	h.creditPostSnapshot(t)
	h.creditLater(t)
	slots := []uint64{h.start, h.start + 3}
	want := creditEventRows(t, h.store, slots...)
	require.Len(t, want, 4, "three credits at the start, one later")
	h.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardOutputs),
	)
	reached, _ := h.scenario.HideStoreReads("GetPostSnapshotRewardCreditEvents")
	ldb := h.scenario.LedgerDB()
	require.Equal(t, want, creditEventRows(t, ldb, slots...))

	h.scenario.RollbackBeforeBoundary()
	h.scenario.HideStoreReads()
	want = creditEventRows(t, h.store, slots...)
	require.Len(t, want, 3, "the rollback removes the later credit")
	h.scenario.HideStoreReads("GetPostSnapshotRewardCreditEvents")
	require.Equal(t, want, creditEventRows(t, ldb, slots...))
	require.Zero(t, reached(), "a read reached the metadata store")
}

// Until a family is drained the store's own rows answer, so a live-set
// ledger reads them unchanged.
func TestPostSnapshotCreditEventsStayOnTheStoreUntilDrained(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarness(t, nil)
	h.creditPostSnapshot(t)
	want := creditEventRows(t, h.store, h.start)
	require.Len(t, want, 3)
	require.Equal(t, want, creditEventRows(t, h.scenario.LedgerDB(), h.start))
}
