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
	"github.com/blinklabs-io/dingo/database/models"
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

// The credit events of a slot come from the set, each with the source hash
// of its credit, before and after a rollback that removes the later slot's
// credit.
func TestPostSnapshotCreditEventsAreServedFromTheSet(t *testing.T) {
	t.Parallel()
	h := newRewardOutputHarnessReads(t, nil, "set")
	h.creditPostSnapshot(t)
	h.creditLater(t)
	slots := []uint64{h.start, h.start + 3}
	reached, _ := h.scenario.HideStoreReads("GetPostSnapshotRewardCreditEvents")
	ldb := h.scenario.LedgerDB()
	event := func(
		slot uint64, ref models.StakeCredentialRef, source byte, amount uint64,
	) string {
		return fmt.Sprintf("%d %d/%x %x %d %t",
			slot, ref.Tag, ref.Key, []byte{source}, amount, true)
	}
	atStart := []string{
		event(h.start, h.owner, 0xa1, 11),
		event(h.start, h.owner, 0xa2, 12),
		event(h.start, h.deleg, 0xa3, 5),
	}
	slices.Sort(atStart)
	all := append(slices.Clone(atStart), event(h.start+3, h.deleg, 0xa4, 4))
	slices.Sort(all)
	require.Equal(t, all, creditEventRows(t, ldb, slots...))

	h.scenario.RollbackBeforeBoundary()
	require.Equal(t, atStart, creditEventRows(t, ldb, slots...),
		"the rollback removes the later credit")
	require.Zero(t, reached(), "a read reached the metadata store")
}
