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

package sqlstore

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestGovernanceProposalSubmissionOrderSQLite(t *testing.T) {
	t.Parallel()
	exerciseGovernanceProposalSubmissionOrder(t, newManagementTestStore(t))
}

// exerciseGovernanceProposalSubmissionOrder pins the proposal order the
// RATIFY and ENACT reads return on each backend: within a slot, block
// transaction position then action position, independent of hash and row ID;
// rows without a position keep hash order; and a rollback removes the
// positions of the proposals it deletes, so re-applying them restores the
// same order.
func exerciseGovernanceProposalSubmissionOrder(t *testing.T, store *Store) {
	t.Helper()
	type key struct {
		seed        byte
		actionIndex uint32
	}
	proposal := func(
		seed byte,
		actionIndex uint32,
		slot uint64,
		txIndex *uint32,
	) *models.GovernanceProposal {
		return &models.GovernanceProposal{
			TxHash:        bytes.Repeat([]byte{seed}, 32),
			ActionIndex:   actionIndex,
			ActionType:    2,
			ProposedEpoch: 5,
			ExpiresEpoch:  15,
			ReturnAddress: make([]byte, 29),
			AnchorHash:    make([]byte, 32),
			TxIndex:       txIndex,
			AddedSlot:     slot,
		}
	}
	index := func(i uint32) *uint32 { return &i }
	requireOrder := func(got []*models.GovernanceProposal, want ...key) {
		t.Helper()
		require.Len(t, got, len(want))
		for i, w := range want {
			require.Equal(t, bytes.Repeat([]byte{w.seed}, 32), got[i].TxHash,
				"position %d", i)
			require.Equal(t, w.actionIndex, got[i].ActionIndex,
				"position %d", i)
		}
	}
	orderRows := func() int {
		t.Helper()
		var count int
		require.NoError(t, store.writeDB.QueryRow(
			"SELECT COUNT(*) FROM governance_proposal_order",
		).Scan(&count))
		return count
	}

	// Slot 500: transaction 0x02 precedes 0x01 in the block, and 0x01 is
	// stored first.
	slot500 := []*models.GovernanceProposal{
		proposal(0x01, 0, 500, index(1)),
		proposal(0x02, 1, 500, index(0)),
		proposal(0x02, 0, 500, index(0)),
	}
	// Slot 600 rows predate recorded positions.
	slot600 := []*models.GovernanceProposal{
		proposal(0x04, 0, 600, nil),
		proposal(0x03, 0, 600, nil),
	}
	for _, p := range append(append([]*models.GovernanceProposal{}, slot500...),
		slot600...) {
		require.NoError(t, store.SetGovernanceProposal(p, nil))
	}
	active, err := store.GetActiveGovernanceProposals(5, nil)
	require.NoError(t, err)
	requireOrder(active,
		key{0x02, 0}, key{0x02, 1}, key{0x01, 0},
		key{0x03, 0}, key{0x04, 0},
	)
	require.NotNil(t, active[0].TxIndex)
	require.Equal(t, uint32(0), *active[0].TxIndex)
	require.Nil(t, active[3].TxIndex)

	// A later write without a position keeps the recorded one.
	ratifiedEpoch, ratifiedSlot := uint64(6), uint64(650)
	for _, p := range []*models.GovernanceProposal{
		proposal(0x01, 0, 500, nil), proposal(0x02, 1, 500, nil),
	} {
		p.RatifiedEpoch = &ratifiedEpoch
		p.RatifiedSlot = &ratifiedSlot
		require.NoError(t, store.SetGovernanceProposal(p, nil))
	}
	ratified, err := store.GetRatifiedGovernanceProposals(nil)
	require.NoError(t, err)
	requireOrder(ratified, key{0x02, 1}, key{0x01, 0})
	require.Equal(t, 3, orderRows())

	// Rolling back past slot 500 removes those proposals and their positions;
	// re-applying the block restores the same order.
	require.NoError(t, store.DeleteGovernanceProposalsAfterSlot(450, nil))
	require.Zero(t, orderRows())
	active, err = store.GetActiveGovernanceProposals(5, nil)
	require.NoError(t, err)
	require.Empty(t, active)
	for _, p := range slot500 {
		p.ID = 0
		require.NoError(t, store.SetGovernanceProposal(p, nil))
	}
	active, err = store.GetActiveGovernanceProposals(5, nil)
	require.NoError(t, err)
	requireOrder(active, key{0x02, 0}, key{0x02, 1}, key{0x01, 0})
	require.Equal(t, 3, orderRows())
}
