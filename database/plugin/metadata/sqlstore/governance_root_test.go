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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestLastEnactedGovernanceRootSQLite(t *testing.T) {
	t.Parallel()
	exerciseLastEnactedGovernanceRoot(t, newManagementTestStore(t))
}

// exerciseLastEnactedGovernanceRoot pins GetLastEnactedGovernanceProposal's
// same-boundary contract on each backend: a purpose chain enacted at one
// boundary resolves to its terminal action whatever the row-insertion order,
// only an enacted child of the same purpose supersedes its parent, a
// soft-deleted child does not, and a later boundary still wins.
func exerciseLastEnactedGovernanceRoot(t *testing.T, store *Store) {
	t.Helper()
	parameterChange := []uint8{uint8(lcommon.GovActionTypeParameterChange)}
	hardFork := []uint8{uint8(lcommon.GovActionTypeHardForkInitiation)}
	enacted := func(
		seed byte,
		actionType lcommon.GovActionType,
		parent []byte,
		epoch, slot uint64,
	) *models.GovernanceProposal {
		proposal := &models.GovernanceProposal{
			TxHash:        bytes.Repeat([]byte{seed}, 32),
			ActionType:    uint8(actionType),
			ProposedEpoch: epoch - 1,
			ExpiresEpoch:  epoch + 10,
			EnactedEpoch:  &epoch,
			EnactedSlot:   &slot,
			ReturnAddress: make([]byte, 29),
			AnchorHash:    make([]byte, 32),
			AddedSlot:     slot - 1,
		}
		if parent != nil {
			parentIndex := uint32(0)
			proposal.ParentTxHash = parent
			proposal.ParentActionIdx = &parentIndex
		}
		return proposal
	}
	requireRoot := func(actionTypes []uint8, want []byte) {
		t.Helper()
		root, err := store.GetLastEnactedGovernanceProposal(actionTypes, nil)
		require.NoError(t, err)
		if want == nil {
			require.Nil(t, root)
			return
		}
		require.NotNil(t, root)
		require.Equal(t, want, root.TxHash)
	}

	parent := enacted(0x01, lcommon.GovActionTypeParameterChange, nil, 10, 1000)
	child := enacted(
		0x02, lcommon.GovActionTypeParameterChange, parent.TxHash, 10, 1000,
	)
	grandchild := enacted(
		0x03, lcommon.GovActionTypeParameterChange, child.TxHash, 10, 1000,
	)
	// Insert the chain tip first so the tip has the lowest row ID.
	for _, proposal := range []*models.GovernanceProposal{
		grandchild, child, parent,
	} {
		require.NoError(t, store.SetGovernanceProposal(proposal, nil))
	}
	requireRoot(parameterChange, grandchild.TxHash)

	// A same-boundary row of another purpose naming the tip as its parent
	// does not supersede it.
	crossPurpose := enacted(
		0x04, lcommon.GovActionTypeHardForkInitiation, grandchild.TxHash,
		10, 1000,
	)
	require.NoError(t, store.SetGovernanceProposal(crossPurpose, nil))
	requireRoot(parameterChange, grandchild.TxHash)
	requireRoot(hardFork, crossPurpose.TxHash)

	// A soft-deleted child no longer supersedes its parent.
	deletedSlot := uint64(1000)
	grandchild.DeletedSlot = &deletedSlot
	require.NoError(t, store.SetGovernanceProposal(grandchild, nil))
	requireRoot(parameterChange, child.TxHash)

	// A proposal enacted at a later boundary is the root.
	later := enacted(
		0x05, lcommon.GovActionTypeParameterChange, child.TxHash, 11, 1100,
	)
	require.NoError(t, store.SetGovernanceProposal(later, nil))
	requireRoot(parameterChange, later.TxHash)
}
