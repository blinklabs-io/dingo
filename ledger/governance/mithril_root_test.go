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

package governance

import (
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"testing"
)

// TestMithrilSeededRootUnblocksChainedProposal mirrors the failure
// shape of issue #2195: a chained HardForkInitiation arriving on a
// node whose only enacted-root visibility comes from a Mithril
// snapshot must be accepted by validateParentChain when the per-
// purpose root has been seeded as a synthetic enacted row.
func TestMithrilSeededRootUnblocksChainedProposal(t *testing.T) {
	db, store := newTallyTestDB(t)

	rootHash := testBytes(32, 0xA1)
	parentIdx := uint32(0)

	// Synthetic seeded root, equivalent to what
	// ledgerstate.seedPrevGovActionIds writes.
	enactedEpoch := uint64(500)
	enactedSlot := uint64(123_456)
	require.NoError(t, store.DB().Create(&models.GovernanceProposal{
		TxHash:        rootHash,
		ActionIndex:   parentIdx,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 0,
		ExpiresEpoch:  0,
		EnactedEpoch:  &enactedEpoch,
		EnactedSlot:   &enactedSlot,
		Deposit:       0,
		ReturnAddress: make([]byte, 29),
		AnchorURL:     "",
		AnchorHash:    make([]byte, 32),
		AddedSlot:     0,
	}).Error)

	root, err := db.GetLastEnactedGovernanceProposal(
		[]uint8{uint8(lcommon.GovActionTypeHardForkInitiation)},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, root, "synthetic root should be visible to ratification")

	// Chained child whose parent points at the seeded root. Without
	// the synthetic row this would be rejected by validateParentChain
	// (currentRoot=nil, parent set) and silently expire — the bug.
	child := &models.GovernanceProposal{
		ActionType:      uint8(lcommon.GovActionTypeHardForkInitiation),
		ParentTxHash:    rootHash,
		ParentActionIdx: &parentIdx,
	}
	assert.True(
		t, validateParentChain(child, root),
		"chained child must validate against the seeded root",
	)

	// And without the root, the child still fails — we want the
	// regression to come back if seeding ever silently no-ops.
	assert.False(
		t, validateParentChain(child, nil),
		"sanity: chained child still fails without a root",
	)
}

// TestValidateParentChain_NoConfidenceRootAllowsCommitteeUpdate
// proves that when committee NoConfidence is the seeded root,
// a subsequent UpdateCommittee whose parent matches the root is
// accepted (purposeCommittee groups both action types).
func TestValidateParentChain_NoConfidenceRootAllowsCommitteeUpdate(t *testing.T) {
	rootHash := testBytes(32, 0xC1)
	parentIdx := uint32(2)

	root := &models.GovernanceProposal{
		TxHash:      rootHash,
		ActionIndex: parentIdx,
		ActionType:  uint8(lcommon.GovActionTypeNoConfidence),
	}
	child := &models.GovernanceProposal{
		ActionType:      uint8(lcommon.GovActionTypeUpdateCommittee),
		ParentTxHash:    rootHash,
		ParentActionIdx: &parentIdx,
	}
	assert.True(
		t, validateParentChain(child, root),
		"UpdateCommittee chained off NoConfidence root must validate",
	)
}
