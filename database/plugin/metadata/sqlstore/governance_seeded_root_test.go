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

// A ledger-state import seeds a per-purpose root at added_slot 0, enacted at
// the snapshot anchor. Indexing the root's own historical proposal later
// upserts the same row with real proposal data. The row must keep added_slot
// 0, so it still reports no enactment epoch while governance resolves it as
// the current root.
func TestSeededRootKeepsUnknownEnactmentAfterHistoricalUpsert(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	txHash := bytes.Repeat([]byte{0x06}, 32)
	anchorEpoch := uint64(1458)
	anchorSlot := uint64(125971200)
	require.NoError(t, store.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		EnactedEpoch:  &anchorEpoch,
		EnactedSlot:   &anchorSlot,
		ReturnAddress: make([]byte, 29),
		AnchorHash:    make([]byte, 32),
	}, nil))

	returnAddress := bytes.Repeat([]byte{0xe0}, 29)
	require.NoError(t, store.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		ProposedEpoch: 1368,
		ExpiresEpoch:  1398,
		Deposit:       1_000_000_000,
		ReturnAddress: returnAddress,
		AnchorHash:    bytes.Repeat([]byte{0x01}, 32),
		AddedSlot:     118195200,
	}, nil))

	got, err := store.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(1_000_000_000), got.Deposit)
	require.Equal(t, returnAddress, got.ReturnAddress)
	require.Zero(t, got.AddedSlot)
	require.NotNil(t, got.EnactedEpoch)
	require.Nil(t, got.RecordedEnactedEpoch())

	root, err := store.GetLastEnactedGovernanceProposal(
		[]uint8{uint8(lcommon.GovActionTypeUpdateCommittee)}, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, root)
	require.Equal(t, txHash, root.TxHash)
}
