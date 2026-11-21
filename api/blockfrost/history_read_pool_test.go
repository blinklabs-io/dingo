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

package blockfrost

import (
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// Vote recovery loads each vote's block through its own read transaction.
// Over a metadata store whose read pool holds one connection, a votes page
// that still holds its snapshot while loading blocks waits on itself.
func TestNodeAdapterVotesReleaseSnapshotBeforeBlockReads(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapterWithIndexer(
		t, dbtest.IndexerOptions{MaxConnections: 1},
	)

	meta := db.Metadata()
	proposal := &models.GovernanceProposal{
		TxHash:        fill32(0x01),
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 1,
		ExpiresEpoch:  7,
		AnchorHash:    fill32(0x5a),
		ReturnAddress: rewardAccount(t, 0x77),
		GovActionCbor: []byte{0x81, 0x06},
		AddedSlot:     10,
	}
	require.NoError(t, meta.SetGovernanceProposal(proposal, nil))
	slot := uint64(100)
	require.NoError(t, meta.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: bytes32of(28, 0xd1),
		Vote:            models.VoteYes,
		AddedSlot:       slot,
		VoteUpdatedSlot: &slot,
	}, nil))

	done := make(chan error, 1)
	go func() {
		_, _, err := adapter.GovernanceProposalVotes(
			context.Background(),
			GovActionRef{TxHash: fill32(0x01), Index: 0},
			asc,
		)
		done <- err
	}()
	err := testutil.RequireReceive(
		t, done, 10*time.Second,
		"votes page blocked on its own pooled read connection",
	)
	require.ErrorIs(t, err, ErrHistoryNotRetained)
}
