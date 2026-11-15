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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// Once the governance family is drained the listing and the vote history,
// which need the SQL rows, refuse with ErrHistoryNotRetained instead of
// answering empty, and a single proposal is read through the ledger.
func TestNodeAdapterGovernanceReadsOfADrainedFamily(t *testing.T) {
	t.Parallel()
	fx := newEpochFixture(t)
	ctx := context.Background()
	fx.addProposal(
		t, 0x01, 0, lcommon.GovActionTypeInfo, []byte{0x81, 0x06}, 100,
	)
	ref := GovActionRef{TxHash: fill32(0x01), Index: 0}
	rows, total, err := fx.adapter.GovernanceProposals(ctx, asc)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, rows, 1)

	fx.adapter.ledgerState.SetDrainedFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyGovernance),
	)

	_, _, err = fx.adapter.GovernanceProposals(ctx, asc)
	require.ErrorIs(t, err, ErrHistoryNotRetained)
	_, _, err = fx.adapter.GovernanceProposalVotes(ctx, ref, asc)
	require.ErrorIs(t, err, ErrHistoryNotRetained)
	_, _, err = fx.adapter.CommitteeVotes(
		ctx, nil, false, asc,
	)
	require.ErrorIs(t, err, ErrHistoryNotRetained)
	detail, err := fx.adapter.GovernanceProposal(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, hexOf(fill32(0x01)), detail.TxHash)
	// With the database's own latch set too, the ledger's read fails closed
	// where a direct SQL read would still find the row.
	require.NoError(t, fx.adapter.ledgerState.Database().RecordDrainedFamily(
		ctx, nodesettings.FamilyGovernance,
	))
	_, err = fx.adapter.GovernanceProposal(ctx, ref)
	require.ErrorIs(t, err, database.ErrFamilyDrained)
}
