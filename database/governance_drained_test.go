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

package database_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/stretchr/testify/require"
)

// A drained governance family is never read or written through the database:
// every read and write fails with ErrFamilyDrained before it reaches a
// statement, because the set is the only copy.
func TestDrainedGovernanceFailsEveryDatabaseAccess(t *testing.T) {
	t.Parallel()
	db, guard, _ := newDrainedTruncateDB(t)
	tables := statedrain.TableNames(nodesettings.FamilyGovernance)
	require.Len(t, tables, 6)
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyGovernance,
	))
	guard.Arm(nodesettings.FamilyGovernance, tables)
	ctx := t.Context()
	hash := bytes.Repeat([]byte{0x77}, 32)
	check := func(name string, err error) {
		t.Helper()
		require.ErrorIs(t, err, database.ErrFamilyDrained, name)
	}
	_, err := db.GetGovernanceProposal(ctx, hash, 0, nil)
	check("GetGovernanceProposal", err)
	_, err = db.GetActiveGovernanceProposals(ctx, 1, nil)
	check("GetActiveGovernanceProposals", err)
	_, err = db.GetGovernanceProposalSet(ctx, nil)
	check("GetGovernanceProposalSet", err)
	_, err = db.GetGovernanceProposalSetAtSlot(ctx, 1, nil)
	check("GetGovernanceProposalSetAtSlot", err)
	_, err = db.GetExpiringGovernanceProposals(ctx, 1, nil)
	check("GetExpiringGovernanceProposals", err)
	_, err = db.GetExpiredGovernanceProposalsAt(ctx, 1, 1, nil)
	check("GetExpiredGovernanceProposalsAt", err)
	_, err = db.GetExpiredAwaitingDropGovernanceProposals(ctx, 1, nil)
	check("GetExpiredAwaitingDropGovernanceProposals", err)
	_, err = db.GetDroppedGovernanceProposalsAt(ctx, 1, 1, nil)
	check("GetDroppedGovernanceProposalsAt", err)
	_, err = db.GetRatifiedGovernanceProposals(ctx, nil)
	check("GetRatifiedGovernanceProposals", err)
	_, err = db.GetEnactedGovernanceProposalsAt(ctx, 1, 1, nil)
	check("GetEnactedGovernanceProposalsAt", err)
	_, err = db.GetLastEnactedGovernanceProposal(ctx, []uint8{1}, nil)
	check("GetLastEnactedGovernanceProposal", err)
	_, err = db.GetChildGovernanceProposals(ctx, hash, 0, nil)
	check("GetChildGovernanceProposals", err)
	_, err = db.GetGovernanceVotes(ctx, 1, nil)
	check("GetGovernanceVotes", err)
	_, err = db.GetGovernanceVotesAtSlot(ctx, 1, 1, nil)
	check("GetGovernanceVotesAtSlot", err)
	_, err = db.GetAllGovernanceProposals(ctx, nil)
	check("GetAllGovernanceProposals", err)
	_, err = db.GetAllGovernanceVotes(ctx, nil)
	check("GetAllGovernanceVotes", err)
	check("SetGovernanceProposal", db.SetGovernanceProposal(
		ctx, &models.GovernanceProposal{TxHash: hash}, nil,
	))
	check("SetGovernanceVote", db.SetGovernanceVote(
		ctx, &models.GovernanceVote{ProposalID: 1}, nil,
	))
	check("ClearGovernanceProposalRatification",
		db.ClearGovernanceProposalRatification(ctx, hash, 0, 1, nil))
	_, err = db.DeleteGovernanceVotesForDrep(ctx, 0, hash[:28], 1, 1, nil)
	check("DeleteGovernanceVotesForDrep", err)
	check("DeleteGovernanceProposalsAfterSlot",
		db.DeleteGovernanceProposalsAfterSlot(ctx, 1, nil))
	check("DeleteGovernanceVotesAfterSlot",
		db.DeleteGovernanceVotesAfterSlot(ctx, 1, nil))
	_, err = dbadapter.New(db).GetGovernanceProposalsRefundedAt(1, nil)
	check("GetGovernanceProposalsRefundedAt", err)
	require.Empty(t, guard.Trips(), "a statement reached a drained table")
}

// The rollback of a database that drained the governance family issues no
// statement against its tables, neither the proposal and vote deletes nor
// their history deletes.
func TestTruncateAfterSlotTouchesNoDrainedGovernanceTable(t *testing.T) {
	t.Parallel()
	db, guard, point := newDrainedTruncateDB(t)
	require.NoError(t, db.SetBlockNonce(
		point.Hash, point.Slot, bytes.Repeat([]byte{0xc2}, 32), true, nil,
	))
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyGovernance,
	))
	guard.Arm(
		nodesettings.FamilyGovernance,
		statedrain.TableNames(nodesettings.FamilyGovernance),
	)

	_, _, err := db.TruncateAfterSlot(t.Context(), point, 0, nil)

	require.NoError(t, err)
	require.Empty(t, guard.Trips(), "a drained table was touched")
}
