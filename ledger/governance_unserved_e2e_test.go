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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// With the set failed, the LedgerDB fails every read and write rather than
// answer nothing.
func TestGovernanceReadsFailWhenTheSetCannotServe(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	// A failed set serves no read.
	scenario.FailSet()
	ctx := t.Context()
	ldb := scenario.LedgerDB()
	hash := bytes.Repeat([]byte{0x77}, 32)
	_, err = ldb.GetGovernanceProposal(ctx, hash, 0, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetGovernanceProposalSet(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetActiveGovernanceProposals(ctx, 1, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetRatifiedGovernanceProposals(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetLastEnactedGovernanceProposal(ctx, []uint8{1}, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetGovernanceVotes(ctx, 1, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetAllGovernanceProposals(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetAllGovernanceVotes(ctx, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetGovernanceProposalsRefundedAt(1, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
	_, err = ldb.GetGovernanceVotesAtSlot(ctx, 1, 10, nil)
	require.ErrorIs(t, err, ledger.ErrLedgerStateAsOfUnavailable)
	txn := ldb.Begin(ctx, true)
	defer txn.Release()
	require.ErrorIs(
		t,
		ldb.SetGovernanceProposal(ctx, &models.GovernanceProposal{
			TxHash: hash, AnchorURL: "u",
		}, txn),
		ledger.ErrLedgerStateUnserved,
	)
	require.ErrorIs(t, ldb.SetGovernanceVote(ctx, &models.GovernanceVote{
		ProposalID: 1, VoterCredential: hash[:28],
	}, txn), ledger.ErrLedgerStateUnserved)
	require.ErrorIs(t, ldb.ClearGovernanceProposalRatification(
		ctx, hash, 0, 5, txn,
	), ledger.ErrLedgerStateUnserved)
	_, err = ldb.DeleteGovernanceVotesForDrep(ctx, 0, hash[:28], 1, 5, txn)
	require.ErrorIs(t, err, ledger.ErrLedgerStateUnserved)
}
