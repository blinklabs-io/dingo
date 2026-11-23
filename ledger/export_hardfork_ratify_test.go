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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// HardForkRatifyOverSetForTest runs the hard fork fixture's ratifying
// boundary and returns the fixture's proposal as the ledger's LedgerDB reads
// it afterwards. The fixture's ledger state is in its LedgerDB set, so the
// proposal the boundary tallies is one the set encoded and decoded. The
// proposal has no parent and no hard fork has been enacted.
func HardForkRatifyOverSetForTest(t *testing.T) *models.GovernanceProposal {
	t.Helper()
	f := newHardForkRatifyFixture(t)
	require.Nil(t, f.proposal.ParentTxHash)
	require.Nil(t, f.proposal.ParentActionIdx)
	_, err := f.ls.governanceServing(nil)
	require.NoError(t, err, "the set does not serve governance reads")

	f.rollover(t, f.ls.currentEpoch, f.pparams)
	got, err := f.ls.LedgerDB().GetGovernanceProposal(
		t.Context(), f.proposal.TxHash, f.proposal.ActionIndex, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, got)
	return got
}
