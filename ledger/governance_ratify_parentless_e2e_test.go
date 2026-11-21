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
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// A proposal with no parent, read from the live UTxO set, ratifies at the
// boundary where its votes clear the thresholds. The set's row encoding does
// not tell an absent parent hash from an empty one, and ratification takes a
// proposal with a non-nil parent hash for the child of a root that was never
// enacted.
func TestGovernanceRatifiesParentlessProposalFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	proposal := ledger.HardForkRatifyOverSetForTest(
		t, set,
		func(store ledgerdb.LedgerDB) {
			require.NoError(t, set.LoadLedgerState(t.Context(), store))
		},
	)
	require.NotNil(
		t,
		proposal.RatifiedEpoch,
		"the parentless hard fork proposal did not ratify",
	)
	require.Equal(t, uint64(742), *proposal.RatifiedEpoch)
	require.Nil(t, proposal.ParentTxHash)
}
