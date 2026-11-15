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

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// With the family drained and no set serving its rows, the reward round's
// counts and fees and the capture's counts fail rather than read the emptied
// imported-count tables and under-count.
func TestRewardBlockReadsFailWhenTheDrainedFamilyHasNoServingSet(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, ledger.UtxoSetReadsOff, nil, true)
	r.write()
	require.NoError(t, r.scenario.DB.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyRewardBlocksFees,
	))
	r.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardBlocksFees),
	)
	_, _, _, err := r.scenario.Counts(4)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, err = r.scenario.Fees(4)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
	_, _, _, err = r.scenario.EpochCounts(4)
	require.ErrorIs(t, err, ledger.ErrDrainedFamilyUnserved)
}
