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
	"github.com/stretchr/testify/require"
)

// A loaded set keeps reward block rows available after their metadata family drains.
func TestRewardBlockReadsStayOnTheSetAfterDrain(t *testing.T) {
	t.Parallel()
	r := newRewardBlocksRun(t, "set", nil, true)
	r.write()
	wantCounts, wantTotal, wantKnown, err := r.scenario.Counts(4)
	require.NoError(t, err)
	wantFees, err := r.scenario.Fees(4)
	require.NoError(t, err)
	wantEpochCounts, wantEpochTotal, wantEpochKnown, err := r.scenario.EpochCounts(4)
	require.NoError(t, err)
	r.scenario.DrainFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardBlocksFees),
	)
	reached, hide := r.scenario.HideStoreReads(rewardBlockReadNames...)
	counts, total, known, err := r.scenario.Counts(4)
	require.NoError(t, err)
	fees, err := r.scenario.Fees(4)
	require.NoError(t, err)
	epochCounts, epochTotal, epochKnown, err := r.scenario.EpochCounts(4)
	require.NoError(t, err)
	hide(false)
	require.Equal(t, wantCounts, counts)
	require.Equal(t, wantTotal, total)
	require.Equal(t, wantKnown, known)
	require.Equal(t, wantFees, fees)
	require.Equal(t, wantEpochCounts, epochCounts)
	require.Equal(t, wantEpochTotal, epochTotal)
	require.Equal(t, wantEpochKnown, epochKnown)
	require.Zero(t, reached(), "loaded reward block rows reached metadata")
}
