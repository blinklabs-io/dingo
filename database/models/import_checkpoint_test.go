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

package models

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewardLiveStakeImportCheckpointRoundTrip(t *testing.T) {
	t.Parallel()
	cursor := []byte{1, 0, 0xff, 0x42}
	phase := RewardLiveStakeImportCheckpoint(cursor)

	got, inProgress, err := RewardLiveStakeImportCursor(phase)
	require.NoError(t, err)
	require.True(t, inProgress)
	require.Equal(t, cursor, got)
	require.True(t, IsPhaseCompleted(phase, ImportPhaseGovState))
	require.False(t, IsPhaseCompleted(phase, ImportPhaseRewardLiveStake))
	require.False(t, IsPhaseCompleted(phase, ImportPhaseTip))
}

func TestRewardLiveStakeImportCheckpointRejectsMalformedCursor(t *testing.T) {
	t.Parallel()
	_, inProgress, err := RewardLiveStakeImportCursor(
		ImportPhaseRewardLiveStake + ":not-hex",
	)
	require.True(t, inProgress)
	require.ErrorContains(t, err, "decode reward live stake import cursor")
}

func TestCompletedRewardLiveStakeImportPhaseAdvancesOrdering(t *testing.T) {
	t.Parallel()
	_, inProgress, err := RewardLiveStakeImportCursor(ImportPhaseRewardLiveStake)
	require.NoError(t, err)
	require.False(t, inProgress)
	require.True(t, IsPhaseCompleted(
		ImportPhaseRewardLiveStake,
		ImportPhaseRewardLiveStake,
	))
	require.False(t, IsPhaseCompleted(ImportPhaseRewardLiveStake, ImportPhaseTip))
}
