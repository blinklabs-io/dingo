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

	"github.com/stretchr/testify/require"
)

func TestStakeRewardEpochsForInitialApplication(t *testing.T) {
	epochs, ok := stakeRewardEpochsForApplication(2)
	require.True(t, ok)
	require.Equal(t, uint64(0), epochs.snapshot)
	require.Equal(t, uint64(0), epochs.performance)
	require.Equal(t, uint64(1), epochs.pots)
}
