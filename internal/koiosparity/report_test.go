// Copyright 2025 Blink Labs Software
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

package koiosparity

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildStatusSummaryChecksEpochZero guards against CheckedMin using 0 as
// an "unset" sentinel: epoch 0 is a legitimate checked epoch (pre-staking
// epochs 0/1 are always in scope by default), so it must not be mistaken for
// "no minimum recorded yet" and overwritten by the next epoch checked.
func TestBuildStatusSummaryChecksEpochZero(t *testing.T) {
	statuses := []CheckEpochStatus{
		{Epoch: 0, Status: StatusPass},
		{Epoch: 1, Status: StatusPass},
		{Epoch: 2, Status: StatusPass},
	}
	summary := BuildStatusSummary("preview", nil, statuses)
	require.Equal(t, uint64(0), summary.CheckedMin,
		"epoch 0 is a real checked epoch and must remain the reported minimum")
	require.Equal(t, uint64(2), summary.CheckedMax)
}
