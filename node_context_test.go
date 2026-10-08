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

package dingo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The caller's context reaches the metadata transaction, so a cancelled
// startup aborts the probe instead of running it to completion, and the
// failure names the step that was interrupted.
func TestBackfillRewardLiveStakeHonorsCancelledContext(t *testing.T) {
	t.Parallel()

	n, _ := newLiveLifecycleTestNode(t, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := n.backfillRewardLiveStake(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "failed to probe reward live stake state")
}
