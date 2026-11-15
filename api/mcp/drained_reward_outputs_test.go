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

package mcp

import (
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// get_pool_performance reads reward_pool_output, which the set serves only
// through the ledger, so a drained reward-outputs family is refused.
func TestPoolPerformanceRefusesDrainedRewardOutputs(t *testing.T) {
	t.Parallel()
	args := map[string]any{"pool_id": strings.Repeat("ab", 28)}
	cs := drainedToolSession(
		t, newSchemaDB(t), nodesettings.FamilyRewardOutputs,
	)

	text := callTool(t, cs, "get_pool_performance", args, true)

	require.Contains(t, text, "no longer holds")
	require.Contains(t, text, nodesettings.FamilyRewardOutputs.String())
}
