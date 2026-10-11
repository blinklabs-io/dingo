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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The selector's stale-tip rule is inactive without a wall-clock source, so
// the composition site must install the hook, and it must report an unknown
// clock rather than slot 0 while no ledger is available.
func TestBuildChainSelectorConfigWallClockSlot(t *testing.T) {
	t.Parallel()
	n := &Node{}
	cfg := n.buildChainSelectorConfig(2160, true, 0)
	require.NotNil(t, cfg.WallClockSlot)
	slot, ok := cfg.WallClockSlot()
	assert.False(t, ok)
	assert.Zero(t, slot)
}
