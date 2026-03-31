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

package analysis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChainTipRange_Empty(t *testing.T) {
	min, max, ok := chainTipRange(map[string]uint64{})
	require.False(t, ok)
	require.Zero(t, min)
	require.Zero(t, max)
}

func TestChainTipRange_NonEmpty(t *testing.T) {
	min, max, ok := chainTipRange(map[string]uint64{
		"p1": 42,
		"p2": 7,
		"p3": 99,
	})
	require.True(t, ok)
	require.Equal(t, uint64(7), min)
	require.Equal(t, uint64(99), max)
}
