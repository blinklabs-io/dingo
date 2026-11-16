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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiveStakeDiffNamesTheDifferingKeys(t *testing.T) {
	t.Parallel()
	want := map[string]uint64{"a": 5, "b": 7, "c": 9}
	got := map[string]uint64{"a": 5, "b": 8, "d": 1}
	d := liveStakeDiff(want, got)
	require.Contains(t, d, "differing=3 of store=3 set=3 keys")
	require.Contains(t, d, "62 store=7 set=8")
	require.Contains(t, d, "63 store=9 set=absent")
	require.Contains(t, d, "64 store=absent set=1")
	require.NotContains(t, d, "61 store", "an equal figure is not listed")
	require.Empty(t, strings.TrimSpace(strings.TrimPrefix(liveStakeDiff(want, want),
		"differing=0 of store=3 set=3 keys")))
}

func TestLiveStakeDiffBoundsTheKeysListed(t *testing.T) {
	t.Parallel()
	want := map[string]uint64{}
	got := map[string]uint64{}
	for i := range 3 * liveStakeDiffLimit {
		k := fmt.Sprintf("k%02d", i)
		want[k], got[k] = 1, 2
	}
	d := liveStakeDiff(want, got)
	require.Contains(t, d, fmt.Sprintf("differing=%d", 3*liveStakeDiffLimit))
	require.Equal(t, liveStakeDiffLimit, strings.Count(d, " store=1 set=2"))
}
