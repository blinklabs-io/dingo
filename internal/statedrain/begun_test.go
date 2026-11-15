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

package statedrain_test

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/stretchr/testify/require"
)

func TestBegunFamiliesReadsMarkerKeys(t *testing.T) {
	t.Parallel()
	mask, err := statedrain.BegunFamilies([]string{
		statedrain.MarkerPrefix + nodesettings.FamilyDReps.String(),
		statedrain.MarkerPrefix + nodesettings.FamilyNonces.String(),
	})
	require.NoError(t, err)
	require.Equal(
		t,
		nodesettings.FamilyMask(0).
			With(nodesettings.FamilyDReps).
			With(nodesettings.FamilyNonces),
		mask,
	)
	mask, err = statedrain.BegunFamilies(nil)
	require.NoError(t, err)
	require.Zero(t, mask)
	_, err = statedrain.BegunFamilies([]string{statedrain.MarkerPrefix + "x"})
	require.Error(t, err)
}

func pairEntry(f nodesettings.Family, table string) statedrain.Entry {
	return statedrain.Entry{
		Family: f,
		Tables: []statedrain.Table{{Name: table, Key: "id"}},
		Loaded: func() (bool, error) { return true, nil },
		Verify: func(context.Context) error { return nil },
	}
}

// A stop between the dreps latch and the accounts drain leaves exactly one of
// the pair latched; the next run must finish the accounts drain, with or
// without the accounts marker, rather than refuse to start.
func TestRunFinishesAPairSplitByAStop(t *testing.T) {
	t.Parallel()
	for _, marked := range []bool{false, true} {
		t.Run(map[bool]string{false: "no marker", true: "marker"}[marked],
			func(t *testing.T) {
				t.Parallel()
				store := newFakeStore(3)
				store.tables = map[string]*fakeTable{"d": {0}, "a": {2}}
				store.latch = store.latch.With(nodesettings.FamilyDReps)
				if marked {
					store.syncState[statedrain.MarkerPrefix+
						nodesettings.FamilyAccounts.String()] = "1"
				}
				env := statedrain.Env{
					Store: store, Set: currentSet(), ChunkRows: 5,
					VerifyBeforeDrain: true,
				}
				require.NoError(t, statedrain.Run(
					t.Context(), env, []statedrain.Entry{
						pairEntry(nodesettings.FamilyDReps, "d"),
						pairEntry(nodesettings.FamilyAccounts, "a"),
					},
				))
				require.True(t, store.latch.Has(nodesettings.FamilyDReps))
				require.True(t, store.latch.Has(nodesettings.FamilyAccounts))
				require.Zero(t, store.tables["a"].rows)
				require.Empty(t, store.syncState)
			})
	}
}
