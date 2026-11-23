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

package sqlstore

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// The Blockfrost stake history readers answer from reward_stake_input, so
// they refuse once reward-inputs is drained and answer before.
func TestStakeInputReadersRefuseADrainedRewardInputsFamily(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x31}, 28)
	for _, tc := range []struct {
		name string
		read func(*Store) error
	}{
		{"epoch stakes", func(s *Store) error {
			_, _, err := s.GetEpochStakes(5, nil, 10, 0, nil)
			return err
		}},
		{"account stake history", func(s *Store) error {
			_, _, err := s.GetAccountStakeHistory(0, key, 9, 10, 0, "asc", nil)
			return err
		}},
		{"earliest stake input epoch", func(s *Store) error {
			_, _, err := s.GetEarliestRewardStakeInputEpoch(nil)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			require.NotErrorIs(
				t, tc.read(store), nodesettings.ErrFamilyDrained,
			)
			store.SetDrainedFamilies(
				nodesettings.FamilyMask(0).
					With(nodesettings.FamilyRewardInputs),
			)
			require.ErrorIs(t, tc.read(store), nodesettings.ErrFamilyDrained)
		})
	}
}

// The imported block counts are read only from the tables a drained
// reward-blocks-fees family deletes, so the read refuses instead of reporting
// an epoch whose counts were never imported.
func TestImportedPoolBlockCountsRefuseADrainedFamily(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	_, _, _, err := store.GetImportedPoolBlockCounts(5, nil)
	require.NotErrorIs(t, err, nodesettings.ErrFamilyDrained)

	store.SetDrainedFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyRewardBlocksFees),
	)
	_, _, _, err = store.GetImportedPoolBlockCounts(5, nil)
	require.ErrorIs(t, err, nodesettings.ErrFamilyDrained)
}
