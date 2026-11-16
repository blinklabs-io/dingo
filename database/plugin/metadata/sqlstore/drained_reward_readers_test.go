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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

// Every store read of the reward path that joins the account tables, the
// live stake aggregate, the pool registrations or the reward outputs and
// credited rounds refuses with ErrFamilyDrained once one of them is drained,
// and answers as before while none is.
func TestRewardPathReadersFailClosedOverDrainedTables(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x31}, 28)
	pool := bytes.Repeat([]byte{0x32}, 28)
	const (
		accounts = nodesettings.FamilyAccounts
		pools    = nodesettings.FamilyPoolsHistory
		live     = nodesettings.FamilyLiveStake
		outputs  = nodesettings.FamilyRewardOutputs
	)
	historical := []nodesettings.Family{accounts, pools, live, outputs}
	for _, tc := range []struct {
		name     string
		families []nodesettings.Family
		read     func(*Store) error
	}{
		{"stake by pools at slot", historical, func(s *Store) error {
			_, _, err := s.GetStakeByPoolsAtSlot([][]byte{pool}, 10, 0, 0, nil)
			return err
		}},
		{"epoch boundary stake by pools", historical, func(s *Store) error {
			_, _, err := s.GetEpochBoundaryStakeByPools(
				[][]byte{pool}, 10, 12, nil, 0, 0, nil,
			)
			return err
		}},
		{"pool owner stake at slot", historical, func(s *Store) error {
			_, err := s.GetPoolOwnerStakeAtSlot([][]byte{key}, 10, 0, 0, nil)
			return err
		}},
		{"reward stake inputs for pools", historical, func(s *Store) error {
			_, err := s.GetRewardStakeInputsForPools([][]byte{pool}, 10, 0, 0, nil)
			return err
		}},
		{"epoch boundary reward stake inputs", historical, func(s *Store) error {
			_, err := s.GetEpochBoundaryRewardStakeInputsForPools(
				[][]byte{pool}, 10, 12, nil, 0, 0, nil,
			)
			return err
		}},
		{"epoch boundary delegated pools", historical, func(s *Store) error {
			_, err := s.GetEpochBoundaryDelegatedPoolKeyHashes(10, 12, nil)
			return err
		}},
		{"live stake inputs for pools", historical, func(s *Store) error {
			_, err := s.GetLiveStakeInputsForPools([][]byte{pool}, 0, nil)
			return err
		}},
		{"post-snapshot credits", historical, func(s *Store) error {
			_, err := s.GetPostSnapshotRewardCredits(10, nil)
			return err
		}},
		{"post-snapshot credit events", historical, func(s *Store) error {
			_, err := s.GetPostSnapshotRewardCreditEvents(10, nil)
			return err
		}},
		{
			"reward credits already applied",
			[]nodesettings.Family{accounts},
			func(s *Store) error {
				_, err := s.RewardCreditsAlreadyApplied(
					[]models.AccountRewardCredit{{SourceHash: []byte{1}}}, nil,
				)
				return err
			},
		},
		{
			"pending drep credits",
			[]nodesettings.Family{accounts, outputs},
			func(s *Store) error {
				db, _, err := s.readDBFromTxn(nil)
				if err != nil {
					return err
				}
				_, _, err = s.pendingDRepCredits(
					t.Context(), db, 0, "drep", []any{key},
				)
				return err
			},
		},
		{
			"reward history by credential",
			[]nodesettings.Family{outputs},
			func(s *Store) error {
				_, err := s.GetRewardAccountOutputsByCredential(
					0, key, 10, 0, "asc", nil,
				)
				return err
			},
		},
		{
			"reward history count",
			[]nodesettings.Family{outputs},
			func(s *Store) error {
				_, err := s.CountRewardAccountOutputsByCredential(0, key, nil)
				return err
			},
		},
	} {
		for _, family := range tc.families {
			t.Run(tc.name+"/"+family.String(), func(t *testing.T) {
				t.Parallel()
				store := newManagementTestStore(t)
				require.NotErrorIs(
					t, tc.read(store), nodesettings.ErrFamilyDrained,
					"nothing is drained",
				)
				store.SetDrainedFamilies(
					nodesettings.FamilyMask(0).With(family),
				)
				require.ErrorIs(
					t, tc.read(store), nodesettings.ErrFamilyDrained,
				)
			})
		}
	}
}
