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
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// Every store read that answers from the dreps, accounts, pools-history or
// live-stake rows refuses with ErrFamilyDrained once its family is drained,
// instead of answering from the emptied table, and answers as before while
// none is.
func TestReadersOfDrainedFamiliesFailClosed(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x31}, 28)
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x32}, 28))
	drepRefs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, key),
	}
	predefined := uint64(models.DrepTypeAlwaysAbstain)
	const (
		accounts = nodesettings.FamilyAccounts
		dreps    = nodesettings.FamilyDReps
		pools    = nodesettings.FamilyPoolsHistory
		live     = nodesettings.FamilyLiveStake
	)
	for _, tc := range []struct {
		name     string
		families []nodesettings.Family
		read     func(*Store) error
	}{
		{"drep page", []nodesettings.Family{dreps, live}, func(s *Store) error {
			_, _, err := s.GetDrepsPage(models.DrepListPageQuery{Limit: 10}, nil)
			return err
		}},
		{"drep list", []nodesettings.Family{dreps}, func(s *Store) error {
			_, err := s.GetDreps(nil)
			return err
		}},
		{
			"drep registration deposits", []nodesettings.Family{dreps},
			func(s *Store) error {
				_, err := s.GetDrepLastRegistrationDeposits(nil)
				return err
			},
		},
		{
			"drep voting power", []nodesettings.Family{accounts, live},
			func(s *Store) error {
				_, err := s.GetDRepVotingPower(0, key, 0, nil)
				return err
			},
		},
		{
			"drep voting power batch", []nodesettings.Family{accounts, live},
			func(s *Store) error {
				_, err := s.GetDRepVotingPowerBatch(drepRefs, 0, nil)
				return err
			},
		},
		{
			"drep voting power by type", []nodesettings.Family{accounts, live},
			func(s *Store) error {
				_, err := s.GetDRepVotingPowerByType([]uint64{predefined}, 0, nil)
				return err
			},
		},
		{
			"drep delegator stakes",
			[]nodesettings.Family{accounts, dreps, live},
			func(s *Store) error {
				_, _, err := s.GetDRepDelegatorStakes(
					0, key, nil, 10, 0, "asc", nil,
				)
				return err
			},
		},
		{
			"predefined drep delegator stakes",
			[]nodesettings.Family{accounts, live},
			func(s *Store) error {
				_, _, err := s.GetDRepDelegatorStakes(
					0, nil, &predefined, 10, 0, "asc", nil,
				)
				return err
			},
		},
		{"stake by pools", []nodesettings.Family{accounts, live}, func(s *Store) error {
			_, _, err := s.GetStakeByPools([][]byte{pool.Bytes()}, nil)
			return err
		}},
		{"stake by pool", []nodesettings.Family{accounts, live}, func(s *Store) error {
			_, _, err := s.GetStakeByPool(pool.Bytes(), nil)
			return err
		}},
		{
			"active pool stake total",
			[]nodesettings.Family{accounts, live, pools},
			func(s *Store) error {
				_, err := s.GetActivePoolStakeTotal(nil)
				return err
			},
		},
		{"pool", []nodesettings.Family{pools}, func(s *Store) error {
			_, err := s.GetPool(pool, true, nil)
			return err
		}},
		{"retiring pools page", []nodesettings.Family{pools}, func(s *Store) error {
			_, _, err := s.GetRetiringPoolsPage(5, 10, 0, false, nil)
			return err
		}},
		{"retired pools page", []nodesettings.Family{pools}, func(s *Store) error {
			_, _, err := s.GetRetiredPoolsPage(5, 10, 0, false, nil)
			return err
		}},
		{"active pools page", []nodesettings.Family{pools}, func(s *Store) error {
			_, _, err := s.GetActivePoolKeyHashesPage(10, 0, false, nil)
			return err
		}},
		{"pool certificate events", []nodesettings.Family{pools}, func(s *Store) error {
			_, _, err := s.GetPoolCertificateEvents(pool.Bytes(), 10, 0, "asc", nil)
			return err
		}},
		{"pool delegators", []nodesettings.Family{live}, func(s *Store) error {
			_, _, err := s.GetPoolDelegators(pool.Bytes(), 10, 0, "asc", nil)
			return err
		}},
		{"account sums", []nodesettings.Family{accounts}, func(s *Store) error {
			_, err := s.GetAccountSumsByCredential(0, key, nil)
			return err
		}},
		{"account withdrawals", []nodesettings.Family{accounts}, func(s *Store) error {
			_, err := s.GetAccountWithdrawalHistoryByCredential(
				0, key, 10, 0, "asc", nil,
			)
			return err
		}},
		{
			"account withdrawal count", []nodesettings.Family{accounts},
			func(s *Store) error {
				_, err := s.CountAccountWithdrawalHistoryByCredential(0, key, nil)
				return err
			},
		},
		{
			"account registrations", []nodesettings.Family{accounts},
			func(s *Store) error {
				_, err := s.GetAccountRegistrationHistoryByCredential(
					0, key, 10, 0, "asc", nil,
				)
				return err
			},
		},
		{
			"account registration count", []nodesettings.Family{accounts},
			func(s *Store) error {
				_, err := s.CountAccountRegistrationHistoryByCredential(0, key, nil)
				return err
			},
		},
		{"off-chain DRep anchors", []nodesettings.Family{dreps}, func(s *Store) error {
			_, err := s.EnsureOffchainMetadataPointers(
				context.Background(), time.Unix(0, 0), nil,
			)
			return err
		}},
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

// An account rollback that would restore a pool delegation reads the pool
// retirement history to see whether the pool was reaped since, so it refuses
// when only the pool rows are gone rather than restore the delegation the
// reap removed.
func TestAccountRollbackRefusesWhenPoolHistoryIsDrained(t *testing.T) {
	t.Parallel()
	for _, drained := range []bool{false, true} {
		store := newManagementTestStore(t)
		key := snapshotStakingKey(0x6e)
		poolA := []byte{0xaa, 0xaa}
		poolB := []byte{0xbb, 0xbb}
		importSnapshotAccount(t, store, key, poolA, nil, 0, 100)
		execAccountSQL(t, store, `
INSERT INTO stake_delegation (staking_key, credential_tag, pool_key_hash, added_slot)
VALUES (?, 0, ?, 200)`, key, poolB)
		execAccountSQL(t, store, `
UPDATE account SET pool = ?, added_slot = 300
WHERE credential_tag = 0 AND staking_key = ?`, poolA, key)
		if drained {
			store.SetDrainedFamilies(
				nodesettings.FamilyMask(0).
					With(nodesettings.FamilyPoolsHistory),
			)
		}

		err := store.RestoreAccountStateAtSlot(250, nil)

		if drained {
			require.ErrorIs(t, err, nodesettings.ErrFamilyDrained)
			continue
		}
		require.NoError(t, err)
		got, err := store.GetAccountByCredential(0, key, true, nil)
		require.NoError(t, err)
		require.Equal(t, poolB, got.Pool)
	}
}
