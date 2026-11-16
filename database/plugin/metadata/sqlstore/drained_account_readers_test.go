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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The account lookups and lists refuse with ErrFamilyDrained once accounts is
// drained, rather than answer not found or empty from the emptied table.
func TestAccountLookupsRefuseWhenAccountsAreDrained(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x33}, 28)
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x34}, 28))
	refs := []models.StakeCredentialRef{models.NewStakeCredentialRef(0, key)}
	for _, tc := range []struct {
		name string
		read func(*Store) error
	}{
		{"account", func(s *Store) error {
			_, err := s.GetAccountByCredential(0, key, true, nil)
			return err
		}},
		{"accounts", func(s *Store) error {
			_, err := s.GetAccountsByCredential(refs, true, nil)
			return err
		}},
		{"active credentials", func(s *Store) error {
			_, err := s.GetActiveAccountCredentials(nil)
			return err
		}},
		{"delegator counts", func(s *Store) error {
			_, err := s.GetActiveDelegatorCountsByPool(
				[]lcommon.PoolKeyHash{pool}, nil,
			)
			return err
		}},
		{"delegator credentials", func(s *Store) error {
			_, err := s.GetActiveDelegatorCredentialsByPool(
				[]lcommon.PoolKeyHash{pool}, nil,
			)
			return err
		}},
		{"drep delegators", func(s *Store) error {
			_, err := s.GetDRepDelegators(0, key, nil)
			return err
		}},
		{"predefined drep delegators", func(s *Store) error {
			_, err := s.GetDRepDelegators(2, nil, nil)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			require.NotErrorIs(
				t, tc.read(store), nodesettings.ErrFamilyDrained,
				"nothing is drained",
			)
			store.SetDrainedFamilies(
				nodesettings.FamilyMask(0).With(nodesettings.FamilyAccounts),
			)
			require.ErrorIs(t, tc.read(store), nodesettings.ErrFamilyDrained)
		})
	}
}
