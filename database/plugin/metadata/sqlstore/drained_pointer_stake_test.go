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

// Pointer stake resolves pointers through the account and stake registration
// tables, so a read of it refuses once the accounts family is drained and
// answers as before while it is not.
func TestPointerStakeInputsRefuseADrainedAccountsFamily(t *testing.T) {
	t.Parallel()
	pool := bytes.Repeat([]byte{0x32}, 28)
	read := func(s *Store) error {
		_, err := s.GetPointerStakeInputsForPools(
			[][]byte{pool}, 10, 12, 0, nil,
		)
		return err
	}
	store := newManagementTestStore(t)
	require.NotErrorIs(t, read(store), nodesettings.ErrFamilyDrained)

	store.SetDrainedFamilies(
		nodesettings.FamilyMask(0).With(nodesettings.FamilyAccounts),
	)

	require.ErrorIs(t, read(store), nodesettings.ErrFamilyDrained)
}
