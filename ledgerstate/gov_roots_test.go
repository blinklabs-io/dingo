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

package ledgerstate

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

func TestImportGovStatePurposeRootPresence(t *testing.T) {
	t.Parallel()

	names := []string{"param-update", "hard-fork", "committee", "constitution"}
	groups := [][]uint8{
		{govActionTypeParameterChange},
		{govActionTypeHardForkInitiation},
		{govActionTypeNoConfidence, govActionTypeUpdateCommittee},
		{govActionTypeNewConstitution},
	}
	for i := range names {
		t.Run(names[i]+"/present", func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
			require.NoError(t, err)
			var roots [4]*ParsedGovActionId
			roots[i] = &ParsedGovActionId{
				TxHash: bytes.Repeat([]byte{byte(0x70 + i)}, 32),
			}
			require.NoError(t, importGovState(
				context.Background(),
				govImportConfigForTest(
					db, govStateWithRoots(t, roots, false),
				),
				func(ImportProgress) {},
			))
			for j, g := range groups {
				root, err := db.GetLastEnactedGovernanceProposal(g, nil)
				require.NoError(t, err)
				if j == i {
					require.NotNil(t, root)
					require.Equal(t, roots[i].TxHash, root.TxHash)
				} else {
					require.Nil(t, root, "unexpected root for %s", names[j])
				}
			}
		})
	}

	t.Run("all-absent", func(t *testing.T) {
		t.Parallel()
		db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
		require.NoError(t, err)
		require.NoError(t, importGovState(
			context.Background(),
			govImportConfigForTest(
				db, govStateWithRoots(t, [4]*ParsedGovActionId{}, false),
			),
			func(ImportProgress) {},
		))
		for _, g := range groups {
			root, err := db.GetLastEnactedGovernanceProposal(g, nil)
			require.NoError(t, err)
			require.Nil(t, root)
		}
	})
}

// A snapshot root that collides with a pending proposal row is skipped by
// the seeder, leaving no enacted row; the import must fail rather than
// accept an unseeded root.
func TestImportGovStateRejectsUnseededPurposeRoot(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	root := &ParsedGovActionId{TxHash: bytes.Repeat([]byte{0x11}, 32)}
	pending := govActionStateForTest(
		root.TxHash, 0, govActionTypeParameterChange, nil, 499,
	)
	data := govStateWithRootsAndProposals(
		t,
		[4]*ParsedGovActionId{root, nil, nil, nil},
		false,
		[]any{pending},
		nil,
	)
	err = importGovState(
		context.Background(),
		govImportConfigForTest(db, data),
		func(ImportProgress) {},
	)
	require.ErrorContains(t, err, "is not an enacted governance proposal")
}
