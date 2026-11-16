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

package ledger_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// TestBoundaryPromotesTheStagedRegistrationOfALoadedPool loads a pool whose
// metadata rows hold the registration in force and a staged one at the same
// slot, as a snapshot import writes them, and requires the epoch boundary to
// promote the staged registration.
func TestBoundaryPromotesTheStagedRegistrationOfALoadedPool(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set, "")

	pool := bytes.Repeat([]byte{0x52}, 28)
	vrfNow := bytes.Repeat([]byte{0x53}, 32)
	vrfNext := bytes.Repeat([]byte{0x71}, 32)
	for i, vrf := range [][]byte{vrfNow, vrfNext} {
		record := &models.Pool{
			PoolKeyHash: bytes.Clone(pool), VrfKeyHash: bytes.Clone(vrf),
			Pledge: dbtypes.Uint64(1), Cost: dbtypes.Uint64(1),
		}
		reg := &models.PoolRegistration{
			PoolKeyHash: bytes.Clone(pool), VrfKeyHash: bytes.Clone(vrf),
			AddedSlot: 11, Pledge: dbtypes.Uint64(1), Cost: dbtypes.Uint64(1),
			RegistrationIndex: uint(i),
		}
		require.NoError(t, scenario.DB.Metadata().ImportPool(record, reg, nil))
	}
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	st, err := set.PoolState(pool)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.Equal(t, vrfNow, st.Params.VrfKeyHash)
	require.NotNil(t, st.Future, "the staged registration is held")
	require.Equal(t, vrfNext, st.Future.VrfKeyHash)

	scenario.ApplyBoundaryQuiet()

	st, err = set.PoolState(pool)
	require.NoError(t, err)
	require.NotNil(t, st)
	require.Equal(t, vrfNext, st.Params.VrfKeyHash, "the boundary promotes the staged registration")
	require.Nil(t, st.Future)
}
