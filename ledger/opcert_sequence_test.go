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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestLatestOpCertSequenceTracksHighestObservedAndRollback(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}

	var poolID [28]byte
	for i := range poolID {
		poolID[i] = byte(i + 1)
	}
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolID[:]))
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
		},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  make([]byte, 32),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	sequence, found, err := ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, uint64(0), sequence)

	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 3, 10, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 7, 20, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 5, 30, nil))

	sequence, found, err = ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(7), sequence)

	require.NoError(t, db.RestorePoolStateAtSlot(15, nil))
	sequence, found, err = ls.LatestOpCertSequence(poolID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(3), sequence)
}
