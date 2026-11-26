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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// A set that stopped following the ledger holds rows of blocks the ledger
// has since rolled back or never applied. Every protocol parameter, proposal
// and sync marker read must refuse it, as the other families' reads do.
func TestPParamsReadsRefuseAFailedSet(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}
	fxAttachLedgerDB(t, ls)

	encoded, err := cbor.Encode(&shelley.ShelleyProtocolParameters{
		ProtocolMajor: 2,
	})
	require.NoError(t, err)
	era := eras.ShelleyEraDesc.Id
	ldb := ls.ledgerDB()
	require.NoError(t, ldb.SetPParams(encoded, 0, 0, era, nil))
	require.NoError(t, ldb.SetSyncState(
		database.SyntheticV2CostModelSyncKey, "true", nil,
	))
	require.NoError(t, fxSeedPParamUpdate(
		t, db, []byte{0x01}, encoded, 0, 0,
	))
	decode := func(data []byte) (lcommon.ProtocolParameters, error) {
		var p shelley.ShelleyProtocolParameters
		_, err := cbor.Decode(data, &p)
		return &p, err
	}
	src := setPParamsSource{ls}
	noUpdate := func(models.PParamUpdate) error { return nil }

	// While the set follows the ledger, every reader answers.
	got, err := ldb.GetPParams(0, era, decode, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	marker, err := ldb.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "true", marker)
	rows, err := src.PParamsForEra(era)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NoError(t, src.PParamUpdates(noUpdate))
	_, set, err := src.SyncMarker(database.SyntheticV2CostModelSyncKey)
	require.NoError(t, err)
	require.True(t, set)

	ls.utxoSetFailed.Store(true)

	_, err = ldb.GetPParams(0, era, decode, nil)
	require.ErrorIs(t, err, ErrLedgerStateUnserved, "pparamsReader")
	_, err = ldb.ListPParamsForEra(era, nil)
	require.ErrorIs(t, err, ErrLedgerStateUnserved, "pparamsReader")
	_, err = ldb.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	require.ErrorIs(t, err, ErrLedgerStateUnserved, "syncMarkerView")
	_, err = src.PParamsForEra(era)
	require.ErrorIs(t, err, ErrLedgerStateUnserved, "setPParamsSource")
	require.ErrorIs(
		t, src.PParamUpdates(noUpdate), ErrLedgerStateUnserved,
		"setPParamsSource",
	)
	_, _, err = src.SyncMarker(database.SyntheticV2CostModelSyncKey)
	require.ErrorIs(t, err, ErrLedgerStateUnserved, "setPParamsSource")
}
