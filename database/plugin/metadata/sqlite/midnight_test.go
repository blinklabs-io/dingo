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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestMidnightGovernanceDatumLatestAtOrBefore(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	for _, datum := range []*models.MidnightGovernanceDatum{
		{DatumType: models.MidnightGovernanceDatumTypeTechnicalCommittee, Datum: []byte{1}, BlockNumber: 10},
		{DatumType: models.MidnightGovernanceDatumTypeCouncil, Datum: []byte{2}, BlockNumber: 15},
		{DatumType: models.MidnightGovernanceDatumTypeTechnicalCommittee, Datum: []byte{3}, BlockNumber: 20},
	} {
		require.NoError(t, store.InsertMidnightGovernanceDatum(nil, datum))
	}

	got, err := store.GetLatestMidnightGovernanceDatum(
		models.MidnightGovernanceDatumTypeTechnicalCommittee,
		19,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, got.Datum)

	got, err = store.GetLatestMidnightGovernanceDatum(
		models.MidnightGovernanceDatumTypeTechnicalCommittee,
		20,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{3}, got.Datum)

	got, err = store.GetLatestMidnightGovernanceDatum(
		models.MidnightGovernanceDatumTypeTechnicalCommittee,
		9,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestGetMidnightCandidatesUsesMetadataDatumRows verifies candidate restore
// reads live UTxO and datum metadata without requiring block CBOR.
// It also ensures spent candidate UTxOs are excluded from startup hydration.
func TestGetMidnightCandidatesUsesMetadataDatumRows(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	const candidateAddress = "addr1qytna5k2fq9ler0fuk45j7zfwv7t2zwhp777nvdjqqfr5tz8ztpwnk8zq5ngetcz5k5mckgkajnygtsra9aej2h3ek5seupmvd"
	addr, err := ledger.NewAddress(candidateAddress)
	require.NoError(t, err)

	rawDatum := []byte{0x01}
	datumHash := lcommon.Blake2b256Hash(rawDatum)
	require.NoError(t, store.DB().Create(&models.Datum{
		Hash:      datumHash.Bytes(),
		RawDatum:  rawDatum,
		AddedSlot: 1,
	}).Error)

	txHash := make([]byte, 32)
	txHash[0] = 0x42
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       txHash,
		PaymentKey: addr.PaymentKeyHash().Bytes(),
		StakingKey: addr.StakeKeyHash().Bytes(),
		DatumHash:  datumHash.Bytes(),
		OutputIdx:  7,
		AddedSlot:  1,
	}).Error)
	spentTxHash := make([]byte, 32)
	spentTxHash[0] = 0x43
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:        spentTxHash,
		PaymentKey:  addr.PaymentKeyHash().Bytes(),
		StakingKey:  addr.StakeKeyHash().Bytes(),
		DatumHash:   datumHash.Bytes(),
		OutputIdx:   8,
		AddedSlot:   1,
		DeletedSlot: 2,
	}).Error)

	got, err := store.GetMidnightCandidates(addr, nil)
	require.NoError(t, err)
	require.Equal(t, []models.Utxo{{
		TxId:      txHash,
		OutputIdx: 7,
		Datum:     rawDatum,
	}}, got)
}
