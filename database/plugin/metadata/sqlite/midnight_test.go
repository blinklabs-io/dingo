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

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
)

func TestMidnightGovernanceDatumLatestAtOrBefore(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	for _, datum := range []*models.MidnightGovernanceDatum{
		{DatumType: models.MidnightGovernanceDatumTypeTechnicalCommittee, Datum: []byte{1}, BlockNumber: 10},
		{DatumType: models.MidnightGovernanceDatumTypeCouncil, Datum: []byte{2}, BlockNumber: 15},
		{DatumType: models.MidnightGovernanceDatumTypeTechnicalCommittee, Datum: []byte{3}, BlockNumber: 20},
	} {
		require.NoError(t, store.InsertMidnightGovernanceDatum(datum, nil))
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
