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
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// Labels are stored as text, so the list must order them as numbers: 2
// before 10, and the largest uint64 label last rather than clamped.
func TestListMetadataLabelCountsOrdersLabelsNumerically(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	_, err := store.writeDB.Exec(`
INSERT INTO transaction_metadata_label (transaction_id, label, slot) VALUES
  (1, '674', 10), (2, '674', 20),
  (3, '10', 30), (4, '10', 40), (5, '10', 50),
  (6, '2', 60),
  (7, '18446744073709551615', 70),
  (8, '721', 80)`)
	require.NoError(t, err)

	ascending := []models.MetadataLabelCount{
		{Label: 2, Count: 1},
		{Label: 10, Count: 3},
		{Label: 674, Count: 2},
		{Label: 721, Count: 1},
		{Label: math.MaxUint64, Count: 1},
	}
	got, err := store.ListMetadataLabelCounts(0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, ascending, got)

	got, err = store.ListMetadataLabelCounts(0, 0, true, nil)
	require.NoError(t, err)
	require.Equal(t, []models.MetadataLabelCount{
		ascending[4], ascending[3], ascending[2], ascending[1], ascending[0],
	}, got)

	got, err = store.ListMetadataLabelCounts(2, 1, false, nil)
	require.NoError(t, err)
	require.Equal(t, ascending[1:3], got)

	got, err = store.ListMetadataLabelCounts(0, 3, false, nil)
	require.NoError(t, err)
	require.Equal(t, ascending[3:], got)

	count, err := store.CountMetadataLabels(nil)
	require.NoError(t, err)
	require.Equal(t, len(ascending), count)
}
