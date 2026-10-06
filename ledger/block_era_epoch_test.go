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
	"github.com/stretchr/testify/require"
)

func TestPreviousEraEpoch(t *testing.T) {
	t.Parallel()

	epochs := []models.Epoch{
		{EpochId: 8, EraId: 5},
		{EpochId: 9, EraId: 6},
		{EpochId: 10, EraId: 6},
		{EpochId: 11, EraId: 7},
		{EpochId: 12, EraId: 7},
	}
	tests := []struct {
		name      string
		epochID   uint64
		epochEra  uint
		blockEra  uint
		wantOK    bool
		wantEpoch uint64
		noEpochs  bool
	}{
		{name: "block of its epoch era", epochID: 11, epochEra: 7, blockEra: 7},
		{
			name:      "previous-era block in the boundary epoch",
			epochID:   11,
			epochEra:  7,
			blockEra:  6,
			wantOK:    true,
			wantEpoch: 10,
		},
		{
			name:      "previous-era block in a later epoch",
			epochID:   12,
			epochEra:  7,
			blockEra:  6,
			wantOK:    true,
			wantEpoch: 10,
		},
		{
			name:     "no earlier epoch of the block era",
			epochID:  9,
			epochEra: 6,
			blockEra: 4,
		},
		{
			name:     "no epochs",
			epochID:  11,
			epochEra: 7,
			blockEra: 6,
			noEpochs: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			list := epochs
			if test.noEpochs {
				list = nil
			}
			got, ok := PreviousEraEpoch(
				list,
				test.epochID,
				test.epochEra,
				test.blockEra,
			)
			require.Equal(t, test.wantOK, ok)
			if test.wantOK {
				require.Equal(t, test.wantEpoch, got.EpochId)
				require.Equal(t, test.blockEra, got.EraId)
			}
		})
	}
}
