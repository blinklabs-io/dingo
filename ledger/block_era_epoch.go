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
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
)

// PreviousEraEpoch returns the last epoch before epochID whose era is
// blockEraID, for a block of an earlier era than the era epochEraID recorded
// for its epoch. The epoch that starts an era is recorded with the successor
// era, while transaction validation judges a block of the previous era applied
// there under the previous era's parameters, those of that era's last epoch.
// Historical paths that select protocol parameters by epoch use the returned
// epoch so they apply the same parameters. ok is false for a block of its
// epoch's era, or when no earlier epoch of the block's era is known.
func PreviousEraEpoch(
	epochs []models.Epoch,
	epochID uint64,
	epochEraID, blockEraID uint,
) (models.Epoch, bool) {
	if blockEraID == epochEraID {
		return models.Epoch{}, false
	}
	for _, ep := range slices.Backward(epochs) {
		if ep.EpochId < epochID && ep.EraId == blockEraID {
			return ep, true
		}
	}
	return models.Epoch{}, false
}
