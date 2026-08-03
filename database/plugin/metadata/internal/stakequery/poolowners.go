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

package stakequery

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
)

// PopulatePoolRegistrationOwners loads and attaches the owners for a set of
// pool registrations. Queries are chunked according to the backend's bind
// variable capacity.
func PopulatePoolRegistrationOwners(
	db *gorm.DB,
	registrations []models.PoolRegistration,
) error {
	if len(registrations) == 0 {
		return nil
	}
	ids := make([]uint, 0, len(registrations))
	for _, registration := range registrations {
		ids = append(ids, registration.ID)
	}

	ownersByRegistration := make(
		map[uint][]models.PoolRegistrationOwner,
		len(registrations),
	)
	chunkSize := poolQueryChunkSize(db)
	for start := 0; start < len(ids); start += chunkSize {
		end := min(start+chunkSize, len(ids))
		var owners []models.PoolRegistrationOwner
		if err := db.Where(
			"pool_registration_id IN ?",
			ids[start:end],
		).Find(&owners).Error; err != nil {
			return fmt.Errorf("query pool registration owners: %w", err)
		}
		for _, owner := range owners {
			ownersByRegistration[owner.PoolRegistrationID] = append(
				ownersByRegistration[owner.PoolRegistrationID],
				owner,
			)
		}
	}
	for i := range registrations {
		registrations[i].Owners = ownersByRegistration[registrations[i].ID]
	}
	return nil
}
