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

//go:build dingo_extra_plugins

package postgres

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// GetMIRCertsInSlotRange returns the processed effects of all MIR certificates
// whose added_slot is >= startSlot and < endSlot. Both distribution MIR certs
// (credential→amount pairs) and pot-to-pot transfer MIR certs are returned.
func (d *MetadataStorePostgres) GetMIRCertsInSlotRange(
	startSlot, endSlot uint64,
	txn types.Txn,
) ([]models.MIREffect, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf("GetMIRCertsInSlotRange: resolve db: %w", err)
	}

	var mirs []models.MoveInstantaneousRewards
	if err := db.Preload("Rewards").
		Where("added_slot >= ? AND added_slot < ?", startSlot, endSlot).
		Order("added_slot ASC, id ASC").
		Find(&mirs).Error; err != nil {
		return nil, fmt.Errorf("GetMIRCertsInSlotRange: query: %w", err)
	}

	effects := make([]models.MIREffect, len(mirs))
	for i, m := range mirs {
		effects[i].ID = m.ID
		effects[i].Pot = m.Pot
		effects[i].OtherPot = uint64(m.OtherPot)
		effects[i].Rewards = make([]models.MIRReward, len(m.Rewards))
		for j, r := range m.Rewards {
			effects[i].Rewards[j] = models.MIRReward{
				Credential:    r.Credential,
				CredentialTag: r.CredentialTag,
				Amount:        uint64(r.Amount),
			}
		}
	}
	return effects, nil
}
