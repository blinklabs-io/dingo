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

package database

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	gouroboros "github.com/blinklabs-io/gouroboros/ledger"
)

// GetMidnightCandidates returns the live, materialized UTxOs at the configured
// committee-candidate address so the in-memory index survives restarts.
func (d *Database) GetMidnightCandidates(address string) ([]models.Utxo, error) {
	addr, err := gouroboros.NewAddress(address)
	if err != nil {
		return nil, fmt.Errorf("parse Midnight candidate address: %w", err)
	}
	return d.UtxosByAddress(addr, nil)
}

// InsertMidnightGovernanceDatum inserts a new governance datum row.
// Always inserts — never overwrites — so the latest datum is found by
// querying with ORDER BY block_number DESC.
func (d *Database) InsertMidnightGovernanceDatum(
	datum *models.MidnightGovernanceDatum,
) error {
	txn := d.MetadataTxn(true)
	defer txn.Release()
	if err := d.metadata.InsertMidnightGovernanceDatum(datum, txn.Metadata()); err != nil {
		return fmt.Errorf("insert midnight governance datum: %w", err)
	}
	return txn.Commit()
}

// GetLatestMidnightGovernanceDatum returns the newest datum of datumType at or
// before blockNumber, or nil when no matching datum exists.
func (d *Database) GetLatestMidnightGovernanceDatum(
	datumType string,
	blockNumber uint64,
) (*models.MidnightGovernanceDatum, error) {
	return d.metadata.GetLatestMidnightGovernanceDatum(datumType, blockNumber, nil)
}

// GetLatestMidnightAriadneParams returns the most recently stored Ariadne
// parameters row (ordered by epoch DESC), or nil if none exist.
func (d *Database) GetLatestMidnightAriadneParams() (*models.MidnightAriadneParams, error) {
	return d.metadata.GetLatestMidnightAriadneParams(nil)
}

// UpsertMidnightAriadneParams inserts or updates the Ariadne params row for
// the given epoch. If a row for that epoch already exists, its datum is
// updated with the new value.
func (d *Database) UpsertMidnightAriadneParams(
	params *models.MidnightAriadneParams,
) error {
	txn := d.MetadataTxn(true)
	defer txn.Release()
	if err := d.metadata.UpsertMidnightAriadneParams(params, txn.Metadata()); err != nil {
		return fmt.Errorf("upsert midnight ariadne params: %w", err)
	}
	return txn.Commit()
}

// UpsertMidnightEpochCandidates inserts or replaces the committee-candidate
// snapshot for the given epoch.
func (d *Database) UpsertMidnightEpochCandidates(
	ec *models.MidnightEpochCandidates,
) error {
	txn := d.MetadataTxn(true)
	defer txn.Release()
	if err := d.metadata.UpsertMidnightEpochCandidates(ec, txn.Metadata()); err != nil {
		return fmt.Errorf("upsert midnight epoch candidates: %w", err)
	}
	return txn.Commit()
}
