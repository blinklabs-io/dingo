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
func (d *Database) GetMidnightCandidates(
	address string,
) ([]models.Utxo, error) {
	addr, err := gouroboros.NewAddress(address)
	if err != nil {
		return nil, fmt.Errorf("parse Midnight candidate address: %w", err)
	}
	if err := d.requireStore("GetMidnightCandidates"); err != nil {
		return nil, err
	}
	return d.metadata.GetMidnightCandidates(addr, nil)
}

// GetLatestMidnightGovernanceDatum returns the newest datum of datumType at or
// before blockNumber, or nil when no matching datum exists.
func (d *Database) GetLatestMidnightGovernanceDatum(
	datumType string,
	blockNumber uint64,
) (*models.MidnightGovernanceDatum, error) {
	if err := d.requireStore("GetLatestMidnightGovernanceDatum"); err != nil {
		return nil, err
	}
	return d.metadata.GetLatestMidnightGovernanceDatum(
		datumType,
		blockNumber,
		nil,
	)
}

// GetLatestMidnightAriadneParams returns the most recently stored Ariadne
// parameters row (ordered by epoch DESC), or nil if none exist.
func (d *Database) GetLatestMidnightAriadneParams() (*models.MidnightAriadneParams, error) {
	if err := d.requireStore("GetLatestMidnightAriadneParams"); err != nil {
		return nil, err
	}
	return d.metadata.GetLatestMidnightAriadneParams(nil)
}

// GetMidnightAriadneParamsAtOrBeforeEpoch returns the newest Ariadne params
// row at or before epoch, or nil when none exists.
func (d *Database) GetMidnightAriadneParamsAtOrBeforeEpoch(
	epoch uint64,
) (*models.MidnightAriadneParams, error) {
	if err := d.requireStore("GetMidnightAriadneParamsAtOrBeforeEpoch"); err != nil {
		return nil, err
	}
	return d.metadata.GetMidnightAriadneParamsAtOrBeforeEpoch(epoch, nil)
}

// GetMidnightEpochCandidatesByEpoch returns the committee-candidate snapshot
// for one epoch, or nil when none exists.
func (d *Database) GetMidnightEpochCandidatesByEpoch(
	epoch uint64,
) (*models.MidnightEpochCandidates, error) {
	if err := d.requireStore("GetMidnightEpochCandidatesByEpoch"); err != nil {
		return nil, err
	}
	return d.metadata.GetMidnightEpochCandidatesByEpoch(epoch, nil)
}

// GetMidnightCommitteeCandidateRegistrationsByTxHashes returns every
// candidate-registration provenance row whose tx_hash is in txHashes.
func (d *Database) GetMidnightCommitteeCandidateRegistrationsByTxHashes(
	txHashes [][]byte,
) ([]models.MidnightCommitteeCandidateRegistration, error) {
	if err := d.requireStore("GetMidnightCommitteeCandidateRegistrationsByTxHashes"); err != nil {
		return nil, err
	}
	return d.metadata.GetMidnightCommitteeCandidateRegistrationsByTxHashes(
		txHashes,
		nil,
	)
}
