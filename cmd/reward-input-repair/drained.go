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

package main

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/nodesettings"
)

var errDrainedDatabase = errors.New(
	"the metadata database has drained a ledger-state family",
)

// refuseDrainedDatabase refuses a database that deleted the rows of any
// ledger-state family: the repair reads reward_credit_round and the stake
// inputs, and would take their absence for an uncredited round. A database
// without the latch gate table predates the latch and drained nothing.
func refuseDrainedDatabase(db *sql.DB) error {
	var value string
	err := db.QueryRow(
		`SELECT value FROM node_settings_gate WHERE name = ?`,
		nodesettings.LedgerStateDrainedGateName,
	).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		if !gateTableExists(db) {
			return nil
		}
		return fmt.Errorf("read ledger-state latch: %w", err)
	}
	mask, err := nodesettings.DecodeFamilyMask(value)
	if err != nil {
		return err
	}
	if mask != 0 {
		return fmt.Errorf("%w: %v", errDrainedDatabase, mask.Families())
	}
	return nil
}

func gateTableExists(db *sql.DB) bool {
	var probe int
	err := db.QueryRow(`SELECT 1 FROM node_settings_gate LIMIT 1`).Scan(&probe)
	return err == nil || errors.Is(err, sql.ErrNoRows)
}
