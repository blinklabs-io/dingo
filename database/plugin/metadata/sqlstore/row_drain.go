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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"

	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// liveStakeMaintenanceStopped reports whether reward_live_stake is left
// alone: the live-stake family is drained, or its refresh would read the
// account table, which a drained accounts family no longer holds.
func (s *Store) liveStakeMaintenanceStopped() bool {
	return s.rewardLiveStakeDrained.Load() ||
		s.familyDrained(nodesettings.FamilyAccounts)
}

// errDrainedPair reports a certificate that needs the accounts and dreps
// families to be drained together: a DRep delegation change reads the
// account's prior delegation to maintain the DRep's delegator links, and a
// DRep retirement clears the delegating accounts.
var errDrainedPair = errors.New(
	"the accounts and dreps families must be drained together",
)

// drepFamiliesDrained reports whether the DRep delegator links and the
// account rows are both drained, and errDrainedPair when only one is.
func (s *Store) drepFamiliesDrained() (bool, error) {
	accounts := s.familyDrained(nodesettings.FamilyAccounts)
	dreps := s.familyDrained(nodesettings.FamilyDReps)
	if accounts != dreps {
		return false, errDrainedPair
	}
	return dreps, nil
}

var drainIdentifier = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// drainProtected are the tables whose rows are the database's own
// bookkeeping rather than ledger state.
var drainProtected = map[string]bool{
	"schema_migrations":  true,
	"node_settings_gate": true,
	"sync_state":         true,
}

// DeleteRowsChunk deletes up to limit rows of table in ascending order of
// keyColumn, which must be unique, and returns how many it deleted. A result
// below limit means the table is empty. Every row it deletes was already
// committed, so a caller interrupted between chunks resumes by calling it
// again.
func (s *Store) DeleteRowsChunk(
	ctx context.Context,
	table string,
	keyColumn string,
	limit int,
) (int64, error) {
	if limit < 1 {
		return 0, errors.New("delete chunk limit must be positive")
	}
	if !drainIdentifier.MatchString(table) ||
		!drainIdentifier.MatchString(keyColumn) {
		return 0, fmt.Errorf("invalid table or key %q.%q", table, keyColumn)
	}
	if drainProtected[table] {
		return 0, fmt.Errorf("table %q is not ledger state", table)
	}
	quotedTable := s.dialect.QuoteIdentifier(table)
	quotedKey := s.dialect.QuoteIdentifier(keyColumn)
	var bound any
	err := s.writeDB.QueryRowContext(
		ctx,
		s.dialect.Rebind(fmt.Sprintf(
			"%s SELECT %s FROM %s ORDER BY %s LIMIT 1 OFFSET %d",
			nodesettings.DrainStatementTag,
			quotedKey, quotedTable, quotedKey, limit-1,
		)),
	).Scan(&bound)
	var result sql.Result
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Fewer than limit rows remain, so one statement stays within the
		// chunk.
		result, err = s.writeDB.ExecContext(
			ctx,
			//nolint:perfsprint // gosec rejects the concatenation
			fmt.Sprintf(
				"%s DELETE FROM %s",
				nodesettings.DrainStatementTag, quotedTable,
			),
		)
	case err != nil:
		return 0, fmt.Errorf("find chunk bound in %s: %w", table, err)
	default:
		result, err = s.writeDB.ExecContext(
			ctx,
			s.dialect.Rebind(fmt.Sprintf(
				"%s DELETE FROM %s WHERE %s <= ?",
				nodesettings.DrainStatementTag, quotedTable, quotedKey,
			)),
			bound,
		)
	}
	if err != nil {
		return 0, fmt.Errorf("delete rows of %s: %w", table, err)
	}
	return result.RowsAffected()
}
