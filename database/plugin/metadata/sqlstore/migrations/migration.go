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

// Package migrations implements offline, forward-only metadata upgrades.
package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
)

const DefaultBatchSize = 1000

// SQL contains idempotent DDL statements for one migration phase.
type SQL struct {
	Expand   []string
	Contract []string
}

// Batch is one resumable backfill transaction.
type Batch struct {
	Tx     *sql.Tx
	Cursor string
	Limit  int
	// Rebind converts ? placeholders to the dialect's own form. It is never
	// nil; the runner substitutes an identity function for dialects that take
	// ? directly.
	Rebind func(string) string
}

// BatchResult describes the durable checkpoint after a backfill batch.
type BatchResult struct {
	Cursor string
	Rows   int64
	Done   bool
}

// Backfill executes at most Batch.Limit rows. Runner commits its data changes
// and returned cursor in the same transaction.
type Backfill func(context.Context, Batch) (BatchResult, error)

// Migration is immutable after release. BackfillRevision must change whenever
// the Go backfill behavior changes.
type Migration struct {
	Version          int
	Name             string
	BackfillRevision string
	SQL              map[string]SQL
	Backfill         Backfill
	BatchSize        int
}

func (m Migration) checksum() string {
	hash := sha256.New()
	write := func(value string) {
		_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(value))
	}
	write(strconv.Itoa(m.Version))
	write(m.Name)
	write(m.BackfillRevision)
	dialects := make([]string, 0, len(m.SQL))
	for dialect := range m.SQL {
		dialects = append(dialects, dialect)
	}
	sort.Strings(dialects)
	for _, dialect := range dialects {
		write(dialect)
		// Include phase boundaries and statement counts.  Without these
		// markers, moving a statement from Expand to Contract while keeping
		// the concatenated SQL unchanged would not change the checksum.  That
		// is unsafe for databases resuming an interrupted migration because
		// the statement would then execute at a different lifecycle phase.
		write("expand")
		write(strconv.Itoa(len(m.SQL[dialect].Expand)))
		for _, statement := range m.SQL[dialect].Expand {
			write(statement)
		}
		write("contract")
		write(strconv.Itoa(len(m.SQL[dialect].Contract)))
		for _, statement := range m.SQL[dialect].Contract {
			write(statement)
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func validateRegistry(registry []Migration, dialect string) error {
	if len(registry) == 0 {
		return ErrEmptyRegistry
	}
	seenNames := make(map[string]struct{}, len(registry))
	for idx, migration := range registry {
		expected := idx + 1
		if migration.Version != expected {
			return fmt.Errorf(
				"%w: expected version %d, found %d",
				ErrInvalidRegistry,
				expected,
				migration.Version,
			)
		}
		if migration.Name == "" {
			return fmt.Errorf(
				"%w: version %d has an empty name",
				ErrInvalidRegistry,
				migration.Version,
			)
		}
		if migration.BackfillRevision == "" {
			return fmt.Errorf(
				"%w: version %d has an empty backfill revision",
				ErrInvalidRegistry,
				migration.Version,
			)
		}
		if _, exists := seenNames[migration.Name]; exists {
			return fmt.Errorf(
				"%w: duplicate name %q",
				ErrInvalidRegistry,
				migration.Name,
			)
		}
		seenNames[migration.Name] = struct{}{}
		if _, ok := migration.SQL[dialect]; !ok {
			return fmt.Errorf(
				"%w: version %d has no %s SQL",
				ErrInvalidRegistry,
				migration.Version,
				dialect,
			)
		}
		if migration.BatchSize < 0 {
			return fmt.Errorf(
				"%w: version %d has a negative batch size",
				ErrInvalidRegistry,
				migration.Version,
			)
		}
	}
	return nil
}
