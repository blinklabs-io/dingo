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

package statedrain

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// ErrDrainedRead is wrapped by the error a TripGuard returns for a statement
// that reads a drained family's table.
var ErrDrainedRead = errors.New("read of a drained ledger-state table")

// ErrDrainedWrite is wrapped by the error a TripGuard returns for a statement
// that inserts into, updates or deletes from a drained family's table.
var ErrDrainedWrite = errors.New("write to a drained ledger-state table")

// tableRef matches the keyword that introduces a table and the table's name,
// with an optional schema qualifier.
var tableRef = regexp.MustCompile(
	`(?i)\b(from|join|into|update|truncate(?:\s+table)?)\s+(?:only\s+)?` +
		`(?:["\x60\[]?[a-z_][a-z0-9_]*["\x60\]]?\s*\.\s*)?` +
		`["\x60\[]?([a-z_][a-z0-9_]*)`,
)

// Trip records one refused statement.
type Trip struct {
	Family nodesettings.Family
	Table  string
	Query  string
	// Write is true when the statement modifies the table.
	Write bool
}

// TripGuard is a metadata.StatementGuard that refuses any statement touching
// a table of a drained family, a read or a write. It is a debug and test aid
// that catches a path the set did not take over: every refusal is recorded,
// so a caller that swallows the error is still seen. The one statement shape
// it lets through is the drain's own chunked delete, which carries
// nodesettings.DrainStatementTag.
type TripGuard struct {
	mu     sync.Mutex
	tables map[string]nodesettings.Family
	trips  []Trip
}

// NewTripGuard returns a guard with no drained family.
func NewTripGuard() *TripGuard {
	return &TripGuard{tables: map[string]nodesettings.Family{}}
}

// Arm marks tables as drained for family.
func (g *TripGuard) Arm(family nodesettings.Family, tables []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, table := range tables {
		g.tables[strings.ToLower(table)] = family
	}
}

// CheckStatement refuses a statement that reads or writes an armed table.
func (g *TripGuard) CheckStatement(query string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.tables) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(query)
	if strings.HasPrefix(trimmed, nodesettings.DrainStatementTag) {
		return nil
	}
	isDelete := strings.HasPrefix(strings.ToLower(trimmed), "delete")
	for i, ref := range tableRef.FindAllStringSubmatch(query, -1) {
		family, ok := g.tables[strings.ToLower(ref[2])]
		if !ok {
			continue
		}
		keyword := strings.ToLower(ref[1])
		write := keyword != "from" && keyword != "join" ||
			isDelete && i == 0
		g.trips = append(g.trips, Trip{family, ref[2], query, write})
		sentinel := ErrDrainedRead
		if write {
			sentinel = ErrDrainedWrite
		}
		return fmt.Errorf(
			"%w: family %s table %s", sentinel, family, ref[2],
		)
	}
	return nil
}

// Trips returns every statement refused so far.
func (g *TripGuard) Trips() []Trip {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]Trip(nil), g.trips...)
}

// DrainedTableIn reports the first table of a family in mask that query
// names anywhere, as a whole identifier in any case, quoted or not. Unlike
// TripGuard it does not parse the statement's shape, so it also catches a
// table in a comma join or behind an alias, at the price of refusing a string
// literal that spells a table name.
func DrainedTableIn(
	mask nodesettings.FamilyMask,
	query string,
) (nodesettings.Family, string, bool) {
	for _, family := range mask.Families() {
		for _, table := range TableNames(family) {
			ref := regexp.MustCompile(
				`(?i)(^|[^a-z0-9_])` + regexp.QuoteMeta(table) +
					`($|[^a-z0-9_])`,
			)
			if ref.MatchString(query) {
				return family, table, true
			}
		}
	}
	return 0, "", false
}
