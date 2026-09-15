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

package docsparity_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
)

// koiosCoverageDoc is the document carrying the Koios coverage table, and
// koiosCoverageHeader is the header row that identifies it. Renaming either
// fails this check rather than skipping it, so the table cannot be moved out
// from under the rule.
const koiosCoverageDoc = "ARCHITECTURE.md"

var koiosCoverageHeader = []string{
	"Koios endpoint",
	"Classification",
	"Fields",
	"Dingo mapping / reason",
}

// koiosFieldKey identifies one row of the coverage contract. The classification
// of a (endpoint, field) pair is what a reader acts on: an exact-match or
// derived-match field is covered by a PASS, and the other two classes are not.
type koiosFieldKey struct {
	endpoint string
	field    string
}

// koiosDocEntry is one documented classification and where it is written.
type koiosDocEntry struct {
	class string
	line  int
}

// koiosCoverageDocEntries returns the classification the coverage table states
// for each (endpoint, field) pair, keeping wildcard entries separate.
//
// A field written with a trailing `*` stands for a group the table
// deliberately abbreviates, such as the Conway `pvt_*` voting thresholds. It
// covers every matrix field of that endpoint sharing the prefix, and a
// wildcard matching nothing is itself drift.
func koiosCoverageDocEntries(
	t *testing.T,
	root string,
) (map[koiosFieldKey]koiosDocEntry, map[koiosFieldKey]koiosDocEntry) {
	t.Helper()

	table, err := parseKoiosCoverageTable(
		readRepoFile(t, root, koiosCoverageDoc),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, problem := range table.problems {
		t.Error(problem)
	}
	return table.exact, table.wildcard
}

// koiosCoverageTable is one parsed coverage table: the classifications it
// states, and the row-level faults found while reading it.
//
// Parsing is separated from reporting so the parser's own rules can be checked
// against a table written to break them, rather than only against whatever
// ARCHITECTURE.md happens to contain today.
type koiosCoverageTable struct {
	exact    map[koiosFieldKey]koiosDocEntry
	wildcard map[koiosFieldKey]koiosDocEntry
	problems []string
}

// parseKoiosCoverageTable reads the coverage table out of doc.
//
// Every table carrying the coverage header is read, not just the first. A
// second table under the same header states the coverage contract just as the
// first does, so reading only one leaves it unchecked -- and a contradiction
// split across two tables would escape the duplicate-row rule that rejects the
// same contradiction inside one.
//
// The error covers the two conditions that leave nothing to check at all: no
// such table, or no field rows in any of them. problems holds the per-row
// faults, which are each worth reporting without abandoning the rest of the
// table.
func parseKoiosCoverageTable(doc string) (koiosCoverageTable, error) {
	lines := strings.Split(doc, "\n")

	var headers []int
	for i, line := range lines {
		if !strings.Contains(line, "|") {
			continue
		}
		cells := splitTableCells(line)
		if len(cells) != len(koiosCoverageHeader) {
			continue
		}
		matched := true
		for j, want := range koiosCoverageHeader {
			if cells[j] != want {
				matched = false
				break
			}
		}
		if matched {
			headers = append(headers, i)
		}
	}
	if len(headers) == 0 {
		return koiosCoverageTable{}, fmt.Errorf(
			"%s has no table headed %q; the Koios coverage contract is "+
				"unchecked until it is restored",
			koiosCoverageDoc,
			strings.Join(koiosCoverageHeader, " | "),
		)
	}

	table := koiosCoverageTable{
		exact:    make(map[koiosFieldKey]koiosDocEntry),
		wildcard: make(map[koiosFieldKey]koiosDocEntry),
	}
	for _, header := range headers {
		for i := header + 2; i < len(lines); i++ {
			if !strings.HasPrefix(strings.TrimSpace(lines[i]), "|") {
				break
			}
			cells := splitTableCells(lines[i])
			if len(cells) < 3 {
				table.problems = append(table.problems, fmt.Sprintf(
					"%s: coverage row has %d cells, want at least 3",
					docLocation(koiosCoverageDoc, i+1),
					len(cells),
				))
				continue
			}
			endpoint := unquote(cells[0])
			class := unquote(cells[1])
			fields := 0
			for _, field := range strings.Split(cells[2], ",") {
				field = unquote(field)
				if field == "" {
					continue
				}
				fields++
				key := koiosFieldKey{endpoint: endpoint, field: field}
				entry := koiosDocEntry{class: class, line: i + 1}
				target := table.exact
				if strings.HasSuffix(field, "*") {
					target = table.wildcard
				}
				// Assigning over an existing key would drop the earlier row. A
				// wrong classification followed by a correct duplicate would
				// then leave only the correct one to compare, so the table
				// would pass while still telling a reader two different things
				// about the same field.
				if previous, duplicate := target[key]; duplicate {
					table.problems = append(table.problems, fmt.Sprintf(
						"%s: duplicate coverage row for %s %s, already "+
							"documented at %s",
						docLocation(koiosCoverageDoc, entry.line),
						key.endpoint,
						key.field,
						docLocation(koiosCoverageDoc, previous.line),
					))
					continue
				}
				target[key] = entry
			}
			// A row whose Fields cell names nothing records no classification,
			// so every later check skips it: it is neither compared against
			// the matrix nor rejected for an unknown classification. Dropping
			// it silently makes an incomplete row read as a documented one.
			if fields == 0 {
				table.problems = append(table.problems, fmt.Sprintf(
					"%s: coverage row for %s names no field, so its "+
						"classification %q is never checked",
					docLocation(koiosCoverageDoc, i+1),
					endpoint,
					class,
				))
			}
		}
	}
	if len(table.exact) == 0 {
		return koiosCoverageTable{}, fmt.Errorf(
			"%s: the coverage table has no field rows",
			docLocation(koiosCoverageDoc, headers[0]+1),
		)
	}
	return table, nil
}

// koiosCoverageClasses returns every classification the code defines, so an
// unrecognised value in the table is reported as such rather than as a
// mismatch against every field that carries it.
func koiosCoverageClasses() map[string]bool {
	return map[string]bool{
		string(koiosparity.CoverageExactMatch):                true,
		string(koiosparity.CoverageDerivedMatch):              true,
		string(koiosparity.CoverageIntentionallyIncomparable): true,
		string(koiosparity.CoverageUnsupported):               true,
	}
}

// TestArchitectureDocumentsKoiosCoverageMatrix checks the Koios coverage table
// against koiosparity.KoiosCoverageMatrix, which is the contract the parity
// checker actually applies.
//
// The classification is the load-bearing part: a PASS covers only the
// exact-match and derived-match fields, so a table that classifies a field
// differently from the code tells an operator that a field is checked when it
// is not, or the reverse. This compares the endpoint, field and classification
// in both directions and leaves the mapping/reason prose alone, so rewording a
// reason does not fail the check.
func TestArchitectureDocumentsKoiosCoverageMatrix(t *testing.T) {
	root := repoRoot(t)
	exact, wildcard := koiosCoverageDocEntries(t, root)
	classes := koiosCoverageClasses()

	for key, entry := range exact {
		if !classes[entry.class] {
			t.Errorf(
				"%s: %s %s has unknown classification %q",
				docLocation(koiosCoverageDoc, entry.line),
				key.endpoint,
				key.field,
				entry.class,
			)
		}
	}
	for key, entry := range wildcard {
		if !classes[entry.class] {
			t.Errorf(
				"%s: %s %s has unknown classification %q",
				docLocation(koiosCoverageDoc, entry.line),
				key.endpoint,
				key.field,
				entry.class,
			)
		}
	}

	matrix := koiosparity.KoiosCoverageMatrix()
	if len(matrix) == 0 {
		t.Fatal("koiosparity.KoiosCoverageMatrix is empty")
	}

	usedWildcard := make(map[koiosFieldKey]bool)
	for _, field := range matrix {
		key := koiosFieldKey{endpoint: field.Endpoint, field: field.Field}
		class := string(field.Class)
		if entry, ok := exact[key]; ok {
			if entry.class != class {
				t.Errorf(
					"%s: %s %s is documented as %s but "+
						"koiosCoverageMatrix classifies it %s",
					docLocation(koiosCoverageDoc, entry.line),
					key.endpoint,
					key.field,
					entry.class,
					class,
				)
			}
			continue
		}
		matchKey, entry, ok := koiosWildcardFor(wildcard, key)
		if !ok {
			t.Errorf(
				"%s documents no row for %s %s (%s); every field in "+
					"koiosCoverageMatrix belongs in the coverage table",
				koiosCoverageDoc,
				key.endpoint,
				key.field,
				class,
			)
			continue
		}
		usedWildcard[matchKey] = true
		if entry.class != class {
			t.Errorf(
				"%s: %s %s covers %s, which koiosCoverageMatrix "+
					"classifies %s and not %s",
				docLocation(koiosCoverageDoc, entry.line),
				matchKey.endpoint,
				matchKey.field,
				key.field,
				class,
				entry.class,
			)
		}
	}

	documented := make(map[koiosFieldKey]bool, len(matrix))
	for _, field := range matrix {
		documented[koiosFieldKey{
			endpoint: field.Endpoint,
			field:    field.Field,
		}] = true
	}
	for _, key := range koiosSortedKeys(exact) {
		if documented[key] {
			continue
		}
		t.Errorf(
			"%s: %s %s is documented but koiosCoverageMatrix has no such "+
				"field; the table describes coverage the checker does not "+
				"apply",
			docLocation(koiosCoverageDoc, exact[key].line),
			key.endpoint,
			key.field,
		)
	}
	for _, key := range koiosSortedKeys(wildcard) {
		if usedWildcard[key] {
			continue
		}
		t.Errorf(
			"%s: %s %s matches no field in koiosCoverageMatrix",
			docLocation(koiosCoverageDoc, wildcard[key].line),
			key.endpoint,
			key.field,
		)
	}
}

// TestKoiosCoverageTableRejectsDuplicateRows pins the duplicate check in
// parseKoiosCoverageTable.
//
// The classifications are read into a map keyed by (endpoint, field), so a
// second row for a key would otherwise assign over the first. A table that
// states a wrong classification and then contradicts it with a correct
// duplicate would be read as stating only the correct one, and would pass
// while still telling a reader two different things about the same field.
//
// Both the exact and the wildcard map are checked, because they are separate
// maps and a check added to one is not a check on the other.
func TestKoiosCoverageTableRejectsDuplicateRows(t *testing.T) {
	t.Parallel()

	doc := strings.Join([]string{
		"| " + strings.Join(koiosCoverageHeader, " | ") + " |",
		"| --- | --- | --- | --- |",
		"| `/tip` | exact-match | `abs_slot` | mapped |",
		"| `/tip` | unsupported | `abs_slot` | contradicts the row above |",
		"| `/epoch_params` | exact-match | `pvt_*` | mapped |",
		"| `/epoch_params` | unsupported | `pvt_*` | contradicts it |",
	}, "\n")

	table, err := parseKoiosCoverageTable(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(table.problems) != 2 {
		t.Fatalf(
			"want both duplicate rows reported, got %d problem(s): %v",
			len(table.problems),
			table.problems,
		)
	}
	for _, want := range []string{
		"duplicate coverage row for /tip abs_slot",
		"duplicate coverage row for /epoch_params pvt_*",
	} {
		found := false
		for _, problem := range table.problems {
			if strings.Contains(problem, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no problem reports %q; got %v", want, table.problems)
		}
	}

	// The first row of each pair is the one kept. Reporting the duplicate is
	// the whole point, so which row survives only has to be deterministic.
	exactKey := koiosFieldKey{endpoint: "/tip", field: "abs_slot"}
	if got := table.exact[exactKey].class; got != "exact-match" {
		t.Errorf("exact row for %v kept class %q, want the first row", exactKey, got)
	}
	wildcardKey := koiosFieldKey{endpoint: "/epoch_params", field: "pvt_*"}
	if got := table.wildcard[wildcardKey].class; got != "exact-match" {
		t.Errorf("wildcard row for %v kept class %q, want the first row", wildcardKey, got)
	}
}

// TestKoiosCoverageTableRejectsRowWithNoField pins the empty-Fields check in
// parseKoiosCoverageTable.
//
// Every later check keys off a (endpoint, field) pair, so a row whose Fields
// cell names nothing contributes no pair and is skipped by all of them: it is
// neither compared against koiosCoverageMatrix nor rejected for an unknown
// classification. Without this, a row carrying an undefined classification for
// an endpoint the matrix has never heard of reads as documented coverage and
// passes.
func TestKoiosCoverageTableRejectsRowWithNoField(t *testing.T) {
	t.Parallel()

	doc := strings.Join([]string{
		"| " + strings.Join(koiosCoverageHeader, " | ") + " |",
		"| --- | --- | --- | --- |",
		"| `/tip` | exact-match | `abs_slot` | mapped |",
		"| `/account_info` | bogus-class |  | fields not filled in |",
	}, "\n")

	table, err := parseKoiosCoverageTable(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	const want = "/account_info names no field"
	found := false
	for _, problem := range table.problems {
		if strings.Contains(problem, want) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("no problem reports %q; got %v", want, table.problems)
	}
	// The rest of the table still parses, so one incomplete row does not cost
	// the checks on every other row.
	if _, ok := table.exact[koiosFieldKey{
		endpoint: "/tip",
		field:    "abs_slot",
	}]; !ok {
		t.Error("the complete row was dropped alongside the incomplete one")
	}
}

// TestKoiosCoverageTableReadsEverySuchTable pins that a second table under the
// coverage header is read too.
//
// Reading only the first leaves any later one unchecked, so a contradiction
// split across two tables would pass the duplicate-row rule that rejects the
// same contradiction inside one, and a row naming an endpoint the matrix lacks
// would never be compared.
func TestKoiosCoverageTableReadsEverySuchTable(t *testing.T) {
	t.Parallel()

	doc := strings.Join([]string{
		"| " + strings.Join(koiosCoverageHeader, " | ") + " |",
		"| --- | --- | --- | --- |",
		"| `/tip` | exact-match | `abs_slot` | mapped |",
		"",
		"Prose between the two tables.",
		"",
		"| " + strings.Join(koiosCoverageHeader, " | ") + " |",
		"| --- | --- | --- | --- |",
		"| `/tip` | unsupported | `abs_slot` | contradicts the first table |",
		"| `/nope` | exact-match | `bogus` | an endpoint the matrix lacks |",
	}, "\n")

	table, err := parseKoiosCoverageTable(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	const wantDuplicate = "duplicate coverage row for /tip abs_slot"
	found := false
	for _, problem := range table.problems {
		if strings.Contains(problem, wantDuplicate) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf(
			"no problem reports %q; got %v",
			wantDuplicate,
			table.problems,
		)
	}
	// The second table's other row has to reach the maps, or the comparison
	// against koiosCoverageMatrix never sees it either.
	if _, ok := table.exact[koiosFieldKey{
		endpoint: "/nope",
		field:    "bogus",
	}]; !ok {
		t.Error("the second table's rows were not read")
	}
}

// TestKoiosWildcardForPrefersLongestPrefix pins the wildcard selection.
//
// One endpoint may carry nested wildcards, `pvt_*` alongside `pvt_motion_*`.
// Taking the first in sort order compares a pvt_motion_ field against the
// broader row's classification and then reports the narrower row as matching
// nothing at all.
func TestKoiosWildcardForPrefersLongestPrefix(t *testing.T) {
	t.Parallel()

	wildcard := map[koiosFieldKey]koiosDocEntry{
		{endpoint: "/epoch_params", field: "pvt_*"}: {
			class: "unsupported",
			line:  1,
		},
		{endpoint: "/epoch_params", field: "pvt_motion_*"}: {
			class: "exact-match",
			line:  2,
		},
	}

	key := koiosFieldKey{
		endpoint: "/epoch_params",
		field:    "pvt_motion_no_confidence",
	}
	match, entry, ok := koiosWildcardFor(wildcard, key)
	if !ok {
		t.Fatalf("%v matched no wildcard row", key)
	}
	if match.field != "pvt_motion_*" {
		t.Errorf("matched %q, want the longest matching prefix %q",
			match.field, "pvt_motion_*")
	}
	if entry.class != "exact-match" {
		t.Errorf("matched class %q, want %q", entry.class, "exact-match")
	}

	// A field only the broader row covers still resolves to it.
	broad := koiosFieldKey{endpoint: "/epoch_params", field: "pvt_committee"}
	match, _, ok = koiosWildcardFor(wildcard, broad)
	if !ok || match.field != "pvt_*" {
		t.Errorf("%v matched %q (ok=%v), want %q",
			broad, match.field, ok, "pvt_*")
	}
}

// koiosWildcardFor returns the wildcard row covering key, if any.
func koiosWildcardFor(
	wildcard map[koiosFieldKey]koiosDocEntry,
	key koiosFieldKey,
) (koiosFieldKey, koiosDocEntry, bool) {
	var (
		best  koiosFieldKey
		found bool
	)
	for _, candidate := range koiosSortedKeys(wildcard) {
		if candidate.endpoint != key.endpoint {
			continue
		}
		prefix := strings.TrimSuffix(candidate.field, "*")
		if prefix == "" || !strings.HasPrefix(key.field, prefix) {
			continue
		}
		// One endpoint may carry nested wildcards, `pvt_*` alongside
		// `pvt_motion_*`. Taking the first in sort order compares a
		// pvt_motion_ field against the broader row's classification and then
		// reports the narrower row as matching nothing. The longest matching
		// prefix is the row a reader would take as governing the field.
		if found && len(best.field) >= len(candidate.field) {
			continue
		}
		best, found = candidate, true
	}
	if !found {
		return koiosFieldKey{}, koiosDocEntry{}, false
	}
	return best, wildcard[best], true
}

// koiosSortedKeys orders keys so failures are reported deterministically.
func koiosSortedKeys(m map[koiosFieldKey]koiosDocEntry) []koiosFieldKey {
	keys := make([]koiosFieldKey, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].endpoint != keys[j].endpoint {
			return keys[i].endpoint < keys[j].endpoint
		}
		return keys[i].field < keys[j].field
	})
	return keys
}
