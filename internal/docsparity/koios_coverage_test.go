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

	doc := readRepoFile(t, root, koiosCoverageDoc)
	lines := strings.Split(doc, "\n")

	header := -1
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
			header = i
			break
		}
	}
	if header < 0 {
		t.Fatalf(
			"%s has no table headed %q; the Koios coverage contract is "+
				"unchecked until it is restored",
			koiosCoverageDoc,
			strings.Join(koiosCoverageHeader, " | "),
		)
	}

	exact := make(map[koiosFieldKey]koiosDocEntry)
	wildcard := make(map[koiosFieldKey]koiosDocEntry)
	for i := header + 2; i < len(lines); i++ {
		if !strings.HasPrefix(strings.TrimSpace(lines[i]), "|") {
			break
		}
		cells := splitTableCells(lines[i])
		if len(cells) < 3 {
			t.Errorf(
				"%s: coverage row has %d cells, want at least 3",
				docLocation(koiosCoverageDoc, i+1),
				len(cells),
			)
			continue
		}
		endpoint := unquote(cells[0])
		class := unquote(cells[1])
		for _, field := range strings.Split(cells[2], ",") {
			field = unquote(field)
			if field == "" {
				continue
			}
			key := koiosFieldKey{endpoint: endpoint, field: field}
			entry := koiosDocEntry{class: class, line: i + 1}
			if strings.HasSuffix(field, "*") {
				wildcard[key] = entry
				continue
			}
			exact[key] = entry
		}
	}
	if len(exact) == 0 {
		t.Fatalf(
			"%s: the coverage table has no field rows",
			docLocation(koiosCoverageDoc, header+1),
		)
	}
	return exact, wildcard
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

// koiosWildcardFor returns the wildcard row covering key, if any.
func koiosWildcardFor(
	wildcard map[koiosFieldKey]koiosDocEntry,
	key koiosFieldKey,
) (koiosFieldKey, koiosDocEntry, bool) {
	for _, candidate := range koiosSortedKeys(wildcard) {
		if candidate.endpoint != key.endpoint {
			continue
		}
		prefix := strings.TrimSuffix(candidate.field, "*")
		if prefix != "" && strings.HasPrefix(key.field, prefix) {
			return candidate, wildcard[candidate], true
		}
	}
	return koiosFieldKey{}, koiosDocEntry{}, false
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
