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

package architecture_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// alonzoWordAllowOmittedMarker exempts one database.Config literal from
// TestDatabaseConfigSuppliesAlonzoLovelacePerUtxoWord. A comment bearing it
// inside the literal, or in the alonzoWordMarkerLookback lines directly
// above it, documents why that open can never meet a legacy database -- a
// harness that creates the database it opens, say -- rather than the
// omission this test exists to catch.
const alonzoWordAllowOmittedMarker = "alonzopparams:word-not-required"

// alonzoWordField is the database.Config field a legacy database's in-place
// repair reads (database/alonzo_pparams_unit.go).
const alonzoWordField = "AlonzoLovelacePerUtxoWord"

// alonzoWordMarkerLookback is how far above a literal the exemption marker
// may sit. gofmt moves a comment written inside a one-line literal out above
// it, so requiring the marker strictly within the braces would make the
// exemption unwritable for exactly the shortest call sites.
const alonzoWordMarkerLookback = 5

// TestDatabaseConfigSuppliesAlonzoLovelacePerUtxoWord fails for any
// production database.Config literal that omits AlonzoLovelacePerUtxoWord.
//
// The field is not a tuning knob whose zero value is a sensible default: a
// database carrying the pre-gouroboros-v0.205.7 per-byte value in its Alonzo
// protocol-parameter rows repairs in place only when the open supplies it,
// and aborts with a resync-from-genesis instruction when it does not. Every
// construction site therefore has to carry it, and a runtime assertion at any
// one of them proves nothing about the other six -- the defect this guards
// against is precisely a new or edited site that resolves the word nowhere.
func TestDatabaseConfigSuppliesAlonzoLovelacePerUtxoWord(t *testing.T) {
	t.Parallel()

	root := findRepoRoot(t)
	fset := token.NewFileSet()
	var missing []string
	err := filepath.WalkDir(
		root,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				name := d.Name()
				if name == ".git" || name == ".worktrees" || name == "vendor" ||
					name == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") ||
				strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
			if err != nil {
				// A file this module does not build (a foreign build tag's
				// syntax, say) cannot hold a call site this test governs.
				return nil //nolint:nilerr
			}
			lines := strings.Split(string(src), "\n")
			ast.Inspect(file, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok || !isDatabaseConfigType(lit.Type) {
					return true
				}
				if litHasField(lit, alonzoWordField) {
					return true
				}
				start := fset.Position(lit.Lbrace).Line
				end := fset.Position(lit.Rbrace).Line
				if spanContains(
					lines,
					start-alonzoWordMarkerLookback,
					end,
					alonzoWordAllowOmittedMarker,
				) {
					return true
				}
				rel, relErr := filepath.Rel(root, path)
				if relErr != nil {
					rel = path
				}
				missing = append(missing, rel+":"+strconv.Itoa(start))
				return true
			})
			return nil
		},
	)
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(missing) > 0 {
		t.Fatalf(
			"database.Config literal(s) omit %s, so a legacy Alonzo "+
				"protocol-parameter row opened through them cannot be "+
				"repaired in place and demands a resync: %s",
			alonzoWordField,
			strings.Join(missing, ", "),
		)
	}
}

// isDatabaseConfigType reports whether a composite literal's type is
// database.Config or *database.Config.
func isDatabaseConfigType(expr ast.Expr) bool {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Config" {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	return ok && ident.Name == "database"
}

func litHasField(lit *ast.CompositeLit, field string) bool {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); ok && key.Name == field {
			return true
		}
	}
	return false
}

func spanContains(lines []string, start, end int, marker string) bool {
	for i := start - 1; i < end && i < len(lines); i++ {
		if i >= 0 && strings.Contains(lines[i], marker) {
			return true
		}
	}
	return false
}
