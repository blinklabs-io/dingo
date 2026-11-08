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
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
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

// TestDevnetFilesStayLinuxOnly keeps the platform boundary directory-wide.
// Go build constraints are file-scoped, so a newly added untagged file would
// otherwise silently put part of the Docker-backed harness into macOS and
// Windows `go test ./...` runs again.
func TestDevnetFilesStayLinuxOnly(t *testing.T) {
	root := filepath.Join(findRepoRoot(t), "internal", "test", "devnet")
	err := filepath.WalkDir(
		root,
		func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			expr, err := parseGoBuildConstraint(content)
			if err != nil || !requiresBuildTag(expr, "linux") {
				rel, relErr := filepath.Rel(root, path)
				if relErr != nil {
					return relErr
				}
				t.Errorf(
					"%s must have a build constraint that requires linux",
					filepath.ToSlash(rel),
				)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("check DevNet platform constraints: %v", err)
	}
}

var (
	errNoGoBuildConstraint = errors.New("no //go:build constraint")
	errMultipleGoBuild     = errors.New("multiple //go:build constraints")
)

// parseGoBuildConstraint finds the //go:build directive in the portion of a
// Go source file where the go command recognizes it. Leading line and block
// comments may contain a license header; directives inside a block comment or
// after the first non-comment token do not apply to the file.
func parseGoBuildConstraint(content []byte) (constraint.Expr, error) {
	var goBuild string
	p := content
	inBlockComment := false

lines:
	for len(p) > 0 {
		line := p
		if i := bytes.IndexByte(line, '\n'); i >= 0 {
			line, p = line[:i], p[i+1:]
		} else {
			p = nil
		}
		line = bytes.TrimSpace(line)

		if !inBlockComment && constraint.IsGoBuild(string(line)) {
			if goBuild != "" {
				return nil, errMultipleGoBuild
			}
			goBuild = string(line)
		}

	comments:
		for len(line) > 0 {
			if inBlockComment {
				if i := bytes.Index(line, []byte("*/")); i >= 0 {
					inBlockComment = false
					line = bytes.TrimSpace(line[i+2:])
					continue comments
				}
				continue lines
			}
			if bytes.HasPrefix(line, []byte("//")) {
				continue lines
			}
			if bytes.HasPrefix(line, []byte("/*")) {
				inBlockComment = true
				line = bytes.TrimSpace(line[2:])
				continue comments
			}
			break lines
		}
	}

	if goBuild == "" {
		return nil, errNoGoBuildConstraint
	}
	return constraint.Parse(goBuild)
}

// requiresBuildTag returns true only when the expression structurally proves
// that tag must be true. It deliberately rejects expressions it cannot prove,
// keeping additions such as "linux || windows" out of the DevNet tree.
func requiresBuildTag(expr constraint.Expr, tag string) bool {
	switch expr := expr.(type) {
	case *constraint.TagExpr:
		return expr.Tag == tag
	case *constraint.NotExpr:
		return false
	case *constraint.AndExpr:
		return requiresBuildTag(expr.X, tag) ||
			requiresBuildTag(expr.Y, tag)
	case *constraint.OrExpr:
		return requiresBuildTag(expr.X, tag) &&
			requiresBuildTag(expr.Y, tag)
	default:
		return false
	}
}

const modulePath = "github.com/blinklabs-io/dingo"

type importBoundaryRule struct {
	from      string
	forbidden []string
	reason    string
	// testImportExemptions maps a repository-relative _test.go file to the
	// forbidden packages it may import. Keep each entry to a test that must
	// compose both sides of the boundary, and name the reason beside it.
	testImportExemptions map[string][]string
}

// importBoundaryRules encode reviewed package directions for critical domains.
// When an architecture review approves a new dependency, update this list in
// the same change as ARCHITECTURE.md and keep the reason field explicit.
var importBoundaryRules = []importBoundaryRule{
	{
		from: "ledger",
		forbidden: []string{
			"connmanager",
			"peergov",
			"mempool",
		},
		reason: "ledger owns validation and state, while node wiring translates " +
			"neutral events/callbacks into networking or mempool actions",
		testImportExemptions: map[string][]string{
			// Drives a Dijkstra collateral-return transaction through both
			// mempool admission and ledger block application.
			"ledger/dijkstra_collateral_return_production_test.go": {
				"mempool",
			},
			// Drives a ParameterChange proposal through mempool admission,
			// live block application, and replay to compare their decisions.
			"ledger/parameter_change_decisions_test.go": {
				"mempool",
			},
			// Validates a pending Dijkstra batch's account effects through
			// mempool admission against a real ledger state.
			"ledger/dijkstra_batch_mempool_test.go": {
				"mempool",
			},
			// Revalidates a mempool against a real ledger state after a
			// block confirms a pending parent.
			"ledger/mempool_revalidation_test.go": {
				"mempool",
			},
			// Drives Dijkstra batches with child-level defects through mempool
			// admission and every block path.
			"ledger/dijkstra_child_production_test.go": {
				"mempool",
			},
			// Submits Dijkstra transactions through mempool admission, then
			// through block application, replay and rollback.
			"ledger/dijkstra_production_paths_fixture_test.go": {
				"mempool",
			},
			// Requires the same minimum-fee decision from mempool admission
			// as from block validation and replay.
			"ledger/fee_deposit_production_test.go": {
				"mempool",
			},
			// Drives a deferred header failure through ledger recovery, the
			// ouroboros resync handler, and the peer governor.
			"ledger/deferred_header_peer_recovery_e2e_test.go": {
				"connmanager",
				"mempool",
				"peergov",
			},
		},
	},
	{
		from:      "chainselection",
		forbidden: []string{"peergov"},
		reason: "chain selection should emit neutral chain-switch decisions " +
			"without depending on peer governance policy",
	},
	{
		from:      "chainsync",
		forbidden: []string{"ledger"},
		reason: "chainsync should track protocol state without importing " +
			"concrete ledger state",
	},
	{
		from: "database",
		forbidden: []string{
			".",
			"ledger",
			"mempool",
			"dmq",
			"connmanager",
			"peergov",
			"ouroboros",
			"chainsync",
			"chainselection",
			"internal/node",
			"api",
		},
		reason: "database and storage plugins sit below ledger, mempool, " +
			"dmq, networking, node composition, and API packages",
	},
}

// TestImportBoundaries checks reviewed package dependency directions against
// every local Go import in the guarded package trees.
func TestImportBoundaries(t *testing.T) {
	repoRoot := findRepoRoot(t)
	var violations []string
	var errs []error
	violationsCh := make(chan string)
	errsCh := make(chan error)

	var wg sync.WaitGroup
	for _, rule := range importBoundaryRules {
		wg.Go(func() {

			ruleViolations, err := importBoundaryViolations(repoRoot, rule)
			if err != nil {
				errsCh <- err
				return
			}
			for _, violation := range ruleViolations {
				violationsCh <- violation
			}
		})
	}

	go func() {
		wg.Wait()
		close(violationsCh)
		close(errsCh)
	}()

	for violationsCh != nil || errsCh != nil {
		select {
		case violation, ok := <-violationsCh:
			if !ok {
				violationsCh = nil
				continue
			}
			violations = append(violations, violation)
		case err, ok := <-errsCh:
			if !ok {
				errsCh = nil
				continue
			}
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		errorMessages := make([]string, 0, len(errs))
		for _, err := range errs {
			errorMessages = append(errorMessages, err.Error())
		}
		slices.Sort(errorMessages)
		t.Fatalf(
			"check import boundaries:\n%s",
			strings.Join(errorMessages, "\n"),
		)
	}

	if len(violations) > 0 {
		slices.Sort(violations)
		t.Fatalf(
			"forbidden local imports found:\n%s",
			strings.Join(violations, "\n"),
		)
	}
}

// importBoundaryViolations checks one boundary rule, parsing files in parallel
// while bounding concurrent parser work to the configured Go CPU parallelism.
func importBoundaryViolations(
	repoRoot string,
	rule importBoundaryRule,
) ([]string, error) {
	files, err := goFilesBelow(repoRoot, rule.from)
	if err != nil {
		return nil, err
	}

	var violations []string
	var errs []error
	var mu sync.Mutex
	var wg sync.WaitGroup
	jobs := make(chan string)

	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {

			for file := range jobs {
				fileViolations, err := importBoundaryViolationsForFile(
					repoRoot,
					rule,
					file,
				)
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				} else {
					violations = append(violations, fileViolations...)
				}
				mu.Unlock()
			}
		})
	}
	for _, file := range files {
		jobs <- file
	}
	close(jobs)
	wg.Wait()

	if len(errs) > 0 {
		errorMessages := make([]string, 0, len(errs))
		for _, err := range errs {
			errorMessages = append(errorMessages, err.Error())
		}
		slices.Sort(errorMessages)
		return nil, fmt.Errorf("%s", strings.Join(errorMessages, "\n"))
	}
	return violations, nil
}

// importBoundaryViolationsForFile reports every forbidden import from one Go
// source file for the given boundary rule.
func importBoundaryViolationsForFile(
	repoRoot string,
	rule importBoundaryRule,
	file string,
) ([]string, error) {
	imports, err := importsForFile(file)
	if err != nil {
		return nil, err
	}
	relFile, err := relativePath(repoRoot, file)
	if err != nil {
		return nil, err
	}
	var exempt []string
	if strings.HasSuffix(relFile, "_test.go") {
		exempt = rule.testImportExemptions[relFile]
	}

	var violations []string
	for _, importPath := range imports {
		for _, forbidden := range rule.forbidden {
			if !isLocalPackageImport(importPath, forbidden) ||
				slices.Contains(exempt, forbidden) {
				continue
			}
			violations = append(
				violations,
				fmt.Sprintf(
					"%s imports %s: %s",
					relFile,
					importPath,
					rule.reason,
				),
			)
		}
	}
	return violations, nil
}

// findRepoRoot walks upward from the test working directory until it finds the
// module root.
func findRepoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repository root")
		}
		dir = parent
	}
}

// goFilesBelow returns all Go source files below a repository-relative
// directory, excluding fixture and hidden directories.
func goFilesBelow(repoRoot, dir string) ([]string, error) {
	root := filepath.Join(repoRoot, dir)
	var files []string
	err := filepath.WalkDir(
		root,
		func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if shouldSkipDir(entry.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(entry.Name(), ".go") {
				files = append(files, path)
			}
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}
	return files, nil
}

// shouldSkipDir reports whether a directory should be ignored by the boundary
// scan.
func shouldSkipDir(name string) bool {
	return name == "testdata" || strings.HasPrefix(name, ".")
}

// importsForFile parses a Go file in imports-only mode and returns its import
// paths without quote characters.
func importsForFile(file string) ([]string, error) {
	parsed, err := parser.ParseFile(
		token.NewFileSet(),
		file,
		nil,
		parser.ImportsOnly,
	)
	if err != nil {
		return nil, fmt.Errorf("parse imports for %s: %w", file, err)
	}

	imports := make([]string, 0, len(parsed.Imports))
	for _, importSpec := range parsed.Imports {
		imports = append(imports, strings.Trim(importSpec.Path.Value, `"`))
	}
	return imports, nil
}

// isLocalPackageImport reports whether an import path matches the forbidden
// local package itself or any of its subpackages.
func isLocalPackageImport(importPath, forbidden string) bool {
	if forbidden == "." {
		return importPath == modulePath
	}
	forbiddenPath := modulePath + "/" + forbidden
	return importPath == forbiddenPath ||
		strings.HasPrefix(importPath, forbiddenPath+"/")
}

// relativePath formats a source path relative to the repository root for stable
// test failure messages.
func relativePath(repoRoot, file string) (string, error) {
	rel, err := filepath.Rel(repoRoot, file)
	if err != nil {
		return "", fmt.Errorf("make %s relative to %s: %w", file, repoRoot, err)
	}
	return filepath.ToSlash(rel), nil
}
