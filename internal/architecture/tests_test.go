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
	"maps"
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

func TestRequiresBuildTag(t *testing.T) {
	tests := map[string]bool{
		"//go:build linux":                                       true,
		"//go:build linux && devnet":                             true,
		"//go:build devnet && linux":                             true,
		"//go:build (linux && devnet) || (linux && conformance)": true,
		"//go:build windows":                                     false,
		"//go:build devnet":                                      false,
		"//go:build !windows":                                    false,
		"//go:build linux || windows":                            false,
	}
	for line, want := range tests {
		t.Run(line, func(t *testing.T) {
			expr, err := constraint.Parse(line)
			if err != nil {
				t.Fatalf("parse constraint: %v", err)
			}
			if got := requiresBuildTag(expr, "linux"); got != want {
				t.Fatalf("requiresBuildTag() = %v, want %v", got, want)
			}
		})
	}
}

func TestParseGoBuildConstraint(t *testing.T) {
	tests := map[string]struct {
		content string
		want    string
		wantErr error
	}{
		"first line": {
			content: "//go:build linux\n\npackage devnet\n",
			want:    "linux",
		},
		"after line comment license": {
			content: "// Copyright 2026 Blink Labs Software\n" +
				"// Licensed under the Apache License, Version 2.0\n\n" +
				"//go:build linux && devnet\n\npackage devnet\n",
			want: "linux && devnet",
		},
		"after block comment license": {
			content: "/* Copyright 2026 Blink Labs Software */\n\n" +
				"//go:build linux && devnet\n\npackage devnet\n",
			want: "linux && devnet",
		},
		"inside block comment": {
			content: "/*\n//go:build linux\n*/\npackage devnet\n",
			wantErr: errNoGoBuildConstraint,
		},
		"after package clause": {
			content: "package devnet\n\n//go:build linux\n",
			wantErr: errNoGoBuildConstraint,
		},
		"multiple constraints": {
			content: "//go:build linux\n//go:build devnet\npackage devnet\n",
			wantErr: errMultipleGoBuild,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			expr, err := parseGoBuildConstraint([]byte(test.content))
			if !errors.Is(err, test.wantErr) {
				t.Fatalf(
					"parseGoBuildConstraint() error = %v, want %v",
					err,
					test.wantErr,
				)
			}
			if test.wantErr != nil {
				return
			}
			if got := expr.String(); got != test.want {
				t.Fatalf(
					"parseGoBuildConstraint() = %q, want %q",
					got,
					test.want,
				)
			}
		})
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

// sqliteRelaxedMarkers are the DSN fragments that make an open exempt from
// TestSQLiteTestOpensSetSynchronous: an explicit synchronous pragma (in any
// mode, so a test that really needs FULL can say so) or a database that never
// touches disk.
var sqliteRelaxedMarkers = []string{
	"synchronous(",
	"mode=memory",
	":memory:",
}

// TestSQLiteTestOpensSetSynchronous fails for any on-disk SQLite open in a
// test file or under internal/test/ that leaves synchronous unset.
//
// modernc.org/sqlite defaults to synchronous=FULL, one flush per autocommitted
// statement, and a WAL journal does not change that default. Windows CI pays
// ~19ms per flush, which made the SQLite-backed test packages the p90 of the
// job. Every such database is created, asserted against and discarded inside
// one test run, so the flush buys nothing: add _pragma=synchronous(OFF) to the
// DSN. Production connections are out of scope on purpose -- the provider's
// synchronous(NORMAL) contract is documented in DATABASE.md and this rule must
// never reach it, which is why only test files and internal/test/ are scanned.
func TestSQLiteTestOpensSetSynchronous(t *testing.T) {
	t.Parallel()

	repoRoot := findRepoRoot(t)
	files, err := goFilesBelow(repoRoot, ".")
	if err != nil {
		t.Fatalf("list go files: %v", err)
	}

	constsByDir := map[string]map[string]string{}
	var violations []string
	for _, file := range files {
		rel, err := relativePath(repoRoot, file)
		if err != nil {
			t.Fatal(err)
		}
		if !isSQLiteTestHarnessFile(rel) {
			continue
		}
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		dir := filepath.Dir(file)
		consts, ok := constsByDir[dir]
		if !ok {
			consts = packageStringConsts(t, dir)
			constsByDir[dir] = consts
		}
		found, err := sqliteOpensWithoutSynchronous(rel, src, consts)
		if err != nil {
			t.Fatal(err)
		}
		violations = append(violations, found...)
	}

	if len(violations) > 0 {
		slices.Sort(violations)
		t.Fatalf(
			"on-disk SQLite opens in tests must set "+
				"_pragma=synchronous(OFF); the driver default (FULL) "+
				"flushes on every autocommit. Add "+
				"_pragma=journal_mode(MEMORY) too when the test owns "+
				"the file alone (see testDBPragmas in "+
				"migrations/runner_test.go); leave the "+
				"journal mode alone for a file a provider has opened:"+
				"\n  %s",
			strings.Join(violations, "\n  "),
		)
	}
}

// TestSQLiteOpensWithoutSynchronousDetector pins the detector itself, so the
// tree scan above cannot pass because the scanner stopped matching.
func TestSQLiteOpensWithoutSynchronousDetector(t *testing.T) {
	t.Parallel()

	const header = "package p\nimport \"database/sql\"\n" +
		"const relaxed = \"_pragma=synchronous(OFF)\"\n"
	cases := []struct {
		name string
		body string
		want int
	}{
		{
			"bare path flagged",
			`func f(p string) { sql.Open("sqlite", p+"/m.sqlite") }`,
			1,
		},
		{
			"joined path flagged",
			`func f(d string) { sql.Open("sqlite", filepath.Join(d, "m")) }`,
			1,
		},
		{
			"wal without synchronous flagged",
			`func f(p string) {
				sql.Open("sqlite", "file:"+p+"?_pragma=journal_mode(WAL)")
			}`,
			1,
		},
		{
			"opaque dsn flagged",
			`func f(dsn string) { sql.Open("sqlite", dsn) }`,
			1,
		},
		{
			"literal synchronous exempt",
			`func f(p string) {
				sql.Open("sqlite", "file:"+p+"?_pragma=synchronous(OFF)")
			}`,
			0,
		},
		{
			"explicit full exempt",
			`func f(p string) {
				sql.Open("sqlite", "file:"+p+"?_pragma=synchronous(FULL)")
			}`,
			0,
		},
		{
			"const synchronous exempt",
			`func f(p string) { sql.Open("sqlite", "file:"+p+"?"+relaxed) }`,
			0,
		},
		{
			"memory exempt",
			`func f() { sql.Open("sqlite", "file::memory:?cache=shared") }`,
			0,
		},
		{
			"opaque dsn built with memory exempt",
			`func f() {
				dsn := "file:x?mode=memory&cache=shared"
				sql.Open("sqlite", dsn)
			}`,
			0,
		},
		{
			"opaque dsn built with synchronous exempt",
			`func f(p string) {
				dsn := "file:" + p + "?_pragma=synchronous(OFF)"
				sql.Open("sqlite", dsn)
			}`,
			0,
		},
		{
			"dsn closure with memory exempt",
			`func f() {
				dsn := func() string { return "file:x?mode=memory" }
				sql.Open("sqlite", dsn())
			}`,
			0,
		},
		{
			"conditional append flagged",
			`func f(p string, fast bool) {
				dsn := p
				if fast {
					dsn += "?_pragma=synchronous(OFF)"
				}
				sql.Open("sqlite", dsn)
			}`,
			1,
		},
		{
			"conditional reassignment flagged",
			`func f(p string, full bool) {
				dsn := "file:" + p + "?_pragma=synchronous(OFF)"
				if full {
					dsn = p
				}
				sql.Open("sqlite", dsn)
			}`,
			1,
		},
		{
			"var without value flagged",
			`func f(fast bool) {
				var dsn string
				if fast {
					dsn = "file:x?_pragma=synchronous(OFF)"
				}
				sql.Open("sqlite", dsn)
			}`,
			1,
		},
		{
			"marker elsewhere in declaration flagged",
			`func f(p string) {
				sql.Open("sqlite", "file::memory:")
				dsn := p
				sql.Open("sqlite", dsn)
			}`,
			1,
		},
		{
			"call dsn with marker elsewhere flagged",
			`func f(d string) {
				_ = "_pragma=synchronous(OFF)"
				sql.Open("sqlite", pathFor(d))
			}`,
			1,
		},
		{
			"other driver ignored",
			`func f(p string) { sql.Open("pgx", p) }`,
			0,
		},
		{
			"OpenDB flagged",
			`func f(p string) { sqlstore.OpenDB("sqlite", p, "x", false) }`,
			1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			found, err := sqliteOpensWithoutSynchronous(
				"p_test.go",
				[]byte(header+tc.body),
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(found) != tc.want {
				t.Fatalf(
					"got %d violations %v, want %d",
					len(found),
					found,
					tc.want,
				)
			}
		})
	}
}

// isSQLiteTestHarnessFile reports whether a repository-relative path is test
// code: a _test.go file, or any file of the internal/test/ harness packages,
// which are only ever imported by tests.
func isSQLiteTestHarnessFile(rel string) bool {
	return strings.HasSuffix(rel, "_test.go") ||
		strings.HasPrefix(rel, "internal/test/")
}

// packageStringConsts returns the source text of every string-valued
// package-level constant in dir's Go files, keyed by name. A DSN assembled
// from a shared constant (the migrations package's testDBPragmas) is judged
// by what the constant contains rather than by its name.
func packageStringConsts(t *testing.T, dir string) map[string]string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	consts := map[string]string{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		collectStringConsts(consts, fset, src, parsed)
	}
	return consts
}

func collectStringConsts(
	consts map[string]string,
	fset *token.FileSet,
	src []byte,
	parsed *ast.File,
) {
	for _, decl := range parsed.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if i < len(value.Values) {
					consts[name.Name] = nodeSource(fset, src, value.Values[i])
				}
			}
		}
	}
}

// sqliteOpensWithoutSynchronous returns "file:line: dsn" for every
// Open/OpenDB call on the "sqlite" driver whose DSN carries none of
// sqliteRelaxedMarkers.
//
// A DSN is judged by its own text plus what its identifiers resolve to: a
// string constant, or a variable assigned in the enclosing top-level
// declaration. A variable counts only when every plain assignment to it
// carries a marker, so a path that reassigns it, or a var declared with no
// value, leaves the open flagged. A += is not credited: the detector cannot
// tell whether it runs on every path to the open, and appending never
// removes a pragma the assignments already set.
func sqliteOpensWithoutSynchronous(
	name string,
	src []byte,
	consts map[string]string,
) ([]string, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", name, err)
	}

	own := map[string]string{}
	collectStringConsts(own, fset, src, parsed)
	maps.Copy(own, consts)

	var violations []string
	for _, decl := range parsed.Decls {
		resolver := dsnResolver{
			fset:     fset,
			src:      src,
			consts:   own,
			assigns:  plainAssignments(decl),
			visiting: map[string]bool{},
		}
		ast.Inspect(decl, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || !isSQLiteOpenCall(call) {
				return true
			}
			dsn := call.Args[1]
			if resolver.relaxes(dsn) {
				return true
			}
			violations = append(violations, fmt.Sprintf(
				"%s:%d: %s",
				name,
				fset.Position(call.Pos()).Line,
				strings.Join(strings.Fields(nodeSource(fset, src, dsn)), " "),
			))
			return true
		})
	}
	return violations, nil
}

// plainAssignments maps each identifier assigned with =, := or a var/const
// spec anywhere in decl to its right-hand sides. A nil entry is a value the
// detector cannot see: a var with no initializer, one result of a
// multi-value call, or a range variable.
func plainAssignments(decl ast.Decl) map[string][]ast.Expr {
	assigns := map[string][]ast.Expr{}
	add := func(lhs ast.Expr, rhs ast.Expr) {
		if ident, ok := lhs.(*ast.Ident); ok && ident.Name != "_" {
			assigns[ident.Name] = append(assigns[ident.Name], rhs)
		}
	}
	ast.Inspect(decl, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.AssignStmt:
			if n.Tok != token.ASSIGN && n.Tok != token.DEFINE {
				return true
			}
			for i, lhs := range n.Lhs {
				var rhs ast.Expr
				if len(n.Rhs) == len(n.Lhs) {
					rhs = n.Rhs[i]
				}
				add(lhs, rhs)
			}
		case *ast.ValueSpec:
			for i, ident := range n.Names {
				var rhs ast.Expr
				if len(n.Values) == len(n.Names) {
					rhs = n.Values[i]
				}
				add(ident, rhs)
			}
		case *ast.RangeStmt:
			if n.Tok == token.DEFINE {
				add(n.Key, nil)
				if n.Value != nil {
					add(n.Value, nil)
				}
			}
		}
		return true
	})
	return assigns
}

type dsnResolver struct {
	fset     *token.FileSet
	src      []byte
	consts   map[string]string
	assigns  map[string][]ast.Expr
	visiting map[string]bool
}

func (r dsnResolver) relaxes(expr ast.Expr) bool {
	if relaxesSynchronous(nodeSource(r.fset, r.src, expr)) {
		return true
	}
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && r.identRelaxes(ident.Name) {
			found = true
		}
		return !found
	})
	return found
}

// identRelaxes prefers a local assignment over a package constant of the
// same name, since the local one shadows it.
func (r dsnResolver) identRelaxes(name string) bool {
	if r.visiting[name] {
		return false
	}
	r.visiting[name] = true
	defer delete(r.visiting, name)

	if values, ok := r.assigns[name]; ok {
		for _, value := range values {
			if value == nil || !r.relaxes(value) {
				return false
			}
		}
		return true
	}
	return relaxesSynchronous(r.consts[name])
}

// isSQLiteOpenCall matches sql.Open("sqlite", dsn) and
// sqlstore.OpenDB("sqlite", dsn, ...).
func isSQLiteOpenCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (sel.Sel.Name != "Open" && sel.Sel.Name != "OpenDB") {
		return false
	}
	if len(call.Args) < 2 {
		return false
	}
	driver, ok := call.Args[0].(*ast.BasicLit)
	return ok && driver.Kind == token.STRING && driver.Value == `"sqlite"`
}

func relaxesSynchronous(dsnText string) bool {
	return slices.ContainsFunc(
		sqliteRelaxedMarkers,
		func(marker string) bool { return strings.Contains(dsnText, marker) },
	)
}

func nodeSource(fset *token.FileSet, src []byte, node ast.Node) string {
	start := fset.Position(node.Pos()).Offset
	end := fset.Position(node.End()).Offset
	return string(src[start:end])
}

// TestGoFilesBelowIncludesTestFiles pins that the shared walker returns
// _test.go files: TestSQLiteTestOpensSetSynchronous scans only test code, so a
// walker that dropped them would leave that guard with nothing to check.
func TestGoFilesBelowIncludesTestFiles(t *testing.T) {
	t.Parallel()

	repoRoot := findRepoRoot(t)
	files, err := goFilesBelow(repoRoot, "internal/architecture")
	if err != nil {
		t.Fatalf("list go files: %v", err)
	}
	want := filepath.Join(
		repoRoot,
		"internal",
		"architecture",
		"tests_test.go",
	)
	if !slices.Contains(files, want) {
		t.Fatalf("goFilesBelow omitted %s; got %v", want, files)
	}
}

// TestImportBoundaryTestExemptionIsScoped pins that a test-import exemption
// admits only the named package from the named file: another test file, and
// another forbidden package from the exempt file, are still reported.
func TestImportBoundaryTestExemptionIsScoped(t *testing.T) {
	t.Parallel()

	repoRoot := t.TempDir()
	writeGoFile := func(rel string, imports ...string) {
		t.Helper()
		var src strings.Builder
		src.WriteString("package ledger\n\nimport (\n")
		for _, imp := range imports {
			fmt.Fprintf(&src, "\t_ %q\n", modulePath+"/"+imp)
		}
		src.WriteString(")\n")
		path := filepath.Join(repoRoot, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(src.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeGoFile("ledger/exempt_test.go", "mempool", "peergov")
	writeGoFile("ledger/other_test.go", "mempool")
	writeGoFile("ledger/state.go", "mempool")

	rule := importBoundaryRule{
		from:      "ledger",
		forbidden: []string{"peergov", "mempool"},
		reason:    "test",
		testImportExemptions: map[string][]string{
			"ledger/exempt_test.go": {"mempool"},
		},
	}
	violations, err := importBoundaryViolations(repoRoot, rule)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(violations)
	want := []string{
		"ledger/exempt_test.go imports " + modulePath + "/peergov: test",
		"ledger/other_test.go imports " + modulePath + "/mempool: test",
		"ledger/state.go imports " + modulePath + "/mempool: test",
	}
	if !slices.Equal(violations, want) {
		t.Fatalf("violations:\n%v\nwant:\n%v", violations, want)
	}
}
