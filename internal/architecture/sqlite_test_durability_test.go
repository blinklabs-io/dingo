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
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

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
				"the file alone (see migrations/dsn_test.go); leave the "+
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
// A DSN that is a bare identifier or a call (no string literal in it) cannot
// be judged from the argument alone, so it is judged by the enclosing
// top-level declaration instead: the DSN is built there or in a closure
// there. Anything else is judged by the argument's own text, so a function
// that opens one in-memory and one on-disk database cannot excuse the second.
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
		declText := nodeSource(fset, src, decl)
		ast.Inspect(decl, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || !isSQLiteOpenCall(call) {
				return true
			}
			dsn := call.Args[1]
			text := nodeSource(fset, src, dsn)
			if !containsStringLiteral(dsn) {
				text = declText
			}
			ast.Inspect(dsn, func(inner ast.Node) bool {
				if ident, ok := inner.(*ast.Ident); ok {
					text += " " + own[ident.Name]
				}
				return true
			})
			if relaxesSynchronous(text) {
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

func containsStringLiteral(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			found = true
		}
		return !found
	})
	return found
}

func nodeSource(fset *token.FileSet, src []byte, node ast.Node) string {
	start := fset.Position(node.Pos()).Offset
	end := fset.Position(node.End()).Offset
	return string(src[start:end])
}
