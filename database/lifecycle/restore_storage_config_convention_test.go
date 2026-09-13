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

package lifecycle_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// restoreStorageConfigAllowBareMarker exempts a call site from
// TestRestoreCallSitesUseBoundedBadgerConfig. A same-line trailing comment
// bearing this marker documents why passing the zero-value
// RestoreStorageConfig{} there is deliberate -- e.g. the target host's blob
// provider takes a struct{} config and would fail strict decoding of any
// non-empty map -- rather than the unbounded-default oversight this test
// otherwise guards against.
const restoreStorageConfigAllowBareMarker = "restoreconfig:zero-value-required"

// restoreFuncNames are lifecycle's exported entry points whose final
// parameter is a RestoreStorageConfig (see restore.go). Passing one without
// Blob to any of them resolves the blob plugin with a nil provider config,
// which for badger means the production 1 GiB
// value log / 128 MiB memtable defaults -- badger maps the value log at
// twice that, so 2 GiB is really reserved the moment the store opens. On
// Windows that reservation is not sparse, and it is real for as long as
// Restore holds the store open; enough concurrent test restores exhaust a CI
// runner's disk. testutil.BadgerBlobConfig and dbtest.NewDatabase already
// guard every other on-disk test store in this repository against exactly
// this; this test extends that guard to lifecycle.Restore's own callers.
var restoreFuncNames = map[string]bool{
	"Restore":            true,
	"RestoreValidated":   true,
	"RestoreRecoverable": true,
}

// TestRestoreCallSitesUseBoundedBadgerConfig statically scans every test file
// in this directory for a call to Restore, RestoreValidated, or
// RestoreRecoverable whose RestoreStorageConfig literal sets no Blob --
// including the bare RestoreStorageConfig{} and a Metadata-only literal --
// and fails naming each one found, unless the line carries
// restoreStorageConfigAllowBareMarker.
//
// A runtime assertion cannot make this same distinction: badger truncates its
// value log and memtable files back down when a store is cleanly stopped,
// and every Restore call in this package stops its stores well before
// Restore returns, so measuring reserved file sizes after the fact passes
// whether or not a call site supplies a bounded config (see the
// investigation on dingo#3746). Only a source-level check catches the
// regression of a new or edited call site reintroducing the unbounded
// default.
func TestRestoreCallSitesUseBoundedBadgerConfig(t *testing.T) {
	t.Parallel()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}

	fset := token.NewFileSet()
	var violations []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		lines := strings.Split(string(src), "\n")

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isRestoreCall(call.Fun) {
				return true
			}
			for _, arg := range call.Args {
				lit, ok := arg.(*ast.CompositeLit)
				if !ok || !isRestoreStorageConfigType(lit.Type) ||
					setsBlob(lit) {
					continue
				}
				pos := fset.Position(lit.Pos())
				var lineText string
				if pos.Line-1 < len(lines) {
					lineText = lines[pos.Line-1]
				}
				if strings.Contains(
					lineText, restoreStorageConfigAllowBareMarker,
				) {
					continue
				}
				violations = append(violations, fmt.Sprintf(
					"%s:%d: RestoreStorageConfig without Blob resolves "+
						"the restore's blob store with badger's unbounded "+
						"default sizes; set Blob: "+
						"testutil.BadgerBlobConfig(), or mark the line "+
						"with %q if the target host's provider ignores "+
						"its config",
					name, pos.Line, restoreStorageConfigAllowBareMarker,
				))
			}
			return true
		})
	}

	if len(violations) > 0 {
		t.Fatalf(
			"%d call site(s) resolve a Restore blob/metadata plugin with "+
				"an unbounded default config:\n%s",
			len(violations),
			strings.Join(violations, "\n"),
		)
	}
}

func isRestoreCall(fun ast.Expr) bool {
	switch f := fun.(type) {
	case *ast.Ident:
		return restoreFuncNames[f.Name]
	case *ast.SelectorExpr:
		return restoreFuncNames[f.Sel.Name]
	}
	return false
}

func isRestoreStorageConfigType(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name == "RestoreStorageConfig"
	case *ast.SelectorExpr:
		return t.Sel.Name == "RestoreStorageConfig"
	}
	return false
}

// setsBlob reports whether lit, a RestoreStorageConfig literal, supplies a
// Blob provider config. An unkeyed literal with elements sets Blob, its
// first field.
func setsBlob(lit *ast.CompositeLit) bool {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			return true
		}
		if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Blob" {
			return true
		}
	}
	return false
}
