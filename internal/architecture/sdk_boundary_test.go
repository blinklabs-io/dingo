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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	// sdkDir is the plugin SDK, a nested module plugins build against
	// without depending on the node module.
	sdkDir        = "sdk"
	sdkModulePath = modulePath + "/" + sdkDir
	// sdkCardanoDir is the only SDK tree that may expose gouroboros types,
	// which keeps every other contract chain-neutral.
	sdkCardanoDir  = sdkDir + "/cardano"
	gouroborosPath = "github.com/blinklabs-io/gouroboros"
)

// TestSDKImportBoundary keeps the sdk module buildable on its own. The root
// module's ./... does not include sdk/, so nothing else would notice an SDK
// package importing the node: a plugin would then pull the whole node module
// into its build.
func TestSDKImportBoundary(t *testing.T) {
	t.Parallel()

	violations, err := sdkImportViolations(findRepoRoot(t))
	if err != nil {
		t.Fatalf("check sdk import boundary: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf(
			"forbidden sdk imports found:\n%s",
			strings.Join(violations, "\n"),
		)
	}
}

func TestSDKImportBoundaryReportsViolations(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	files := map[string]string{
		"sdk/plugin/node.go":       modulePath + "/internal/node",
		"sdk/plugin/root.go":       modulePath,
		"sdk/plugin/neutral.go":    sdkModulePath + "/chain",
		"sdk/chain/ledger_test.go": gouroborosPath + "/ledger",
		"sdk/chain/stdlib.go":      "context",
		"sdk/chain/sibling.go":     modulePath + "-operator/api",
		"sdk/cardano/types.go":     gouroborosPath + "/ledger",
		"sdk/cardano/ledger.go":    modulePath + "/ledger",
		"sdk/testdata/fixture.go":  modulePath,
	}
	for rel, importPath := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		src := "package p\n\nimport _ \"" + importPath + "\"\n"
		if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := sdkImportViolations(root)
	if err != nil {
		t.Fatalf("sdkImportViolations: %v", err)
	}
	want := []string{
		"sdk/cardano/ledger.go imports " + modulePath + "/ledger",
		"sdk/chain/ledger_test.go imports " + gouroborosPath + "/ledger",
		"sdk/plugin/node.go imports " + modulePath + "/internal/node",
		"sdk/plugin/root.go imports " + modulePath,
	}
	if !slices.Equal(got, want) {
		t.Fatalf(
			"violations:\n%s\nwant:\n%s",
			strings.Join(got, "\n"),
			strings.Join(want, "\n"),
		)
	}
}

// sdkImportViolations reports, sorted, every import under sdk/ of a dingo
// module package outside the sdk itself, and every gouroboros import outside
// sdk/cardano. Test files count: a test import also lands in sdk/go.mod.
func sdkImportViolations(repoRoot string) ([]string, error) {
	if _, err := os.Stat(filepath.Join(repoRoot, sdkDir)); err != nil {
		return nil, err
	}
	files, err := goFilesBelow(repoRoot, sdkDir)
	if err != nil {
		return nil, err
	}
	var violations []string
	for _, file := range files {
		rel, err := relativePath(repoRoot, file)
		if err != nil {
			return nil, err
		}
		imports, err := importsForFile(file)
		if err != nil {
			return nil, err
		}
		inCardano := rel == sdkCardanoDir ||
			strings.HasPrefix(rel, sdkCardanoDir+"/")
		for _, importPath := range imports {
			nodeImport := withinModule(importPath, modulePath) &&
				!withinModule(importPath, sdkModulePath)
			gouroborosImport := !inCardano &&
				withinModule(importPath, gouroborosPath)
			if nodeImport || gouroborosImport {
				violations = append(violations, rel+" imports "+importPath)
			}
		}
	}
	slices.Sort(violations)
	return violations, nil
}

// withinModule reports whether importPath is the module path itself or one of
// its packages. A plain prefix test would also match a sibling module such as
// dingo-operator.
func withinModule(importPath, module string) bool {
	return importPath == module || strings.HasPrefix(importPath, module+"/")
}
