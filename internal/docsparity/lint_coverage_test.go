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
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// lintWorkflow is the workflow that renders the `lint` check. Branch
// protection matches a required context by name, so the coverage rules below
// are pinned to this file rather than searching every workflow for a
// golangci-lint invocation.
const lintWorkflow = ".github/workflows/golangci-lint.yml"

// lintActionRepo is the action whose invocations count as lint coverage. The
// version suffix is stripped before comparison so a bump does not silently
// drop a module from the check.
const lintActionRepo = "golangci/golangci-lint-action"

// workflowStep is the part of an Actions step these rules read: which action
// it runs, the directory it runs in, and the target platform it runs for.
type workflowStep struct {
	Uses string            `yaml:"uses"`
	Env  map[string]string `yaml:"env"`
	With struct {
		WorkingDirectory string `yaml:"working-directory"`
	} `yaml:"with"`
}

// workflowJob is one job's steps.
type workflowJob struct {
	Steps []workflowStep `yaml:"steps"`
}

// actionsWorkflow is the minimal shape of a workflow file.
type actionsWorkflow struct {
	Jobs map[string]workflowJob `yaml:"jobs"`
}

// lintRun is one golangci-lint invocation: the module directory it covers and
// the GOOS it covers it for.
type lintRun struct {
	dir  string
	goos string
}

// defaultLintGOOS is the platform a step with no GOOS override runs as. The
// lint job runs on ubuntu-latest, so an unset GOOS means linux.
const defaultLintGOOS = "linux"

// goModuleDirs returns the repository-relative directory of every Go module
// in the tree, with "." for the root module. This is the source of truth the
// lint workflow is checked against: a module that exists in the tree but not
// in CI is the gap these rules exist to catch, so adding a nested module
// fails this check until the workflow covers it.
func goModuleDirs(t *testing.T, root string) []string {
	t.Helper()

	// filesMatching passes a repository-relative path, so match on the base
	// name: comparing the whole path would find only the root module and
	// leave this check passing vacuously.
	mods := filesMatching(t, root, func(rel string) bool {
		return filepath.Base(rel) == "go.mod"
	})
	dirs := make([]string, 0, len(mods))
	for _, rel := range mods {
		dirs = append(dirs, filepath.ToSlash(filepath.Dir(rel)))
	}
	sort.Strings(dirs)
	return dirs
}

// lintRuns returns every golangci-lint invocation the lint workflow makes.
func lintRuns(t *testing.T, root string) []lintRun {
	t.Helper()

	raw := readRepoFile(t, root, lintWorkflow)
	var parsed actionsWorkflow
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", lintWorkflow, err)
	}

	var runs []lintRun
	for _, job := range parsed.Jobs {
		for _, step := range job.Steps {
			action, _, _ := strings.Cut(step.Uses, "@")
			if action != lintActionRepo {
				continue
			}
			dir := filepath.ToSlash(
				strings.TrimSpace(step.With.WorkingDirectory),
			)
			if dir == "" {
				dir = "."
			}
			goos := strings.TrimSpace(step.Env["GOOS"])
			if goos == "" {
				goos = defaultLintGOOS
			}
			runs = append(runs, lintRun{dir: dir, goos: goos})
		}
	}
	if len(runs) == 0 {
		t.Fatalf(
			"%s runs no %s step",
			lintWorkflow,
			lintActionRepo,
		)
	}
	return runs
}

// TestLintCoversEveryGoModule checks that the lint job runs golangci-lint
// against every Go module in the tree on the default platform. A nested
// module has its own go.mod, so the root module's `./...` never reaches it:
// without a run of its own, a green `lint` check says nothing about that
// module's code.
//
// Only default-GOOS runs count. A GOOS=windows run builds a different set of
// files, so letting it satisfy a module would allow the linux run for that
// module to be dropped while this check stayed green.
func TestLintCoversEveryGoModule(t *testing.T) {
	root := repoRoot(t)

	covered := make(map[string]bool)
	for _, run := range lintRuns(t, root) {
		if run.goos == defaultLintGOOS {
			covered[run.dir] = true
		}
	}

	for _, dir := range goModuleDirs(t, root) {
		if !covered[dir] {
			t.Errorf(
				"module %s has a go.mod but %s never lints it on "+
					"%s; add a golangci-lint step with "+
					"working-directory: %s",
				dir,
				lintWorkflow,
				defaultLintGOOS,
				dir,
			)
		}
	}
}

// TestLintCoversWindowsBuildTags checks that the root module is linted for
// windows as well as linux. Files behind `//go:build windows` are excluded
// from the linux run's build, so every linter is blind to them until a run
// with GOOS=windows compiles them in.
func TestLintCoversWindowsBuildTags(t *testing.T) {
	root := repoRoot(t)

	for _, run := range lintRuns(t, root) {
		if run.dir == "." && run.goos == "windows" {
			return
		}
	}
	t.Errorf(
		"%s never lints the root module with GOOS=windows; "+
			"files behind //go:build windows are unchecked",
		lintWorkflow,
	)
}
