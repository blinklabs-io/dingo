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

// lintWorkflows are the workflows that render the `lint` check. Branch
// protection matches a required context by name, so the coverage rules below
// are pinned to these files rather than searching every workflow for a
// golangci-lint invocation.
//
// There are two because `needs:` cannot cross workflow files: the lint job has
// to live inside each pipeline it gates. go-test.yml is the pull-request
// pipeline and publish.yml the main and release pipeline, and only one of them
// runs for any given event. Every rule below is checked against both, so
// covering a new module in one pipeline and forgetting the other fails here.
var lintWorkflows = []string{
	".github/workflows/go-test.yml",
	".github/workflows/publish.yml",
}

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

// lintRuns returns every golangci-lint invocation the named workflow makes.
func lintRuns(t *testing.T, root, workflow string) []lintRun {
	t.Helper()

	raw := readRepoFile(t, root, workflow)
	var parsed actionsWorkflow
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", workflow, err)
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
			workflow,
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
	modules := goModuleDirs(t, root)

	for _, workflow := range lintWorkflows {
		covered := make(map[string]bool)
		for _, run := range lintRuns(t, root, workflow) {
			if run.goos == defaultLintGOOS {
				covered[run.dir] = true
			}
		}

		for _, dir := range modules {
			if !covered[dir] {
				t.Errorf(
					"module %s has a go.mod but %s never lints it on "+
						"%s; add a golangci-lint step with "+
						"working-directory: %s",
					dir,
					workflow,
					defaultLintGOOS,
					dir,
				)
			}
		}
	}
}
