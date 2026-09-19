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
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// goTestWorkflow is the workflow that renders the `go-test` checks. Branch
// protection matches a required context by name, so this rule is pinned to
// this file rather than searching every workflow for a `go test` invocation.
const goTestWorkflow = ".github/workflows/go-test.yml"

// goTestInvocation matches a `go test` command at the start of a line or
// after a shell separator, so a step that merely mentions the words in a
// comment does not count as coverage.
var goTestInvocation = regexp.MustCompile(`(^|[\n;&|]|&&)\s*go test\s`)

// runStep is the part of an Actions `run` step this rule reads: the script it
// executes and the directory it executes in. `working-directory` is a step
// key here, unlike the `with:` input the lint action takes.
type runStep struct {
	Run              string `yaml:"run"`
	WorkingDirectory string `yaml:"working-directory"`
}

// runJob is one job's run steps.
type runJob struct {
	Steps []runStep `yaml:"steps"`
}

// runWorkflow is the minimal shape of a workflow file for this rule.
type runWorkflow struct {
	Jobs map[string]runJob `yaml:"jobs"`
}

// goTestDirs returns every module directory the go-test workflow runs
// `go test` in, with "." for the root module.
func goTestDirs(t *testing.T, root string) map[string]bool {
	t.Helper()

	raw := readRepoFile(t, root, goTestWorkflow)
	var parsed runWorkflow
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", goTestWorkflow, err)
	}

	dirs := make(map[string]bool)
	for _, job := range parsed.Jobs {
		for _, step := range job.Steps {
			if !goTestInvocation.MatchString(step.Run) {
				continue
			}
			dir := filepath.ToSlash(
				strings.TrimSpace(step.WorkingDirectory),
			)
			if dir == "" {
				dir = "."
			}
			dirs[dir] = true
		}
	}
	if len(dirs) == 0 {
		t.Fatalf("%s runs no go test step", goTestWorkflow)
	}
	return dirs
}

// TestGoTestCoversEveryGoModule checks that the go-test workflow runs the
// tests of every Go module in the tree. A nested module has its own go.mod,
// so the root module's `./...` never reaches it: without a run of its own, a
// green go-test check says nothing about that module's code, and a failing
// package there is invisible to every configured check.
func TestGoTestCoversEveryGoModule(t *testing.T) {
	root := repoRoot(t)

	covered := goTestDirs(t, root)
	for _, dir := range goModuleDirs(t, root) {
		if !covered[dir] {
			t.Errorf(
				"module %s has a go.mod but %s never tests it; "+
					"add a go test step with working-directory: %s",
				dir,
				goTestWorkflow,
				dir,
			)
		}
	}
}
