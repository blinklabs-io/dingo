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
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// runStep is the part of a `run` step this rule reads. working-directory is
// a step-level key rather than an input, so it does not live under `with`
// the way the lint action's copy does.
type runStep struct {
	Name             string `yaml:"name"`
	Run              string `yaml:"run"`
	WorkingDirectory string `yaml:"working-directory"`
}

type runJob struct {
	Steps []runStep `yaml:"steps"`
}

type runWorkflow struct {
	Jobs map[string]runJob `yaml:"jobs"`
}

// commandDirs returns the directory each `run` step executes in, for every
// step whose command satisfies match. A step with no working-directory runs
// at the repository root, which is reported as ".".
func commandDirs(
	t *testing.T,
	root, workflow string,
	match func(command string) bool,
) map[string]bool {
	t.Helper()

	var parsed runWorkflow
	raw := readRepoFile(t, root, workflow)
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", workflow, err)
	}

	dirs := make(map[string]bool)
	for _, job := range parsed.Jobs {
		for _, step := range job.Steps {
			if !match(step.Run) {
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
	return dirs
}

// exampleModulePrefix marks the modules this rule does not apply to. The
// projects under examples/ demonstrate how to consume Dingo; they are not
// code Dingo ships, and running their suites is deliberately not a gate on
// merging or releasing, so that a broken demonstration cannot block real
// work. That is a standing decision rather than an oversight: do not
// "fix" a module reported here by adding a job for examples/.
//
// The exemption is scoped to `go test`. Every module in the tree, examples
// included, is still linted -- see TestLintCoversEveryGoModule -- because a
// golangci-lint run costs seconds and reports on the module it is pointed
// at rather than standing up whatever the example talks to.
const exampleModulePrefix = "examples/"

// TestGoTestCoversEveryGoModule checks that both pipelines run `go test`
// against every non-example Go module in the tree. A nested module has its
// own go.mod, so the root module's `./...` does not descend into it -- the
// go command refuses the pattern outright with "directory prefix ... does
// not contain main module" -- and without a run of its own that module's
// tests never execute in CI however green the suite looks.
//
// This is the `go test` half of the gap TestLintCoversEveryGoModule closes
// for golangci-lint: internal/test/antithesis was linted by CI for weeks
// while its three test packages ran nowhere.
func TestGoTestCoversEveryGoModule(t *testing.T) {
	root := repoRoot(t)
	modules := goModuleDirs(t, root)

	for _, workflow := range []string{prPipeline, publishPipeline} {
		covered := commandDirs(
			t,
			root,
			workflow,
			func(command string) bool {
				return strings.Contains(command, "go test") &&
					strings.Contains(command, "./...")
			},
		)
		if len(covered) == 0 {
			t.Fatalf("%s runs no go test step", workflow)
		}

		for _, dir := range modules {
			if strings.HasPrefix(dir, exampleModulePrefix) {
				continue
			}
			if !covered[dir] {
				t.Errorf(
					"module %s has a go.mod but %s never runs go test "+
						"in it; add a step with working-directory: %s",
					dir,
					workflow,
					dir,
				)
			}
		}
	}
}
