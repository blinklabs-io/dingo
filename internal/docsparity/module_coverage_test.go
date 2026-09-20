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
	"encoding/json"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// runStep is the part of a `run` step these rules read. working-directory is
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

// TestGoTestCoversEveryGoModule checks that both pipelines run `go test`
// against every Go module in the tree. A nested module has its own go.mod, so
// the root module's `./...` does not descend into it -- the go command
// refuses the pattern outright with "directory prefix ... does not contain
// main module" -- and without a run of its own that module's tests never
// execute in CI however green the suite looks.
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

// npmProject is one npm project in the tree: the directory holding its
// lockfile, and whether its package.json declares a build script.
type npmProject struct {
	dir      string
	hasBuild bool
}

// npmProjects returns every directory holding a package-lock.json. The
// lockfile is what makes `npm ci` possible and is what Dependabot's npm
// ecosystem updates, so it -- rather than package.json alone -- defines the
// set of projects CI has to install. .github/package.json describes the
// golang-npm release wrapper, carries no lockfile, and is not built here.
func npmProjects(t *testing.T, root string) []npmProject {
	t.Helper()

	locks := filesMatching(t, root, func(rel string) bool {
		return path.Base(rel) == "package-lock.json"
	})
	projects := make([]npmProject, 0, len(locks))
	for _, rel := range locks {
		dir := path.Dir(rel)
		var manifest struct {
			Scripts map[string]string `json:"scripts"`
		}
		raw := readRepoFile(t, root, path.Join(dir, "package.json"))
		if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
			t.Fatalf("parse %s/package.json: %v", dir, err)
		}
		projects = append(projects, npmProject{
			dir:      dir,
			hasBuild: manifest.Scripts["build"] != "",
		})
	}
	sort.Slice(projects, func(i, j int) bool {
		return projects[i].dir < projects[j].dir
	})
	return projects
}

// TestExamplesJobBuildsEveryNpmProject checks that both pipelines install and
// build every npm project in the tree. Nothing in the Go pipeline compiles
// TypeScript, so a project without steps of its own is verified by no check
// at all -- while .github/dependabot.yml keeps raising weekly dependency
// bumps against it, which is the change most likely to break it.
func TestExamplesJobBuildsEveryNpmProject(t *testing.T) {
	root := repoRoot(t)
	projects := npmProjects(t, root)
	if len(projects) == 0 {
		t.Fatal("no package-lock.json found in the tree")
	}

	for _, workflow := range []string{prPipeline, publishPipeline} {
		installed := commandDirs(
			t,
			root,
			workflow,
			func(command string) bool {
				return strings.Contains(command, "npm ci")
			},
		)
		built := commandDirs(
			t,
			root,
			workflow,
			func(command string) bool {
				return strings.Contains(command, "npm run build")
			},
		)

		for _, project := range projects {
			if !installed[project.dir] {
				t.Errorf(
					"%s has a package-lock.json but %s never runs "+
						"npm ci in it",
					project.dir,
					workflow,
				)
			}
			if project.hasBuild && !built[project.dir] {
				t.Errorf(
					"%s declares a build script but %s never runs "+
						"npm run build in it; a dependency bump that "+
						"fails to compile would pass every check",
					project.dir,
					workflow,
				)
			}
		}
	}
}
