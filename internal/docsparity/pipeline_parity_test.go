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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The two CI pipelines. go-test.yml runs on a pull request; publish.yml runs
// on a push to main and on a release tag. Exactly one of them runs for any
// given event, which is what keeps a merge from starting both.
const (
	prPipeline      = ".github/workflows/go-test.yml"
	publishPipeline = ".github/workflows/publish.yml"
)

// pipelineStages are the jobs both pipelines must define identically. They are
// duplicated between the files rather than factored into a reusable workflow
// because `needs:` cannot cross workflow files, and a called workflow would
// rename every check context that branch protection matches on. The price of
// that duplication is drift, which is what this file exists to prevent.
var pipelineStages = []string{
	"lint",
	"govulncheck",
	"examples-web",
	"go-test-linux-quick",
	"go-test-modules",
	"go-test-linux",
	"go-test-linux-race",
	"go-test-windows",
	"go-test-macos",
}

// pipelineJobs decodes a workflow's jobs as plain YAML values, so a comparison
// covers everything a job declares -- runner, services, env, every step and its
// command -- rather than the handful of fields a typed struct would name.
// Comments do not survive the decode, so the two files may explain themselves
// differently.
func pipelineJobs(t *testing.T, root, workflow string) map[string]any {
	t.Helper()

	var parsed struct {
		Jobs map[string]any `yaml:"jobs"`
	}
	raw := readRepoFile(t, root, workflow)
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", workflow, err)
	}
	if len(parsed.Jobs) == 0 {
		t.Fatalf("%s declares no jobs", workflow)
	}
	return parsed.Jobs
}

// jobNeeds returns a job's `needs` list. Actions accepts either a scalar or a
// sequence, so both shapes are normalized here.
func jobNeeds(t *testing.T, workflow, name string, job any) []string {
	t.Helper()

	fields, ok := job.(map[string]any)
	if !ok {
		t.Fatalf("%s job %s is not a mapping", workflow, name)
	}
	switch needs := fields["needs"].(type) {
	case nil:
		return nil
	case string:
		return []string{needs}
	case []any:
		out := make([]string, 0, len(needs))
		for _, entry := range needs {
			text, ok := entry.(string)
			if !ok {
				t.Fatalf(
					"%s job %s has a non-string needs entry %v",
					workflow,
					name,
					entry,
				)
			}
			out = append(out, text)
		}
		return out
	default:
		t.Fatalf(
			"%s job %s has an unsupported needs shape %T",
			workflow,
			name,
			fields["needs"],
		)
		return nil
	}
}

// TestPipelineStagesMatch checks that every shared stage is defined the same
// way in both pipelines. A fix applied to the pull-request pipeline and not to
// the publish one means main is tested differently from the change that was
// reviewed, which is the failure mode the old split between go-test.yml and
// publish.yml's `ci` job actually produced: the release gate drifted into
// running commands no pull request had run.
func TestPipelineStagesMatch(t *testing.T) {
	root := repoRoot(t)
	prJobs := pipelineJobs(t, root, prPipeline)
	publishJobs := pipelineJobs(t, root, publishPipeline)

	for _, stage := range pipelineStages {
		pr, ok := prJobs[stage]
		if !ok {
			t.Errorf("%s has no %s job", prPipeline, stage)
			continue
		}
		published, ok := publishJobs[stage]
		if !ok {
			t.Errorf("%s has no %s job", publishPipeline, stage)
			continue
		}
		if !reflect.DeepEqual(pr, published) {
			t.Errorf(
				"job %s differs between %s and %s; the stages are duplicated "+
					"because needs: cannot cross workflow files, so a change "+
					"to one has to be made in both",
				stage,
				prPipeline,
				publishPipeline,
			)
		}
	}
}

// TestPipelineStagesAreOrdered checks the dependency chain that makes the
// pipeline cheap before it is expensive: lint gates the quick Linux suite,
// which gates the four platform suites. Without it a stage could be detached
// from its gate and start fanning out four runners again on a change that does
// not compile.
func TestPipelineStagesAreOrdered(t *testing.T) {
	root := repoRoot(t)

	// The platform suites that must not start until the cheap gate is green.
	fanOut := []string{
		"go-test-linux",
		"go-test-linux-race",
		"go-test-windows",
		"go-test-macos",
	}

	for _, workflow := range []string{prPipeline, publishPipeline} {
		jobs := pipelineJobs(t, root, workflow)

		quick, ok := jobs["go-test-linux-quick"]
		if !ok {
			t.Errorf("%s has no go-test-linux-quick job", workflow)
			continue
		}
		if !contains(jobNeeds(t, workflow, "go-test-linux-quick", quick), "lint") {
			t.Errorf(
				"%s: go-test-linux-quick does not need lint; the cheapest "+
					"check has to gate the suite below it",
				workflow,
			)
		}

		for _, name := range fanOut {
			job, ok := jobs[name]
			if !ok {
				t.Errorf("%s has no %s job", workflow, name)
				continue
			}
			needs := jobNeeds(t, workflow, name, job)
			if !contains(needs, "go-test-linux-quick") {
				t.Errorf(
					"%s: %s does not need go-test-linux-quick; it would fan "+
						"out before the cheap Linux suite has run",
					workflow,
					name,
				)
			}
		}
	}
}

// TestReleaseGatesOnGovulncheck checks that a tag cannot publish while
// govulncheck is failing.
//
// This was lost once already. `make govulncheck` was the last step of
// publish.yml's `ci` job and create-draft-release needed `[ci]`, so the gate
// was implicit in the job boundary. Splitting those steps into separate jobs
// dropped it, and nothing failed, because a missing edge in a dependency graph
// looks exactly like a graph that never had one.
//
// go-test.yml is deliberately not checked here. Its govulncheck job gates
// nothing, so a new upstream advisory fails the run without blocking every
// merge in the repository while it is triaged.
func TestReleaseGatesOnGovulncheck(t *testing.T) {
	root := repoRoot(t)
	jobs := pipelineJobs(t, root, publishPipeline)

	job, ok := jobs["create-draft-release"]
	if !ok {
		t.Fatalf("%s has no create-draft-release job", publishPipeline)
	}
	needs := jobNeeds(t, publishPipeline, "create-draft-release", job)
	if !contains(needs, "govulncheck") {
		t.Errorf(
			"%s: create-draft-release does not need govulncheck; a tag could "+
				"upload binaries and publish a release with a reachable "+
				"vulnerability",
			publishPipeline,
		)
	}
}

// buildGates names, for each build job, every test job for its own runner OS.
// Gating a build on its own platform and no other is what lets the Linux
// binaries start while the Windows suite is still running.
//
// Linux has two suites and a build must wait for both. Naming only one would
// leave the other free to be dropped from the build's dependencies with this
// check still green, which is exactly the hole that would let build-linux run
// without the race suite.
var buildGates = map[string][]string{
	"build-linux":   {"go-test-linux", "go-test-linux-race"},
	"build-windows": {"go-test-windows"},
	"build-macos":   {"go-test-macos"},
}

// otherPlatformTests are the test jobs a given build job must NOT depend on.
var otherPlatformTests = map[string][]string{
	"build-linux":   {"go-test-windows", "go-test-macos"},
	"build-windows": {"go-test-linux", "go-test-linux-race", "go-test-macos"},
	"build-macos":   {"go-test-linux", "go-test-linux-race", "go-test-windows"},
}

// TestBuildsGateOnTheirOwnPlatform checks that each build job waits for its own
// runner OS's tests and for nothing else's. Depending on another platform is
// what put the whole build stage behind the slowest and least predictable test
// job in the pipeline; depending on none of them is what let images and
// binaries publish from a commit no suite had covered.
func TestBuildsGateOnTheirOwnPlatform(t *testing.T) {
	root := repoRoot(t)

	for _, workflow := range []string{prPipeline, publishPipeline} {
		jobs := pipelineJobs(t, root, workflow)

		for _, build := range sortedKeys(buildGates) {
			job, ok := jobs[build]
			if !ok {
				t.Errorf("%s has no %s job", workflow, build)
				continue
			}
			needs := jobNeeds(t, workflow, build, job)

			for _, gate := range buildGates[build] {
				if !contains(needs, gate) {
					t.Errorf(
						"%s: %s does not need %s; a build must not start "+
							"until every suite for its own platform passes",
						workflow,
						build,
						gate,
					)
				}
			}
			for _, foreign := range otherPlatformTests[build] {
				if contains(needs, foreign) {
					t.Errorf(
						"%s: %s needs %s, a test job for another platform; "+
							"that puts this build behind an unrelated "+
							"platform's runtime",
						workflow,
						build,
						foreign,
					)
				}
			}
		}
	}
}

// TestPipelineTriggersDoNotOverlap checks that a single event never starts both
// pipelines. They define the same job names, so an event that triggered both
// would render two check runs called `lint`, two called `go-test (Linux)`, and
// leave branch protection matching an ambiguous context -- besides doubling the
// runner cost of every merge, which is what this split was made to avoid.
func TestPipelineTriggersDoNotOverlap(t *testing.T) {
	root := repoRoot(t)

	// The complete allowlist per pipeline, not a list of events to reject.
	// Rejecting named events only would pass an addition nobody thought of:
	// pull_request_target on publish.yml would start both pipelines for one
	// pull request, and would do it with a writable token against unreviewed
	// code.
	allowed := map[string][]string{
		prPipeline:      {"pull_request", "workflow_dispatch"},
		publishPipeline: {"push"},
	}

	for _, workflow := range []string{prPipeline, publishPipeline} {
		want := make(map[string]struct{}, len(allowed[workflow]))
		for _, event := range allowed[workflow] {
			want[event] = struct{}{}
		}

		got := workflowTriggers(t, root, workflow)
		for event := range got {
			if _, ok := want[event]; !ok {
				t.Errorf(
					"%s triggers on %s, which is not in its allowlist %v; "+
						"exactly one pipeline may run for any given event",
					workflow,
					event,
					allowed[workflow],
				)
			}
		}
		for event := range want {
			if _, ok := got[event]; !ok {
				t.Errorf(
					"%s no longer triggers on %s",
					workflow,
					event,
				)
			}
		}
	}
}

// workflowTriggers returns the event names in a workflow's `on:` block.
func workflowTriggers(t *testing.T, root, workflow string) map[string]struct{} {
	t.Helper()

	// `on` is a YAML 1.1 boolean, so gopkg.in/yaml.v3 decodes the unquoted key
	// as `true` rather than the string "on". Both spellings are accepted here
	// so this does not depend on how the workflow happens to quote it.
	var parsed map[string]any
	raw := readRepoFile(t, root, workflow)
	if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
		t.Fatalf("parse %s: %v", workflow, err)
	}

	var block any
	for key, value := range parsed {
		if key == "on" || key == "true" {
			block = value
			break
		}
	}
	if block == nil {
		t.Fatalf("%s has no on: block", workflow)
	}

	events := make(map[string]struct{})
	switch on := block.(type) {
	case string:
		events[on] = struct{}{}
	case []any:
		for _, entry := range on {
			events[fmt.Sprint(entry)] = struct{}{}
		}
	case map[string]any:
		for key := range on {
			events[key] = struct{}{}
		}
	default:
		t.Fatalf("%s has an unsupported on: shape %T", workflow, block)
	}
	return events
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
