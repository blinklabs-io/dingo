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
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const devnetRunner = "internal/test/devnet/run-tests.sh"

var devnetLeiosInvocation = regexp.MustCompile(
	`(^|[\s/])run-tests\.sh\s+--leios(\s|$)`,
)

type devnetWorkflowStep struct {
	Name string            `yaml:"name"`
	If   string            `yaml:"if"`
	Uses string            `yaml:"uses"`
	Run  string            `yaml:"run"`
	Env  map[string]string `yaml:"env"`
	With map[string]any    `yaml:"with"`
}

type devnetWorkflowJob struct {
	Env   map[string]string    `yaml:"env"`
	Steps []devnetWorkflowStep `yaml:"steps"`
}

type devnetWorkflow struct {
	On   map[string]any
	Env  map[string]string
	Jobs map[string]devnetWorkflowJob
}

func (w *devnetWorkflow) UnmarshalYAML(node *yaml.Node) error {
	var raw struct {
		On   map[string]any               `yaml:"on"`
		Env  map[string]string            `yaml:"env"`
		Jobs map[string]devnetWorkflowJob `yaml:"jobs"`
	}
	if err := node.Decode(&raw); err != nil {
		return err
	}
	*w = devnetWorkflow(raw)
	return nil
}

// TestLeiosDevNetScenarioRunsInWorkflow keeps the Dijkstra/Leios
// producer-to-peer DevNet scenario wired into a workflow. The scenario only
// runs through run-tests.sh --leios, so without a workflow that calls it the
// endorser-block path is never exercised automatically. The workflow must be
// off the pull-request and push path (the scenario needs Docker and minutes of
// chain time), and must keep the failure artifacts run-tests.sh preserves.
func TestLeiosDevNetScenarioRunsInWorkflow(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)

	var (
		found    bool
		workflow devnetWorkflow
		job      devnetWorkflowJob
		runIdx   int
		rel      string
	)
	for _, file := range workflowFiles(t, root) {
		var candidate devnetWorkflow
		if err := yaml.Unmarshal(
			[]byte(readRepoFile(t, root, file)), &candidate,
		); err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, j := range candidate.Jobs {
			for i, step := range j.Steps {
				if devnetLeiosInvocation.MatchString(step.Run) {
					found, workflow, job, runIdx, rel = true, candidate, j, i, file
				}
			}
		}
	}
	if !found {
		t.Fatalf(
			"no workflow step runs `%s --leios`; the Leios producer-to-peer DevNet scenario is never exercised automatically",
			devnetRunner,
		)
	}

	if _, ok := workflow.On["schedule"]; !ok {
		if _, ok := workflow.On["workflow_dispatch"]; !ok {
			t.Errorf("%s must trigger on schedule or workflow_dispatch", rel)
		}
	}
	for _, trigger := range []string{"pull_request", "pull_request_target", "push"} {
		if _, ok := workflow.On[trigger]; ok {
			t.Errorf(
				"%s must not trigger on %s: the DevNet scenario is too heavy for the merge path",
				rel, trigger,
			)
		}
	}

	run := job.Steps[runIdx]
	artifactDir := run.Env["DEVNET_ARTIFACT_DIR"]
	if artifactDir == "" {
		artifactDir = job.Env["DEVNET_ARTIFACT_DIR"]
	}
	if artifactDir == "" {
		artifactDir = workflow.Env["DEVNET_ARTIFACT_DIR"]
	}
	if artifactDir == "" {
		t.Fatalf(
			"%s must set DEVNET_ARTIFACT_DIR so failure evidence lands at a known path",
			rel,
		)
	}

	uploaded := false
	for _, step := range job.Steps[runIdx+1:] {
		if !strings.HasPrefix(step.Uses, "actions/upload-artifact@") {
			continue
		}
		path, _ := step.With["path"].(string)
		if !strings.Contains(path, artifactDir) &&
			!strings.Contains(path, "env.DEVNET_ARTIFACT_DIR") {
			continue
		}
		if !strings.Contains(step.If, "failure()") &&
			!strings.Contains(step.If, "always()") {
			t.Errorf(
				"artifact upload %q must run when the scenario fails (if: failure() or always())",
				step.Name,
			)
		}
		uploaded = true
	}
	if !uploaded {
		t.Errorf(
			"%s must upload DEVNET_ARTIFACT_DIR with actions/upload-artifact after the scenario step",
			rel,
		)
	}
}
