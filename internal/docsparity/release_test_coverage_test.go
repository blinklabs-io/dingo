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
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const publishWorkflow = ".github/workflows/publish.yml"

var releaseServiceTestTriggers = []string{
	"POSTGRES_PASSWORD",
	"POSTGRES_DSN",
	"MYSQL_ROOT_PASSWORD",
	"MYSQL_DSN",
	"DINGO_TEST_S3_BUCKET",
}

type releaseWorkflowStep struct {
	Name string            `yaml:"name"`
	Run  string            `yaml:"run"`
	Env  map[string]string `yaml:"env"`
}

type releaseWorkflowJob struct {
	Env   map[string]string     `yaml:"env"`
	Steps []releaseWorkflowStep `yaml:"steps"`
}

type releaseWorkflow struct {
	Env  map[string]string             `yaml:"env"`
	Jobs map[string]releaseWorkflowJob `yaml:"jobs"`
}

func releaseStepEnv(
	workflow releaseWorkflow,
	job releaseWorkflowJob,
	step releaseWorkflowStep,
) map[string]string {
	env := make(map[string]string)
	for key, value := range workflow.Env {
		env[key] = value
	}
	for key, value := range job.Env {
		env[key] = value
	}
	for key, value := range step.Env {
		env[key] = value
	}
	return env
}

// TestReleaseValidationSeparatesServicesFromRace keeps the tagged release
// gate aligned with the two Linux jobs in go-test.yml: the service-backed
// suite covers PostgreSQL and MySQL without race instrumentation, while the
// race suite covers the same packages with those optional backends disabled.
// Running all three conformance backends under the race detector exceeds the
// package timeout without identifying a stuck test.
func TestReleaseValidationSeparatesServicesFromRace(t *testing.T) {
	root := repoRoot(t)
	raw := readRepoFile(t, root, publishWorkflow)
	var workflow releaseWorkflow
	if err := yaml.Unmarshal([]byte(raw), &workflow); err != nil {
		t.Fatalf("parse %s: %v", publishWorkflow, err)
	}

	job, ok := workflow.Jobs["ci"]
	if !ok {
		t.Fatalf("%s has no ci job", publishWorkflow)
	}

	serviceRun := false
	raceRun := false
	for _, step := range job.Steps {
		if !strings.Contains(step.Run, "go test") ||
			!strings.Contains(step.Run, "./...") {
			continue
		}
		env := releaseStepEnv(workflow, job, step)
		if strings.Contains(step.Run, "-race") {
			raceRun = true
			for _, key := range releaseServiceTestTriggers {
				if env[key] != "" {
					t.Errorf(
						"release race step %q exposes %s; run service-backed conformance without -race",
						step.Name,
						key,
					)
				}
			}
			continue
		}

		if env["POSTGRES_PASSWORD"] != "" &&
			env["MYSQL_ROOT_PASSWORD"] != "" &&
			env["DINGO_TEST_S3_BUCKET"] != "" {
			serviceRun = true
		}
	}

	if !serviceRun {
		t.Errorf(
			"%s ci job has no service-backed uninstrumented full test run",
			publishWorkflow,
		)
	}
	if !raceRun {
		t.Errorf("%s ci job has no full race test run", publishWorkflow)
	}
}
