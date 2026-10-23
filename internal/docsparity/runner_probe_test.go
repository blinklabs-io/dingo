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
)

// runnerProbe is the script every test job runs at its start and end. A red
// or slow test job is otherwise unexplained: runner speed varies several-fold
// by host, and the job log does not say which host it got.
const runnerProbe = ".github/scripts/runner-probe.py"

// TestEveryTestJobProbesTheRunner checks every test job in both workflows,
// including a publish job if it gains tests later.
func TestEveryTestJobProbesTheRunner(t *testing.T) {
	root := repoRoot(t)
	for _, workflow := range []string{prPipeline, publishPipeline} {
		jobs := pipelineJobs(t, root, workflow)
		testJobs := 0
		for name, rawJob := range jobs {
			job, ok := rawJob.(map[string]any)
			if !ok {
				continue
			}
			steps, _ := job["steps"].([]any)
			start, end, firstTest, lastTest := -1, -1, -1, -1
			endAlways := false
			for i, raw := range steps {
				step, _ := raw.(map[string]any)
				run, _ := step["run"].(string)
				switch {
				case strings.Contains(run, runnerProbe+" start"):
					start = i
				case strings.Contains(run, runnerProbe+" end"):
					end = i
					endAlways = step["if"] == "always()"
				case strings.Contains(run, "go test") ||
					strings.Contains(run, "make test"):
					if firstTest < 0 {
						firstTest = i
					}
					lastTest = i
				}
			}
			if firstTest < 0 {
				continue
			}
			testJobs++
			switch {
			case start < 0 || start > firstTest:
				t.Errorf(
					"%s job %s must run %s start before its first test step",
					workflow, name, runnerProbe,
				)
			case end < 0 || end < lastTest || !endAlways:
				t.Errorf(
					"%s job %s must run %s end with if: always() after its "+
						"last test step",
					workflow, name, runnerProbe,
				)
			}
		}
		if workflow == prPipeline && testJobs == 0 {
			t.Errorf("%s has no test jobs", workflow)
		}
	}
}
