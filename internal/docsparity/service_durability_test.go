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
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// serviceDurability lists, per database image, the server options the CI
// service containers must be started with. The conformance replays reset the
// schema between every vector, so a run issues tens of thousands of
// committing statements and is bound by fsync latency on a slow runner disk.
// The containers are discarded with the job, so nothing here needs to survive
// a crash.
//
// Each option is matched as whole whitespace-separated tokens, so a value
// that merely contains one (`--innodb-file-per-table=OFFLINE`) does not count.
var serviceDurability = []struct {
	imagePrefix string
	options     []string
}{
	{
		imagePrefix: "postgres:",
		options: []string{
			"-c fsync=off",
			"-c synchronous_commit=off",
			"-c full_page_writes=off",
		},
	},
	{
		imagePrefix: "mysql:",
		options: []string{
			"--innodb-file-per-table=OFF",
			"--innodb-flush-log-at-trx-commit=0",
			"--sync-binlog=0",
			"--skip-log-bin",
		},
	},
}

// serviceWorkflowJob is a job reduced to the service containers it declares.
type serviceWorkflowJob struct {
	Services map[string]struct {
		Image   string `yaml:"image"`
		Command string `yaml:"command"`
	} `yaml:"services"`
}

// TestServiceContainersRelaxDurability checks that every PostgreSQL and MySQL
// service container in both pipelines starts with relaxed durability. Both
// files are checked because a run on main must not be slower than the pull
// request that was reviewed; TestPipelineStagesMatch already keeps the job
// definitions themselves identical.
func TestServiceContainersRelaxDurability(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)

	for _, workflow := range []string{prPipeline, publishPipeline} {
		var parsed struct {
			Jobs map[string]serviceWorkflowJob `yaml:"jobs"`
		}
		raw := readRepoFile(t, root, workflow)
		if err := yaml.Unmarshal([]byte(raw), &parsed); err != nil {
			t.Fatalf("parse %s: %v", workflow, err)
		}

		jobNames := make([]string, 0, len(parsed.Jobs))
		for name := range parsed.Jobs {
			jobNames = append(jobNames, name)
		}
		sort.Strings(jobNames)

		seen := make(map[string]int)
		for _, jobName := range jobNames {
			for serviceName, service := range parsed.Jobs[jobName].Services {
				for _, want := range serviceDurability {
					if !strings.HasPrefix(service.Image, want.imagePrefix) {
						continue
					}
					seen[want.imagePrefix]++

					// Padding with spaces turns a substring search into a
					// whole-token match at both ends.
					command := " " +
						strings.Join(strings.Fields(service.Command), " ") +
						" "
					for _, option := range want.options {
						if !strings.Contains(command, " "+option+" ") {
							t.Errorf(
								"%s: job %s service %s (%s) does not set "+
									"%q in its command; the conformance "+
									"replays are fsync-bound on a slow "+
									"runner disk",
								workflow,
								jobName,
								serviceName,
								service.Image,
								option,
							)
						}
					}
				}
			}
		}

		for _, want := range serviceDurability {
			if seen[want.imagePrefix] == 0 {
				t.Errorf(
					"%s declares no %s service, so its durability options "+
						"cannot be checked",
					workflow,
					want.imagePrefix,
				)
			}
		}
	}
}
