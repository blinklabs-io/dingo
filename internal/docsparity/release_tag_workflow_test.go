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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestReleaseTagValidation(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	script := filepath.Join(root, ".github/scripts/validate-release-tag.sh")

	for _, tc := range []struct {
		name    string
		tag     string
		version string
		valid   bool
	}{
		{name: "stable", tag: "v1.2.3", version: "1.2.3", valid: true},
		{
			name:    "prerelease and build",
			tag:     "v1.2.3-rc.1+build.5",
			version: "1.2.3-rc.1+build.5",
			valid:   true,
		},
		{
			name: "command injection",
			tag:  "v1.2.3-#;e${IFS}id;#",
		},
		{name: "leading zero", tag: "v01.2.3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			output, err := exec.Command(script, tc.tag).CombinedOutput()
			if tc.valid {
				if err != nil {
					t.Fatalf("validate %q: %v: %s", tc.tag, err, output)
				}
				if got := strings.TrimSpace(string(output)); got != tc.version {
					t.Fatalf("validate %q = %q, want %q", tc.tag, got, tc.version)
				}
				return
			}
			if err == nil {
				t.Fatalf("validate %q unexpectedly succeeded: %s", tc.tag, output)
			}
		})
	}
}

func TestPrivilegedReleaseJobsNeedValidatedTag(t *testing.T) {
	t.Parallel()
	workflow := readRepoFile(t, repoRoot(t), publishWorkflow)

	for _, fragment := range []string{
		"validate-release-tag:\n",
		"      - validate-release-tag\n",
		"needs: [finalize-release, validate-release-tag]",
		"RELEASE_TAG: ${{ needs.validate-release-tag.outputs.release_tag }}",
		"PACKAGE_VERSION: ${{ needs.validate-release-tag.outputs.package_version }}",
	} {
		if !strings.Contains(workflow, fragment) {
			t.Errorf("publish workflow is missing validated release-tag wiring %q", fragment)
		}
	}
	if got := strings.Count(
		workflow,
		"needs: [finalize-release, validate-release-tag]",
	); got != 2 {
		t.Errorf("validated release-tag dependency count = %d, want 2", got)
	}
}

func TestReleaseConsumerUpdateFailsClosed(t *testing.T) {
	t.Parallel()
	script := releaseConsumerUpdateScript(t)

	t.Run("missing cardano-up template", func(t *testing.T) {
		dir := t.TempDir()
		requireTestDir(t, filepath.Join(dir, "consumer/packages/dingo"))
		output, err := runConsumerUpdate(t, dir, script, "cardano-up")
		if err == nil {
			t.Fatalf("consumer update unexpectedly succeeded: %s", output)
		}
		if !strings.Contains(string(output), "No dingo package templates found") {
			t.Fatalf("consumer update error = %q", output)
		}
	})

	t.Run("invalid chart version", func(t *testing.T) {
		dir := t.TempDir()
		chart := filepath.Join(dir, "consumer/charts/dingo/Chart.yaml")
		values := filepath.Join(dir, "consumer/charts/dingo/values.yaml")
		requireTestFile(t, chart, "version: invalid\nappVersion: \"0.1.0\"\n")
		requireTestFile(t, values, "image:\n  tag: \"0.1.0\"\n")
		output, err := runConsumerUpdate(t, dir, script, "helm")
		if err == nil {
			t.Fatalf("consumer update unexpectedly succeeded: %s", output)
		}
		if !strings.Contains(string(output), "Unsupported chart version") {
			t.Fatalf("consumer update error = %q", output)
		}
		if got := readRepoFile(t, dir, "consumer/charts/dingo/Chart.yaml"); strings.Contains(got, "version: ..1") {
			t.Fatalf("invalid chart version was rewritten: %q", got)
		}
	})

	t.Run("chart version increment", func(t *testing.T) {
		dir := t.TempDir()
		chart := filepath.Join(dir, "consumer/charts/dingo/Chart.yaml")
		values := filepath.Join(dir, "consumer/charts/dingo/values.yaml")
		requireTestFile(t, chart, "version: 0.3.2\nappVersion: \"0.1.0\"\n")
		requireTestFile(t, values, "image:\n  tag: \"0.1.0\"\n")
		output, err := runConsumerUpdate(t, dir, script, "helm")
		if err != nil {
			t.Fatalf("consumer update: %v: %s", err, output)
		}
		if got := readRepoFile(t, dir, "consumer/charts/dingo/Chart.yaml"); got != "version: 0.3.3\nappVersion: \"1.2.3\"\n" {
			t.Fatalf("updated Chart.yaml = %q", got)
		}
	})
}

func releaseConsumerUpdateScript(t *testing.T) string {
	t.Helper()
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(
		[]byte(readRepoFile(t, repoRoot(t), publishWorkflow)),
		&workflow,
	); err != nil {
		t.Fatalf("parse %s: %v", publishWorkflow, err)
	}
	for _, step := range workflow.Jobs["update-consumers"].Steps {
		if step.Name == "Update consumer version" {
			return step.Run
		}
	}
	t.Fatal("update-consumers has no version update step")
	return ""
}

func runConsumerUpdate(
	t *testing.T,
	dir, script, kind string,
) ([]byte, error) {
	t.Helper()
	cmd := exec.Command("bash", "-c", script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"CONSUMER_KIND="+kind,
		"PACKAGE_VERSION=1.2.3",
	)
	return cmd.CombinedOutput()
}

func requireTestDir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
}

func requireTestFile(t *testing.T, path, content string) {
	t.Helper()
	requireTestDir(t, filepath.Dir(path))
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
