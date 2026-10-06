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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
	); got != 5 {
		t.Errorf("validated release-tag dependency count = %d, want 5", got)
	}
}
