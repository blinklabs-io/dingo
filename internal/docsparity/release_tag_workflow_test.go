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
	); got != 5 {
		t.Errorf("validated release-tag dependency count = %d, want 5", got)
	}
}

func TestReleaseConsumerUpdateFailsClosed(t *testing.T) {
	t.Parallel()
	t.Run("missing cardano-up template", func(t *testing.T) {
		dir := t.TempDir()
		requireTestDir(t, filepath.Join(dir, "consumer/packages/dingo"))
		output, err := runConsumerUpdate(t, dir, releaseConsumerUpdateScript(t, "cardano-up"), "cardano-up")
		if err == nil {
			t.Fatalf("consumer update unexpectedly succeeded: %s", output)
		}
		if !strings.Contains(string(output), "No Dingo package manifest is available") {
			t.Fatalf("consumer update error = %q", output)
		}
	})

	t.Run("invalid chart version", func(t *testing.T) {
		dir := t.TempDir()
		chart := filepath.Join(dir, "consumer/charts/dingo/Chart.yaml")
		values := filepath.Join(dir, "consumer/charts/dingo/values.yaml")
		requireTestFile(t, chart, "version: invalid\nappVersion: \"0.1.0\"\n")
		requireTestFile(t, values, "image:\n  tag: \"0.1.0\"\n")
		output, err := runConsumerUpdate(t, dir, releaseConsumerUpdateScript(t, "helm"), "helm")
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
		output, err := runConsumerUpdate(t, dir, releaseConsumerUpdateScript(t, "helm"), "helm")
		if err != nil {
			t.Fatalf("consumer update: %v: %s", err, output)
		}
		if got := readRepoFile(t, dir, "consumer/charts/dingo/Chart.yaml"); got != "version: 0.3.3\nappVersion: \"1.2.3\"\n" {
			t.Fatalf("updated Chart.yaml = %q", got)
		}
	})
}

func TestReleaseConsumerUpdateContinuesRemoteBranch(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	remote := filepath.Join(root, "consumer.git")
	seed := filepath.Join(root, "seed")
	consumer := filepath.Join(root, "consumer")

	releaseRunGit(t, root, "init", "--bare", "--initial-branch=main", remote)
	releaseRunGit(t, root, "init", "--initial-branch=main", seed)
	releaseRunGit(t, seed, "config", "user.name", "release test")
	releaseRunGit(t, seed, "config", "user.email", "release-test@example.com")
	requireTestFile(t, filepath.Join(seed, "consumer.txt"), "main\n")
	requireTestFile(
		t,
		filepath.Join(seed, "roles/dingo/defaults/main.yml"),
		"dingo_version: '1.2.1'\n",
	)
	releaseRunGit(t, seed, "add", ".")
	releaseRunGit(t, seed, "commit", "-m", "initial consumer")
	releaseRunGit(t, seed, "remote", "add", "origin", remote)
	releaseRunGit(t, seed, "push", "-u", "origin", "main")
	baseCommit := strings.TrimSpace(releaseRunGit(t, seed, "rev-parse", "HEAD"))
	releaseRunGit(t, seed, "switch", "-c", "dingo-v1.2.3")
	requireTestFile(t, filepath.Join(seed, "release.txt"), "existing branch\n")
	requireTestFile(
		t,
		filepath.Join(seed, "roles/dingo/defaults/main.yml"),
		"dingo_version: '1.2.2'\n",
	)
	releaseRunGit(t, seed, "add", ".")
	releaseRunGit(t, seed, "commit", "-m", "existing release update")
	releaseRunGit(t, seed, "push", "-u", "origin", "dingo-v1.2.3")
	releaseRunGit(t, root, "clone", remote, consumer)

	outputFile := filepath.Join(root, "github-output")
	cmd := exec.Command("bash", "-c", releaseConsumerStepScript(
		t,
		"ansible",
		"Prepare consumer release branch",
	))
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"RELEASE_TAG=v1.2.3",
		"GITHUB_OUTPUT="+outputFile,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("prepare consumer branch: %v: %s", err, output)
	}

	if got := strings.TrimSpace(releaseRunGit(t, consumer, "branch", "--show-current")); got != "dingo-v1.2.3" {
		t.Fatalf("checked out branch = %q", got)
	}
	remoteHead := strings.TrimSpace(releaseRunGit(
		t,
		root,
		"--git-dir="+remote,
		"rev-parse",
		"refs/heads/dingo-v1.2.3",
	))
	if got := strings.TrimSpace(releaseRunGit(t, consumer, "rev-parse", "HEAD")); got != remoteHead {
		t.Fatalf("consumer HEAD = %s, want existing remote head %s", got, remoteHead)
	}
	if got := readRepoFile(t, consumer, "release.txt"); got != "existing branch\n" {
		t.Fatalf("existing release branch content = %q", got)
	}

	openScript := releaseConsumerStepScript(t, "ansible", "Open consumer pull request")
	for _, forbidden := range []string{"git switch -C", "git push --force-with-lease"} {
		if strings.Contains(openScript, forbidden) {
			t.Errorf("consumer publication still rewrites branch with %q", forbidden)
		}
	}
	if !strings.Contains(openScript, `git push -u origin "$RELEASE_BRANCH"`) {
		t.Error("consumer publication does not use a normal upstream push")
	}

	if output, err := runConsumerUpdate(
		t,
		root,
		releaseConsumerUpdateScript(t, "ansible"),
		"ansible",
	); err != nil {
		t.Fatalf("update existing consumer branch: %v: %s", err, output)
	}
	fakeBin := filepath.Join(root, "bin")
	ghLog := filepath.Join(root, "gh.log")
	requireTestFile(t, filepath.Join(fakeBin, "gh"), "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$GH_LOG\"\n")
	if err := os.Chmod(filepath.Join(fakeBin, "gh"), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command("bash", "-c", openScript)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"GH_LOG="+ghLog,
		"BASE_COMMIT="+baseCommit,
		"RELEASE_BRANCH=dingo-v1.2.3",
		"RELEASE_TAG=v1.2.3",
		"PACKAGE_VERSION=1.2.3",
		"CONSUMER_KIND=ansible",
		"CONSUMER_REPOSITORY=example/consumer",
		"GITHUB_REPOSITORY=blinklabs-io/dingo",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("publish continued consumer branch: %v: %s", err, output)
	}
	newRemoteHead := strings.TrimSpace(releaseRunGit(
		t,
		root,
		"--git-dir="+remote,
		"rev-parse",
		"refs/heads/dingo-v1.2.3",
	))
	if newRemoteHead == remoteHead {
		t.Fatal("consumer release branch did not advance")
	}
	releaseRunGit(t, root, "--git-dir="+remote, "merge-base", "--is-ancestor", remoteHead, newRemoteHead)
	if got := releaseRunGit(
		t,
		root,
		"--git-dir="+remote,
		"show",
		"refs/heads/dingo-v1.2.3:roles/dingo/defaults/main.yml",
	); got != "dingo_version: '1.2.3'\n" {
		t.Fatalf("published consumer version = %q", got)
	}
	if got := readRepoFile(t, root, "gh.log"); !strings.Contains(got, "pr create") {
		t.Fatalf("rerun did not recover the missing consumer PR: %q", got)
	}
}

func TestHomebrewUpdateContinuesRemoteBranch(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	remote := filepath.Join(root, "tap.git")
	seed := filepath.Join(root, "seed")
	tap := filepath.Join(root, "homebrew-tap")

	releaseRunGit(t, root, "init", "--bare", "--initial-branch=main", remote)
	releaseRunGit(t, root, "init", "--initial-branch=main", seed)
	releaseRunGit(t, seed, "config", "user.name", "release test")
	releaseRunGit(t, seed, "config", "user.email", "release-test@example.com")
	requireTestFile(t, filepath.Join(seed, "Formula/dingo.rb"), `class Dingo < Formula
  url "https://example.com/dingo-v1.2.1.tar.gz"
  sha256 "old"
  ldflags "-X version.CommitHash=1111111"
end
`)
	releaseRunGit(t, seed, "add", ".")
	releaseRunGit(t, seed, "commit", "-m", "initial tap")
	releaseRunGit(t, seed, "remote", "add", "origin", remote)
	releaseRunGit(t, seed, "push", "-u", "origin", "main")
	baseCommit := strings.TrimSpace(releaseRunGit(t, seed, "rev-parse", "HEAD"))
	releaseRunGit(t, seed, "switch", "-c", "dingo-v1.2.3")
	requireTestFile(t, filepath.Join(seed, "release.txt"), "existing branch\n")
	releaseRunGit(t, seed, "add", ".")
	releaseRunGit(t, seed, "commit", "-m", "existing tap update")
	releaseRunGit(t, seed, "push", "-u", "origin", "dingo-v1.2.3")
	releaseRunGit(t, root, "clone", remote, tap)

	outputFile := filepath.Join(root, "github-output")
	cmd := exec.Command("bash", "-c", releaseWorkflowStepScript(
		t,
		"update-homebrew",
		"Prepare tap release branch",
	))
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"RELEASE_TAG=v1.2.3",
		"GITHUB_OUTPUT="+outputFile,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("prepare tap branch: %v: %s", err, output)
	}
	remoteHead := strings.TrimSpace(releaseRunGit(
		t,
		root,
		"--git-dir="+remote,
		"rev-parse",
		"refs/heads/dingo-v1.2.3",
	))
	if got := strings.TrimSpace(releaseRunGit(t, tap, "rev-parse", "HEAD")); got != remoteHead {
		t.Fatalf("tap HEAD = %s, want existing remote head %s", got, remoteHead)
	}

	updateScript := releaseWorkflowStepScript(t, "update-homebrew", "Update formula")
	cmd = exec.Command("bash", "-c", updateScript)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"TARBALL_URL=https://example.com/dingo-v1.2.3.tar.gz",
		"SHA256=new-sha",
		"SHORT_COMMIT=3333333",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("update existing tap branch: %v: %s", err, output)
	}

	openScript := releaseWorkflowStepScript(t, "update-homebrew", "Open tap pull request")
	for _, forbidden := range []string{"git switch -C", "git push --force-with-lease"} {
		if strings.Contains(openScript, forbidden) {
			t.Errorf("tap publication still rewrites branch with %q", forbidden)
		}
	}
	if !strings.Contains(openScript, `git push -u origin "$RELEASE_BRANCH"`) {
		t.Error("tap publication does not use a normal upstream push")
	}
	fakeBin := filepath.Join(root, "bin")
	ghLog := filepath.Join(root, "gh.log")
	requireTestFile(t, filepath.Join(fakeBin, "gh"), "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$GH_LOG\"\n")
	if err := os.Chmod(filepath.Join(fakeBin, "gh"), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command("bash", "-c", openScript)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"GH_LOG="+ghLog,
		"BASE_COMMIT="+baseCommit,
		"RELEASE_BRANCH=dingo-v1.2.3",
		"RELEASE_TAG=v1.2.3",
		"TAP_REPO=example/tap",
		"GITHUB_REPOSITORY=blinklabs-io/dingo",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("publish continued tap branch: %v: %s", err, output)
	}
	newRemoteHead := strings.TrimSpace(releaseRunGit(
		t,
		root,
		"--git-dir="+remote,
		"rev-parse",
		"refs/heads/dingo-v1.2.3",
	))
	if newRemoteHead == remoteHead {
		t.Fatal("tap release branch did not advance")
	}
	releaseRunGit(t, root, "--git-dir="+remote, "merge-base", "--is-ancestor", remoteHead, newRemoteHead)
	if got := releaseRunGit(
		t,
		root,
		"--git-dir="+remote,
		"show",
		"refs/heads/dingo-v1.2.3:Formula/dingo.rb",
	); !strings.Contains(got, "dingo-v1.2.3.tar.gz") ||
		!strings.Contains(got, `sha256 "new-sha"`) ||
		!strings.Contains(got, "version.CommitHash=3333333") {
		t.Fatalf("published formula is stale: %q", got)
	}
	if got := readRepoFile(t, root, "gh.log"); !strings.Contains(got, "pr create") {
		t.Fatalf("rerun did not recover the missing tap PR: %q", got)
	}
}

func releaseConsumerJob(kind string) string {
	switch kind {
	case "ansible":
		return "update-ansible"
	case "cardano-up":
		return "update-cardano-up-package"
	case "helm":
		return "update-helm-chart"
	default:
		return ""
	}
}

func releaseConsumerUpdateScript(t *testing.T, kind string) string {
	t.Helper()
	job := releaseConsumerJob(kind)
	if job == "" {
		t.Fatalf("unknown consumer kind %q", kind)
	}
	name := map[string]string{
		"ansible":    "Update Ansible role version",
		"cardano-up": "Add cardano-up package version",
		"helm":       "Update Helm chart version",
	}[kind]
	if name == "" {
		t.Fatalf("unknown consumer kind %q", kind)
	}
	return releaseConsumerStepScript(t, kind, name)
}

func releaseConsumerStepScript(t *testing.T, kind, name string) string {
	t.Helper()
	return releaseWorkflowStepScript(t, releaseConsumerJob(kind), name)
}

func releaseWorkflowStepScript(t *testing.T, job, name string) string {
	t.Helper()
	var workflow releaseWorkflow
	if err := yaml.Unmarshal(
		[]byte(readRepoFile(t, repoRoot(t), publishWorkflow)),
		&workflow,
	); err != nil {
		t.Fatalf("parse %s: %v", publishWorkflow, err)
	}
	for _, step := range workflow.Jobs[job].Steps {
		if step.Name == name {
			return step.Run
		}
	}
	t.Fatalf("%s has no %q step", job, name)
	return ""
}

func releaseRunGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
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
