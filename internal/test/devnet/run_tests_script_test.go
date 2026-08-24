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

package devnet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fakeDockerScript = `#!/usr/bin/env bash
set -euo pipefail

printf '%q ' "$@" >>"${FAKE_DOCKER_LOG}"
printf '\n' >>"${FAKE_DOCKER_LOG}"

case "${1:-}" in
  inspect)
    printf 'healthy\n'
    ;;
  volume)
    # The stake-key volume exists. An empty response to "volume ls" also
    # prevents the failure-artifact path from trying to copy a config volume.
    ;;
  compose)
    case " $* " in
      *" ps --status running --quiet "*) printf 'fake-container\n' ;;
      *" exec -T "*) printf '1 0 0\n' ;;
    esac
    ;;
  run)
    shift
    host_user=false
    output_dir=''
    while (( $# > 0 )); do
      case "$1" in
        --user)
          host_user=true
          shift 2
          ;;
        -v)
          mount="$2"
          if [[ "${mount}" == *:/out ]]; then
            output_dir="${mount%:/out}"
          fi
          shift 2
          ;;
        *) shift ;;
      esac
    done
    if [[ -n "${output_dir}" ]]; then
      mkdir -p "${output_dir}/stake"
      printf 'fake stake key\n' >"${output_dir}/stake/genesis.skey"
      if [[ "${host_user}" != "true" ]]; then
        # Model a root-created container directory that the host user can read
        # but cannot remove recursively.
        chmod 0555 "${output_dir}/stake"
      fi
    fi
    ;;
esac
`

const fakeGoScript = `#!/usr/bin/env bash
exit "${FAKE_GO_EXIT}"
`

const fakeIDScript = `#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  -u) printf '1234\n' ;;
  -g) printf '5678\n' ;;
  *) exit 2 ;;
esac
`

const failingRmScript = `#!/usr/bin/env bash
exit 42
`

type fakeDevnetResult struct {
	exitCode     int
	output       string
	dockerLog    string
	stakeDirs    []string
	artifactDirs []string
}

func TestRunTestsPreservesTestStatusWhenCleanupFails(t *testing.T) {
	for _, test := range []struct {
		name     string
		testExit int
	}{
		{name: "success", testExit: 0},
		{name: "failure", testExit: 23},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := runFakeDevnet(t, test.testExit, true)
			assert.Equal(t, test.testExit, result.exitCode, result.output)
		})
	}
}

func TestRunTestsKeepUpPreservesSuccess(t *testing.T) {
	result := runFakeDevnet(t, 0, true, "--keep-up")
	assert.Equal(t, 0, result.exitCode, result.output)
	assert.NotContains(t, result.dockerLog, " down -v",
		"--keep-up should not tear down a passing network")
}

func TestRunTestsCleansContainerCreatedTemporaryFiles(t *testing.T) {
	for _, test := range []struct {
		name              string
		testExit          int
		wantArtifactCount int
	}{
		{name: "success", testExit: 0, wantArtifactCount: 0},
		{name: "failure", testExit: 23, wantArtifactCount: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := runFakeDevnet(t, test.testExit, false)
			assert.Equal(t, test.testExit, result.exitCode, result.output)
			assert.Contains(t, result.dockerLog,
				"run --rm --user 1234:5678",
				"stake-key copy did not use the host uid:gid")
			assert.Empty(t, result.stakeDirs,
				"runner left its stake-key temp tree behind\n%s", result.output)
			assert.Len(t, result.artifactDirs, test.wantArtifactCount,
				"runner did not apply its artifact retention policy\n%s", result.output)
		})
	}
}

func runFakeDevnet(
	t *testing.T,
	testExit int,
	failRm bool,
	runnerArgs ...string,
) fakeDevnetResult {
	t.Helper()

	root := repoRootDir(t)
	tempRoot := t.TempDir()
	// A fail-before run intentionally leaves a read-only directory behind.
	// Restore owner permissions before testing.TempDir performs final cleanup.
	t.Cleanup(func() {
		_ = filepath.Walk(tempRoot, func(path string, info os.FileInfo, err error) error {
			if err == nil && info.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
	})

	fakeBin := filepath.Join(tempRoot, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o700))
	writeExecutable(t, filepath.Join(fakeBin, "docker"), fakeDockerScript)
	writeExecutable(t, filepath.Join(fakeBin, "go"), fakeGoScript)
	writeExecutable(t, filepath.Join(fakeBin, "id"), fakeIDScript)
	if failRm {
		writeExecutable(t, filepath.Join(fakeBin, "rm"), failingRmScript)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args := []string{
		filepath.Join(root, "internal", "test", "devnet", "run-tests.sh"),
	}
	args = append(args, runnerArgs...)
	cmd := exec.CommandContext(ctx, "bash", args...)
	cmd.Dir = root
	cmd.Env = cleanRunnerEnv(map[string]string{
		"FAKE_DOCKER_LOG": filepath.Join(tempRoot, "docker.log"),
		"FAKE_GO_EXIT":    strconv.Itoa(testExit),
		"MODE":            "dingo",
		"PATH":            fakeBin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TMPDIR":          tempRoot,
	})
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("run-tests.sh did not finish:\n%s", output.String())
	}

	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr, output.String())
		exitCode = exitErr.ExitCode()
	}
	stakeDirs, err := filepath.Glob(
		filepath.Join(tempRoot, "dingo-devnet-stake-keys.*"),
	)
	require.NoError(t, err)
	artifactDirs, err := filepath.Glob(
		filepath.Join(tempRoot, "dingo-devnet-artifacts.*"),
	)
	require.NoError(t, err)
	dockerLog, err := os.ReadFile(filepath.Join(tempRoot, "docker.log"))
	require.NoError(t, err)
	return fakeDevnetResult{
		exitCode:     exitCode,
		output:       output.String(),
		dockerLog:    string(dockerLog),
		stakeDirs:    stakeDirs,
		artifactDirs: artifactDirs,
	}
}

func cleanRunnerEnv(overrides map[string]string) []string {
	blocked := map[string]struct{}{
		"COMPOSE_PROFILES":    {},
		"DEVNET_ACCELERATED":  {},
		"DEVNET_ARTIFACT_DIR": {},
		"DEVNET_CIP50_TEST":   {},
		"FAKE_DOCKER_LOG":     {},
		"FAKE_GO_EXIT":        {},
		"MODE":                {},
		"PATH":                {},
		"TMPDIR":              {},
	}
	env := make([]string, 0, len(os.Environ())+len(overrides))
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if _, found := blocked[key]; !found {
			env = append(env, item)
		}
	}
	for key, value := range overrides {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}
	return env
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o700))
}
