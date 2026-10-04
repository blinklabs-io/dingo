//go:build linux

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
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunTestsExplicitDockerRuntime(t *testing.T) {
	t.Parallel()
	result := runFakeDevnetWithEnv(t, 0, false, map[string]string{
		"DEVNET_RUNTIME": "container",
	}, "--runtime", "docker", "--accelerated")
	require.Zero(t, result.exitCode, result.output)
	require.Contains(t, result.dockerLog, "run-tests\n")
}

func TestAppleContainerRuntime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		exitCode int
		keepUp   bool
	}{
		{name: "success"},
		{name: "failure", exitCode: 23},
		{name: "keep", keepUp: true},
		{name: "failed-keep", exitCode: 23, keepUp: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := filepath.Join(t.TempDir(), "checkout with spaces")
			dir := filepath.Join(root, "internal", "test", "devnet")
			bin := filepath.Join(root, "bin")
			require.NoError(t, os.MkdirAll(dir, 0o755))
			require.NoError(t, os.MkdirAll(bin, 0o755))
			for _, name := range []string{"run-tests.sh", "run-tests-container.sh"} {
				contents, err := os.ReadFile(name)
				require.NoError(t, err)
				writeExecutable(t, filepath.Join(dir, name), string(contents))
			}
			writeExecutable(t, filepath.Join(bin, "uname"), `#!/usr/bin/env bash
case "$1" in -s) echo Darwin ;; -m) echo arm64 ;; esac
`)
			writeExecutable(
				t,
				filepath.Join(bin, "container"),
				`#!/usr/bin/env bash
printf '%q ' "$@" >>"$FAKE_CONTAINER_LOG"
printf '\n' >>"$FAKE_CONTAINER_LOG"
case "$1" in
  inspect) exit 1 ;;
  exec)
    case " $* " in
      *" bash internal/test/devnet/run-tests.sh "*) exit "$FAKE_CONTAINER_EXIT" ;;
    esac ;;
  stop) exit 42 ;;
esac
`,
			)
			logFile := filepath.Join(root, "container.log")
			args := []string{
				filepath.Join(dir, "run-tests.sh"),
				"--runtime=container",
				"--accelerated",
				"-run",
				"TestOne|TestTwo",
			}
			if tc.keepUp {
				args = append(args, "--keep-up")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", args...)
			cmd.Env = []string{
				"PATH=" + bin + ":" + os.Getenv("PATH"),
				"HOME=" + t.TempDir(),
				"FAKE_CONTAINER_LOG=" + logFile,
				"FAKE_CONTAINER_EXIT=" + strconv.Itoa(tc.exitCode),
				"DEVNET_MEMPOOL_PROVIDER=dag",
				"DEVNET_RUNTIME=docker",
			}
			output, err := cmd.CombinedOutput()
			exitCode := 0
			if err != nil {
				var exitErr *exec.ExitError
				require.True(t, errors.As(err, &exitErr), "%s", output)
				exitCode = exitErr.ExitCode()
			}
			require.Equal(t, tc.exitCode, exitCode, "%s", output)
			data, err := os.ReadFile(logFile)
			require.NoError(t, err)
			log := string(data)
			require.Contains(t, log, "DEVNET_MEMPOOL_PROVIDER=dag")
			require.Contains(t, log, "DEVNET_RUNTIME=docker")
			require.Contains(t, log, "GOTOOLCHAIN=auto")
			require.Contains(t, log, `--accelerated -run TestOne\|TestTwo`)
			require.NotContains(t, log, "--runtime")
			require.Equal(
				t,
				!tc.keepUp || tc.exitCode != 0,
				strings.Contains(log, "stop --time"),
			)
			_, err = os.Stat(
				filepath.Join(root, ".devnet", "apple-container", "lock"),
			)
			require.True(t, os.IsNotExist(err), "runner lock must be released")
		})
	}
}
