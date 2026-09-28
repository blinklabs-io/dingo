//go:build !windows

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

package bin_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

const (
	entrypointChildKindEnv = "DINGO_TEST_ENTRYPOINT_CHILD_KIND"
	bootstrapChild         = "bootstrap"
	serveChild             = "serve"
)

// TestMain turns a re-executed copy of this test binary into the fake dingo
// process used below. Using a real Go process keeps the signal behavior the
// same as the production binary, including os/signal enabling SIGINT for a
// process that the non-interactive entrypoint shell starts in the background.
func TestMain(m *testing.M) {
	if childKind := os.Getenv(entrypointChildKindEnv); childKind != "" {
		os.Exit(runEntrypointChild(childKind))
	}
	os.Exit(m.Run())
}

func runEntrypointChild(kind string) int {
	readyFile := os.Getenv(
		"DINGO_TEST_" + strings.ToUpper(kind) + "_READY_FILE",
	)
	if kind == bootstrapChild && os.Getenv("DINGO_TEST_BOOTSTRAP_WAIT") == "" {
		if err := os.WriteFile(readyFile, []byte("ready\n"), 0o600); err != nil {
			return 125
		}
		return 0
	}

	signals := make(chan os.Signal, 1)
	signalNotify(signals)
	defer signal.Stop(signals)
	if err := os.WriteFile(readyFile, []byte("ready\n"), 0o600); err != nil {
		return 125
	}
	received := <-signals

	signalName := received.String()
	if received == syscall.SIGINT {
		signalName = "SIGINT"
	} else if received == syscall.SIGTERM {
		signalName = "SIGTERM"
	}
	signalFile := os.Getenv(
		"DINGO_TEST_" + strings.ToUpper(kind) + "_SIGNAL_FILE",
	)
	if err := os.WriteFile(signalFile, []byte(signalName+"\n"), 0o600); err != nil {
		return 125
	}

	exitCode, err := strconv.Atoi(
		os.Getenv("DINGO_TEST_" + strings.ToUpper(kind) + "_EXIT_CODE"),
	)
	if err != nil {
		return 125
	}
	return exitCode
}

func signalNotify(signals chan os.Signal) {
	// The parent still exercises the real entrypoint; only its dingo child is
	// replaced by this signal-aware test process.
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
}

func TestEntrypointForwardsSignalsDuringMithrilBootstrap(t *testing.T) {
	tests := []struct {
		name        string
		resume      bool
		signal      syscall.Signal
		signalName  string
		childStatus int
	}{
		{
			name:        "first run SIGTERM",
			signal:      syscall.SIGTERM,
			signalName:  "SIGTERM",
			childStatus: 37,
		},
		{
			name:        "resumed bootstrap SIGINT",
			resume:      true,
			signal:      syscall.SIGINT,
			signalName:  "SIGINT",
			childStatus: 38,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newEntrypointHarness(t, test.resume)
			harness.env = append(
				harness.env,
				"DINGO_TEST_BOOTSTRAP_WAIT=1",
				"DINGO_TEST_BOOTSTRAP_EXIT_CODE="+strconv.Itoa(
					test.childStatus,
				),
			)
			cmd, done, output := harness.start(t)

			testutil.WaitForCondition(
				t,
				func() bool { return fileExists(harness.bootstrapReadyFile) },
				2*time.Second,
				"Mithril bootstrap child did not become ready",
			)
			require.NoError(t, cmd.Process.Signal(test.signal))

			err := waitForEntrypoint(t, cmd, done, output)
			require.Equal(
				t,
				test.childStatus,
				commandExitCode(t, err),
				output.String(),
			)
			require.Equal(
				t,
				test.signalName+"\n",
				readFile(t, harness.bootstrapSignalFile),
			)
			require.False(
				t,
				fileExists(harness.serveReadyFile),
				"serve must not start after an interrupted bootstrap",
			)
		})
	}
}

func TestEntrypointSignalHandlingSurvivesBootstrapToServeHandoff(t *testing.T) {
	harness := newEntrypointHarness(t, false)
	harness.env = append(harness.env, "DINGO_TEST_SERVE_EXIT_CODE=39")
	cmd, done, output := harness.start(t)

	testutil.WaitForCondition(
		t,
		func() bool { return fileExists(harness.serveReadyFile) },
		2*time.Second,
		"serve child did not become ready after Mithril bootstrap",
	)
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))

	err := waitForEntrypoint(t, cmd, done, output)
	require.Equal(t, 39, commandExitCode(t, err), output.String())
	require.Equal(t, "SIGTERM\n", readFile(t, harness.serveSignalFile))
}

type entrypointHarness struct {
	env                 []string
	bootstrapReadyFile  string
	bootstrapSignalFile string
	serveReadyFile      string
	serveSignalFile     string
}

type entrypointProcess struct {
	done chan struct{}
	err  error
}

func newEntrypointHarness(t *testing.T, resume bool) *entrypointHarness {
	t.Helper()
	root := t.TempDir()
	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o700))

	testBinary, err := os.Executable()
	require.NoError(t, err)
	dingoWrapper := `#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "mithril" && "${2:-}" == "sync" ]]; then
  export DINGO_TEST_ENTRYPOINT_CHILD_KIND=bootstrap
else
  export DINGO_TEST_ENTRYPOINT_CHILD_KIND=serve
fi
exec "${DINGO_TEST_BINARY}"
`
	writeExecutable(t, filepath.Join(fakeBin, "dingo"), dingoWrapper)
	writeExecutable(
		t,
		filepath.Join(fakeBin, "sqlite3"),
		"#!/usr/bin/env bash\nprintf 'in_progress\\n'\n",
	)

	databasePath := filepath.Join(root, "db")
	if resume {
		require.NoError(t, os.Mkdir(databasePath, 0o700))
		require.NoError(
			t,
			os.WriteFile(
				filepath.Join(databasePath, "metadata.sqlite"),
				nil,
				0o600,
			),
		)
	}

	harness := &entrypointHarness{
		bootstrapReadyFile:  filepath.Join(root, "bootstrap.ready"),
		bootstrapSignalFile: filepath.Join(root, "bootstrap.signal"),
		serveReadyFile:      filepath.Join(root, "serve.ready"),
		serveSignalFile:     filepath.Join(root, "serve.signal"),
	}
	harness.env = cleanEnvironment(
		os.Environ(),
		"CARDANO_CONFIG",
		"CARDANO_DATABASE_PATH",
		"CARDANO_NETWORK",
		"DINGO_DEBUG",
		"DINGO_LOG_FILE",
		"DINGO_SOCKET_PATH",
		"PATH",
		"RESTORE_SNAPSHOT",
		entrypointChildKindEnv,
	)
	harness.env = append(
		harness.env,
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"CARDANO_NETWORK=devnet",
		"CARDANO_DATABASE_PATH="+databasePath,
		"DINGO_SOCKET_PATH="+filepath.Join(root, "ipc", "dingo.socket"),
		"RESTORE_SNAPSHOT=1",
		"DINGO_TEST_BINARY="+testBinary,
		"DINGO_TEST_BOOTSTRAP_READY_FILE="+harness.bootstrapReadyFile,
		"DINGO_TEST_BOOTSTRAP_SIGNAL_FILE="+harness.bootstrapSignalFile,
		"DINGO_TEST_SERVE_READY_FILE="+harness.serveReadyFile,
		"DINGO_TEST_SERVE_SIGNAL_FILE="+harness.serveSignalFile,
	)
	return harness
}

func (h *entrypointHarness) start(
	t *testing.T,
) (*exec.Cmd, *entrypointProcess, *bytes.Buffer) {
	t.Helper()
	entrypointPath, err := filepath.Abs("entrypoint.sh")
	require.NoError(t, err)

	cmd := exec.Command("bash", entrypointPath) //nolint:gosec
	cmd.Env = h.env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	output := &bytes.Buffer{}
	cmd.Stdout = output
	cmd.Stderr = output
	require.NoError(t, cmd.Start())

	process := &entrypointProcess{done: make(chan struct{})}
	go func() {
		process.err = cmd.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		// The process group is private to this test. Killing it also bounds the
		// fail-before case, where the old entrypoint dies without forwarding the
		// signal and leaves its bootstrap child running.
		select {
		case <-process.done:
			return
		default:
		}
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-process.done
	})
	return cmd, process, output
}

func waitForEntrypoint(
	t *testing.T,
	cmd *exec.Cmd,
	process *entrypointProcess,
	output *bytes.Buffer,
) error {
	t.Helper()
	select {
	case <-process.done:
		return process.err
	case <-time.After(2 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-process.done
		t.Fatalf(
			"entrypoint did not exit after forwarded signal: %v\n%s",
			process.err,
			output.String(),
		)
		return nil
	}
}

func commandExitCode(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	require.True(
		t,
		errors.As(err, &exitErr),
		"unexpected command error: %v",
		err,
	)
	return exitErr.ExitCode()
}

func writeExecutable(t *testing.T, path, contents string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o700))
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(contents)
}

func cleanEnvironment(env []string, remove ...string) []string {
	removed := make(map[string]struct{}, len(remove))
	for _, key := range remove {
		removed[key] = struct{}{}
	}
	cleaned := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if _, found := removed[key]; !found {
			cleaned = append(cleaned, entry)
		}
	}
	return cleaned
}
