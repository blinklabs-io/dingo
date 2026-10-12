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
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	entrypointChildKindEnv = "DINGO_TEST_ENTRYPOINT_CHILD_KIND"
	bootstrapChild         = "bootstrap"
	serveChild             = "serve"

	// entrypointStepTimeout bounds each step the tests wait for: a child
	// becoming ready and the entrypoint exiting after a forwarded signal.
	// Both spawn real processes, so a loaded or race-instrumented runner can
	// take well past a couple of seconds. It is only ever reached on failure.
	entrypointStepTimeout = 30 * time.Second

	// entrypointChildDelayEnv makes the fake child stall before signalling
	// ready and before exiting, standing in for a starved runner.
	entrypointChildDelayEnv = "DINGO_TEST_ENTRYPOINT_CHILD_DELAY"

	// entrypointChildHangEnv makes the fake child start and then never become
	// ready, standing in for a child that is wedged after spawning.
	entrypointChildHangEnv = "DINGO_TEST_ENTRYPOINT_CHILD_HANG"
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
	startedFile := os.Getenv(
		"DINGO_TEST_" + strings.ToUpper(kind) + "_STARTED_FILE",
	)
	if err := os.WriteFile(startedFile, []byte("started\n"), 0o600); err != nil {
		return 125
	}
	childDelay, _ := time.ParseDuration(os.Getenv(entrypointChildDelayEnv))
	if os.Getenv(entrypointChildHangEnv) != "" {
		// Started but never ready: wait to be killed by the harness. Not
		// select{}: without cgo (the darwin default) the runtime reports that
		// as a deadlock and exits 2, so the child dies instead of hanging.
		for {
			time.Sleep(time.Hour)
		}
	}
	if kind == bootstrapChild && os.Getenv("DINGO_TEST_BOOTSTRAP_WAIT") == "" {
		if err := os.WriteFile(readyFile, []byte("ready\n"), 0o600); err != nil {
			return 125
		}
		return 0
	}

	signals := make(chan os.Signal, 1)
	signalNotify(signals)
	defer signal.Stop(signals)
	time.Sleep(childDelay)
	if err := os.WriteFile(readyFile, []byte("ready\n"), 0o600); err != nil {
		return 125
	}
	received := <-signals
	time.Sleep(childDelay)

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
			_, process, output := harness.start(t)

			harness.requireBootstrapReady(t, process, output)
			require.NoError(t, process.cmd.Process.Signal(test.signal))

			err := waitForEntrypoint(t, process.cmd, process, output)
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
				fileExists(harness.serveStartedFile),
				"serve must not start after an interrupted bootstrap",
			)
			require.False(
				t,
				fileExists(harness.serveReadyFile),
				"serve must not become ready after an interrupted bootstrap",
			)
		})
	}
}

// A child that is slow to start and slow to exit, as on a starved runner, must
// not fail the forwarding assertions: only a missing forward should.
func TestEntrypointForwardsSignalsToSlowChild(t *testing.T) {
	t.Parallel()

	harness := newEntrypointHarness(t, false)
	harness.env = append(
		harness.env,
		"DINGO_TEST_BOOTSTRAP_WAIT=1",
		"DINGO_TEST_BOOTSTRAP_EXIT_CODE=41",
		entrypointChildDelayEnv+"=2500ms",
	)
	_, process, output := harness.start(t)

	harness.requireBootstrapReady(t, process, output)
	require.NoError(t, process.cmd.Process.Signal(syscall.SIGTERM))

	err := waitForEntrypoint(t, process.cmd, process, output)
	require.Equal(t, 41, commandExitCode(t, err), output.String())
	require.Equal(
		t, "SIGTERM\n", readFile(t, harness.bootstrapSignalFile),
	)
}

func TestEntrypointSignalHandlingSurvivesBootstrapToServeHandoff(t *testing.T) {
	harness := newEntrypointHarness(t, false)
	harness.env = append(harness.env, "DINGO_TEST_SERVE_EXIT_CODE=39")
	_, process, output := harness.start(t)

	harness.requireServeReady(t, process, output)
	require.NoError(t, process.cmd.Process.Signal(syscall.SIGTERM))

	err := waitForEntrypoint(t, process.cmd, process, output)
	require.Equal(t, 39, commandExitCode(t, err), output.String())
	require.Equal(t, "SIGTERM\n", readFile(t, harness.serveSignalFile))
}

// A child that spawns and then wedges must fail the readiness wait, and the
// failure must say it started, so the stage is not misread as a spawn stall.
func TestEntrypointReadinessWaitFailsForChildThatNeverBecomesReady(
	t *testing.T,
) {
	t.Parallel()

	harness := newEntrypointHarness(t, false)
	harness.env = append(
		harness.env,
		"DINGO_TEST_BOOTSTRAP_WAIT=1",
		entrypointChildHangEnv+"=1",
	)
	_, process, _ := harness.start(t)

	err := waitForEntrypointChildReady(
		process,
		harness.bootstrapStartedFile,
		harness.bootstrapReadyFile,
		entrypointStepTimeout,
		300*time.Millisecond,
		"Mithril bootstrap",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "started but did not become ready")
	require.False(t, fileExists(harness.bootstrapReadyFile))
}

type entrypointHarness struct {
	env                  []string
	bootstrapStartedFile string
	bootstrapReadyFile   string
	bootstrapSignalFile  string
	serveStartedFile     string
	serveReadyFile       string
	serveSignalFile      string
}

func (h *entrypointHarness) requireBootstrapReady(
	t *testing.T, process *entrypointProcess, output *bytes.Buffer,
) {
	t.Helper()
	requireEntrypointChildReady(
		t,
		process,
		output,
		h.bootstrapStartedFile,
		h.bootstrapReadyFile,
		entrypointStepTimeout,
		entrypointStepTimeout,
		"Mithril bootstrap",
	)
}

func (h *entrypointHarness) requireServeReady(
	t *testing.T, process *entrypointProcess, output *bytes.Buffer,
) {
	t.Helper()
	requireEntrypointChildReady(
		t,
		process,
		output,
		h.serveStartedFile,
		h.serveReadyFile,
		entrypointStepTimeout,
		entrypointStepTimeout,
		"serve",
	)
}

type entrypointProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func TestWaitForEntrypointChildReady(t *testing.T) {
	t.Parallel()
	exitErr := errors.New("exit status 7")
	tests := []struct {
		name          string
		started       bool
		ready         bool
		exited        bool
		wantErrorPart string
	}{
		{
			name:          "child never started",
			wantErrorPart: "bootstrap child did not start within",
		},
		{
			name:          "child started but never became ready",
			started:       true,
			wantErrorPart: "bootstrap child started but did not become ready",
		},
		{
			name:          "entrypoint exited before the child started",
			exited:        true,
			wantErrorPart: "bootstrap child did not start: entrypoint exited: exit status 7",
		},
		{
			name:          "entrypoint exited before the child became ready",
			started:       true,
			exited:        true,
			wantErrorPart: "bootstrap child started but exited before becoming ready: exit status 7",
		},
		{
			name:    "child became ready as the entrypoint exited",
			started: true,
			ready:   true,
			exited:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			startedFile := filepath.Join(dir, "child.started")
			readyFile := filepath.Join(dir, "child.ready")
			if test.started {
				require.NoError(t, os.WriteFile(startedFile, []byte("started\n"), 0o600))
			}
			if test.ready {
				require.NoError(t, os.WriteFile(readyFile, []byte("ready\n"), 0o600))
			}
			process := &entrypointProcess{done: make(chan struct{})}
			if test.exited {
				process.err = exitErr
				close(process.done)
			}
			err := waitForEntrypointChildReady(
				process,
				startedFile,
				readyFile,
				10*time.Millisecond,
				10*time.Millisecond,
				"bootstrap",
			)
			if test.wantErrorPart == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErrorPart)
		})
	}
}

// waitForEntrypointChildReady waits for the child to start and then to write
// its ready file, each against its own budget, so a timeout names the stage
// that stalled: spawn queueing on a loaded runner is charged to startBudget
// only, and readyBudget begins once the child is observed running. The ready
// file is re-checked when the entrypoint exits or a deadline fires, because
// the child can write it in the same window: a ready child must not be
// reported as one that never became ready.
func waitForEntrypointChildReady(
	process *entrypointProcess,
	startedFile string,
	readyFile string,
	startBudget time.Duration,
	readyBudget time.Duration,
	name string,
) error {
	deadline := time.NewTimer(startBudget)
	defer func() { deadline.Stop() }()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	running := false
	for {
		select {
		case <-process.done:
			if fileExists(readyFile) {
				return nil
			}
			if !fileExists(startedFile) {
				return fmt.Errorf("%s child did not start: entrypoint exited: %v", name, process.err)
			}
			return fmt.Errorf("%s child started but exited before becoming ready: %v", name, process.err)
		case <-deadline.C:
			if fileExists(readyFile) {
				return nil
			}
			if !running && !fileExists(startedFile) {
				return fmt.Errorf("%s child did not start within %s", name, startBudget)
			}
			return fmt.Errorf("%s child started but did not become ready within %s", name, readyBudget)
		case <-poll.C:
			if fileExists(readyFile) {
				return nil
			}
			if !running && fileExists(startedFile) {
				running = true
				deadline.Stop()
				deadline = time.NewTimer(readyBudget)
			}
		}
	}
}

// requireEntrypointChildReady fails the test with the entrypoint's captured
// output when the child never becomes ready. The entrypoint is stopped before
// the output is read, since its copy goroutine is still writing to it.
func requireEntrypointChildReady(
	t *testing.T,
	process *entrypointProcess,
	output *bytes.Buffer,
	startedFile string,
	readyFile string,
	startBudget time.Duration,
	readyBudget time.Duration,
	name string,
) {
	t.Helper()
	err := waitForEntrypointChildReady(
		process,
		startedFile,
		readyFile,
		startBudget,
		readyBudget,
		name,
	)
	if err == nil {
		return
	}
	_ = syscall.Kill(-process.cmd.Process.Pid, syscall.SIGKILL)
	<-process.done
	require.NoError(t, err, output.String())
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
		bootstrapStartedFile: filepath.Join(root, "bootstrap.started"),
		bootstrapReadyFile:   filepath.Join(root, "bootstrap.ready"),
		bootstrapSignalFile:  filepath.Join(root, "bootstrap.signal"),
		serveStartedFile:     filepath.Join(root, "serve.started"),
		serveReadyFile:       filepath.Join(root, "serve.ready"),
		serveSignalFile:      filepath.Join(root, "serve.signal"),
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
		"DINGO_TEST_BOOTSTRAP_STARTED_FILE="+harness.bootstrapStartedFile,
		"DINGO_TEST_BOOTSTRAP_READY_FILE="+harness.bootstrapReadyFile,
		"DINGO_TEST_BOOTSTRAP_SIGNAL_FILE="+harness.bootstrapSignalFile,
		"DINGO_TEST_SERVE_STARTED_FILE="+harness.serveStartedFile,
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

	process := &entrypointProcess{cmd: cmd, done: make(chan struct{})}
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
	case <-time.After(entrypointStepTimeout):
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
