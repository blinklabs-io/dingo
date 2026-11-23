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

package plugin

import (
	"context"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/pluginfixture"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"log/slog"
)

const externalTestCapability Capability = pluginfixture.Capability

// externalHarness registers the test plugin on a fresh host and records the
// process it launches.
type externalHarness struct {
	host   *Host
	logs   *syncBuffer
	cmd    atomic.Pointer[exec.Cmd]
	dir    string
	binary string
}

func newExternalHarness(t *testing.T, binary string) *externalHarness {
	t.Helper()
	h := &externalHarness{
		host:   NewHost(),
		logs:   &syncBuffer{},
		binary: binary,
	}
	h.host.SetLogger(slog.New(slog.NewTextHandler(h.logs, nil)))
	require.NoError(t, h.host.Declare(externalTestCapability))
	// Not under t.TempDir: its path can exceed the Unix socket limit.
	dir, err := os.MkdirTemp("", "dt")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	h.dir = dir
	require.NoError(t, RegisterExternal[healthpb.HealthClient, struct{}](
		h.host,
		Descriptor{Capability: externalTestCapability, Name: "testplugin"},
		func(cc grpc.ClientConnInterface) healthpb.HealthClient {
			return healthpb.NewHealthClient(cc)
		},
		WithCommand(func(cfg ExternalConfig) (*exec.Cmd, error) {
			cmd, err := defaultCommand(cfg)
			h.cmd.Store(cmd)
			return cmd, err
		}),
	))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = h.host.Stop(ctx)
	})
	return h
}

func (h *externalHarness) resolve(
	t *testing.T,
	args []string,
	extra map[string]any,
) (healthpb.HealthClient, error) {
	t.Helper()
	cfg := map[string]any{
		"command":           h.binary,
		"args":              args,
		"runtimeDir":        h.dir,
		"startTimeout":      "10s",
		"stopTimeout":       "5s",
		"keepaliveInterval": "100ms",
		"keepaliveTimeout":  "5s",
	}
	maps.Copy(cfg, extra)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	return Resolve[healthpb.HealthClient](
		ctx, h.host, externalTestCapability, "testplugin", cfg, struct{}{},
	)
}

func (h *externalHarness) exited(t *testing.T) bool {
	t.Helper()
	cmd := h.cmd.Load()
	require.NotNil(t, cmd, "plugin command was never constructed")
	return cmd.ProcessState != nil
}

func (h *externalHarness) requireRuntimeDirEmpty(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(h.dir)
	require.NoError(t, err)
	require.Empty(t, entries, "plugin runtime directory was not removed")
}

func requireFailure(t *testing.T, ch <-chan Failure) Failure {
	t.Helper()
	return testutil.RequireReceive(t, ch, 20*time.Second, "plugin failure")
}

func failureHandler(h *Host) <-chan Failure {
	ch := make(chan Failure, 4)
	h.SetFailureHandler(func(f Failure) { ch <- f })
	return ch
}

func stopContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestExternalPlugin(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("external plugins use Unix sockets and signals")
	}
	binary := pluginfixture.Build(t)

	t.Run("launch negotiate use stop", func(t *testing.T) {
		t.Parallel()
		h := newExternalHarness(t, binary)
		client, err := h.resolve(t, nil, nil)
		require.NoError(t, err)

		resp, err := client.Check(
			t.Context(),
			&healthpb.HealthCheckRequest{Service: "echo"},
		)
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

		require.NoError(t, h.host.Stop(stopContext(t)))
		require.True(t, h.exited(t))
		require.True(t, h.cmd.Load().ProcessState.Success())
		h.requireRuntimeDirEmpty(t)
	})

	t.Run("contract version mismatch", func(t *testing.T) {
		t.Parallel()
		h := newExternalHarness(t, binary)
		_, err := h.resolve(t, []string{"-capability=example.widget/v1"}, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, `"example.widget/v1"`)
		require.ErrorContains(t, err, `"example.widget/v0"`)
		require.ErrorContains(t, err, "must match exactly")
		require.True(t, h.exited(t), "mismatched plugin was left running")
		h.requireRuntimeDirEmpty(t)
	})

	t.Run("crash at start", func(t *testing.T) {
		t.Parallel()
		h := newExternalHarness(t, binary)
		_, err := h.resolve(
			t,
			[]string{"-exit-at-start=3"},
			map[string]any{"startTimeout": "5s"},
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "plugin exited during startup")
		// The exit cancels negotiation instead of leaving it to run out its
		// start timeout.
		require.ErrorContains(t, err, "code = Canceled")
		require.ErrorContains(t, err, "exit status 3")
		require.ErrorContains(t, err, "refusing to start")
		h.requireRuntimeDirEmpty(t)
	})

	t.Run("missing executable", func(t *testing.T) {
		t.Parallel()
		h := newExternalHarness(t, filepath.Join(h0(t), "absent"))
		_, err := h.resolve(t, nil, nil)
		require.ErrorContains(t, err, "launch plugin")
		h.requireRuntimeDirEmpty(t)
	})

	t.Run("crash at runtime stops the host in reverse start order", func(t *testing.T) {
		t.Parallel()
		h := newExternalHarness(t, binary)
		const other Capability = "example.other/v0"
		require.NoError(t, h.host.Declare(other))
		var externalGoneAtOtherStop atomic.Bool
		otherStopped := make(chan struct{})
		require.NoError(t, Register(
			h.host,
			Descriptor{Capability: other, Name: "inproc"},
			nil,
			func(context.Context, struct{}, struct{}) (struct{}, Instance, error) {
				return struct{}{}, Lifecycle{StopFunc: func(context.Context) error {
					externalGoneAtOtherStop.Store(h.exited(t))
					close(otherStopped)
					return nil
				}}, nil
			},
		))
		failures := make(chan Failure, 1)
		h.host.SetFailureHandler(func(f Failure) {
			failures <- f
			_ = h.host.Stop(context.Background())
		})
		require.NoError(t, ResolveProvider(
			t.Context(), h.host, other, "inproc", nil, struct{}{},
		))
		// A long interval leaves the exit watch as the only way to notice.
		client, err := h.resolve(t, nil, map[string]any{
			"keepaliveInterval": "1h",
		})
		require.NoError(t, err)

		// The process exits as it answers; the call may fail either way.
		_, _ = client.Check(
			t.Context(),
			&healthpb.HealthCheckRequest{Service: "crash"},
		)

		f := requireFailure(t, failures)
		require.Equal(t, externalTestCapability, f.Capability)
		require.Equal(t, "testplugin", f.Provider)
		require.ErrorContains(t, f.Err, "exit status 7")
		testutil.RequireReceive(t, otherStopped, 20*time.Second, "in-process stop")
		require.True(
			t,
			externalGoneAtOtherStop.Load(),
			"in-process provider stopped before the external plugin",
		)
		logged := h.logs.String()
		require.Contains(t, logged, "plugin failed")
		require.Contains(t, logged, "plugin=testplugin")
		require.Contains(t, logged, "exit status 7")
	})

	t.Run("keepalive failure", func(t *testing.T) {
		t.Parallel()
		h := newExternalHarness(t, binary)
		failures := failureHandler(h.host)
		_, err := h.resolve(t, nil, map[string]any{
			"keepaliveTimeout": "300ms",
			"stopTimeout":      "300ms",
		})
		require.NoError(t, err)

		// A stopped process still holds its socket open but cannot answer.
		require.NoError(t, freezeProcess(h.cmd.Load().Process))

		f := requireFailure(t, failures)
		require.ErrorContains(t, f.Err, "keepalive failed")
		err = h.host.Stop(stopContext(t))
		require.ErrorContains(t, err, "was killed")
		require.True(t, h.exited(t))
		h.requireRuntimeDirEmpty(t)
	})

	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"stop escalates to SIGTERM", nil, "exited after SIGTERM"},
		{
			"stop escalates to SIGKILL",
			[]string{"-ignore-term"},
			"ignored Stop and SIGTERM and was killed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newExternalHarness(t, binary)
			client, err := h.resolve(t, tc.args, map[string]any{
				"stopTimeout": "300ms",
			})
			require.NoError(t, err)
			// An open stream keeps the plugin's graceful stop from finishing.
			stream, err := client.Watch(
				t.Context(),
				&healthpb.HealthCheckRequest{Service: "hang"},
			)
			require.NoError(t, err)
			_, err = stream.Recv()
			require.NoError(t, err)

			err = h.host.Stop(stopContext(t))
			require.ErrorContains(t, err, tc.wantErr)
			require.True(t, h.exited(t))
			h.requireRuntimeDirEmpty(t)
		})
	}

	t.Run("rejects an empty command", func(t *testing.T) {
		t.Parallel()
		h := newExternalHarness(t, "")
		_, err := h.resolve(t, nil, nil)
		require.ErrorContains(t, err, "command is empty")
	})

	t.Run("rejects an overlong socket path", func(t *testing.T) {
		t.Parallel()
		h := newExternalHarness(t, binary)
		long := filepath.Join(h.dir, strings.Repeat("d", 100))
		require.NoError(t, os.Mkdir(long, 0o700))
		_, err := h.resolve(t, nil, map[string]any{"runtimeDir": long})
		require.ErrorContains(t, err, "longer than")
	})
}

// h0 returns a short existing directory for paths the test never creates.
func h0(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "dt")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
