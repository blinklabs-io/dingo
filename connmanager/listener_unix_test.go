//go:build !windows

// Copyright 2025 Blink Labs Software
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

package connmanager

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// unixTestTempDir creates a short-lived temp directory suitable for Unix
// socket paths. t.TempDir() embeds the full test name, easily exceeding macOS's
// 104-byte sockaddr_un.sun_path limit. Using a short prefix keeps the path
// under the limit on all platforms.
func unixTestTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "dt*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestStartListener_UnixSocket_RemovesStaleSocketFile verifies that a stale
// Unix socket file left over from an unclean shutdown is automatically removed
// before binding a new listener.
func TestStartListener_UnixSocket_RemovesStaleSocketFile(t *testing.T) {
	defer goleak.VerifyNone(t)

	socketPath := filepath.Join(unixTestTempDir(t), "test.sock")

	// Create a stale unix socket file to simulate an unclean previous shutdown:
	// listen, disable auto-unlink on close, then close so the socket file
	// remains on disk.
	staleLn, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	staleLn.(*net.UnixListener).SetUnlinkOnClose(false)
	staleLn.Close()

	// Verify the socket file is still present
	fi, err := os.Lstat(socketPath)
	require.NoError(t, err, "stale socket file should exist before test")
	require.NotZero(
		t,
		fi.Mode()&os.ModeSocket,
		"stale file should be a unix socket",
	)

	cfg := ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{
			{
				ListenNetwork: "unix",
				ListenAddress: socketPath,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	ctx := context.Background()
	err = cm.Start(ctx)
	require.NoError(t, err, "Start should succeed when stale socket file is removed")

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(stopCtx)
	require.NoError(t, err)
}

// TestStartListener_UnixSocket_NoExistingFile verifies that a Unix socket
// listener starts successfully when no previous socket file exists.
func TestStartListener_UnixSocket_NoExistingFile(t *testing.T) {
	defer goleak.VerifyNone(t)

	socketPath := filepath.Join(unixTestTempDir(t), "test.sock")
	// No pre-existing socket file

	cfg := ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{
			{
				ListenNetwork: "unix",
				ListenAddress: socketPath,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	ctx := context.Background()
	err := cm.Start(ctx)
	require.NoError(t, err, "Start should succeed when no socket file exists")

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = cm.Stop(stopCtx)
	require.NoError(t, err)
}

// TestStartListener_UnixSocket_ErrorOnNonSocketFile verifies that an error is
// surfaced when the socket path is occupied by a regular (non-socket) file.
// The server must not silently delete arbitrary files.
func TestStartListener_UnixSocket_ErrorOnNonSocketFile(t *testing.T) {
	defer goleak.VerifyNone(t)

	socketPath := filepath.Join(unixTestTempDir(t), "not-a-socket")
	f, err := os.Create(socketPath)
	require.NoError(t, err)
	f.Close()

	cfg := ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{
			{
				ListenNetwork: "unix",
				ListenAddress: socketPath,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	ctx := context.Background()
	err = cm.Start(ctx)
	require.Error(t, err, "Start should fail when socket path is a non-socket file")
	assert.Contains(t, err.Error(), "exists and is not a unix socket")
}

// TestStartListener_UnixSocket_ErrorOnDirectory verifies that an error is
// surfaced when the socket path is occupied by a directory.
func TestStartListener_UnixSocket_ErrorOnDirectory(t *testing.T) {
	defer goleak.VerifyNone(t)

	socketPath := filepath.Join(unixTestTempDir(t), "stale-dir")
	require.NoError(t, os.MkdirAll(socketPath, 0o755))

	cfg := ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{
			{
				ListenNetwork: "unix",
				ListenAddress: socketPath,
			},
		},
	}

	cm := NewConnectionManager(cfg)
	ctx := context.Background()
	err := cm.Start(ctx)
	require.Error(t, err, "Start should fail when socket path is a directory")
	assert.Contains(t, err.Error(), "exists and is not a unix socket")
}
