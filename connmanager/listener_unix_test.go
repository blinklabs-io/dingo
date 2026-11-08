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
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestStartListener_UnixSocket_AppliesConfiguredMode verifies that a managed
// socket is never published with permissions inherited from a permissive
// process umask.
func TestStartListener_UnixSocket_AppliesConfiguredMode(t *testing.T) {
	// Not t.Parallel: syscall.Umask changes process-global state.
	oldMask := syscall.Umask(0)
	defer syscall.Umask(oldMask)

	for _, mode := range []os.FileMode{0, 0o600, 0o660} {
		t.Run(fmt.Sprintf("%04o", mode), func(t *testing.T) {
			socketPath := unixTestSocketPath(
				t,
				unixTestTempDir(t),
				"mode.sock",
			)
			cm := NewConnectionManager(ConnectionManagerConfig{
				Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
				PromRegistry: prometheus.NewRegistry(),
				Listeners: []ListenerConfig{{
					ListenNetwork:     "unix",
					ListenAddress:     socketPath,
					UnixSocketMode:    mode,
					UnixSocketModeSet: true,
				}},
			})
			require.NoError(t, cm.Start(t.Context()))

			info, err := os.Lstat(socketPath)
			require.NoError(t, err)
			require.Equal(t, mode, info.Mode().Perm())

			stopCtx, cancel := context.WithTimeout(
				context.Background(),
				2*time.Second,
			)
			defer cancel()
			require.NoError(t, cm.Stop(stopCtx))
			_, err = os.Lstat(socketPath)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestCreateUnixSocketListenerPreservesLinuxAbstractAddress(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("abstract Unix socket names are a Linux feature")
	}
	address := fmt.Sprintf("@dingo-%d-%d", os.Getpid(), time.Now().UnixNano())
	listener, err := createUnixSocketListener(
		t.Context(),
		net.ListenConfig{},
		address,
		0o600,
		true,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	require.Equal(t, address, listener.Addr().String())

	conn, err := net.DialTimeout("unix", address, time.Second)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestStartListener_UnixSocket_RejectsMutableParent(t *testing.T) {
	dir := unixTestTempDir(t)
	require.NoError(t, os.Chmod(dir, 0o770))
	t.Cleanup(func() { require.NoError(t, os.Chmod(dir, 0o700)) })

	cm := NewConnectionManager(ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{{
			ListenNetwork:     "unix",
			ListenAddress:     filepath.Join(dir, "node.sock"),
			UnixSocketMode:    0o660,
			UnixSocketModeSet: true,
		}},
	})
	err := cm.Start(t.Context())
	require.ErrorContains(
		t,
		err,
		"parent must not be writable by group or other users",
	)
}

func TestStartListener_UnixSocket_ReportsSecurePathBudget(t *testing.T) {
	base := unixTestTempDir(t)
	maxPathLen := len(syscall.RawSockaddrUnix{}.Path) - 1
	parentLen := maxPathLen - len(string(filepath.Separator)+"s")
	paddingLen := parentLen - len(base) - 1
	if paddingLen < 1 {
		t.Skip("temporary directory leaves no path-length test budget")
	}
	parent := filepath.Join(base, strings.Repeat("p", paddingLen))
	require.NoError(t, os.Mkdir(parent, 0o700))
	address := filepath.Join(parent, "s")
	require.LessOrEqual(t, len(address), maxPathLen)

	cm := NewConnectionManager(ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{{
			ListenNetwork:     "unix",
			ListenAddress:     address,
			UnixSocketMode:    0o600,
			UnixSocketModeSet: true,
		}},
	})
	err := cm.Start(t.Context())
	require.ErrorContains(t, err, "secure unix socket staging path")
	require.ErrorContains(t, err, "shorten the parent directory")
	entries, readErr := os.ReadDir(parent)
	require.NoError(t, readErr)
	require.Empty(t, entries, "failed publication left staging data")
}

func TestStartListener_UnixSocket_ClosePreservesReplacement(t *testing.T) {
	dir := unixTestTempDir(t)
	address := filepath.Join(dir, "node.sock")
	cm := NewConnectionManager(ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{{
			ListenNetwork:     "unix",
			ListenAddress:     address,
			UnixSocketMode:    0o600,
			UnixSocketModeSet: true,
		}},
	})
	require.NoError(t, cm.Start(t.Context()))
	require.NoError(t, os.Remove(address))
	require.NoError(t, os.WriteFile(address, []byte("replacement"), 0o600))

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, cm.Stop(stopCtx))
	contents, err := os.ReadFile(address)
	require.NoError(t, err)
	require.Equal(t, []byte("replacement"), contents)
}

// TestStartListener_UnixSocket_RemovesStaleSocketFile verifies that a stale
// Unix socket file left over from an unclean shutdown is automatically removed
// before binding a new listener.
func TestStartListener_UnixSocket_RemovesStaleSocketFile(t *testing.T) {
	defer goleak.VerifyNone(t)

	socketPath := unixTestSocketPath(t, unixTestTempDir(t), "test.sock")

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
	require.NoError(
		t,
		err,
		"Start should succeed when stale socket file is removed",
	)

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
	require.Error(
		t,
		err,
		"Start should fail when socket path is a non-socket file",
	)
	assert.Contains(t, err.Error(), "exists and is not a unix socket")
	_, statErr := os.Lstat(socketPath)
	require.NoError(t, statErr, "regular file should not be removed")
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
	fi, statErr := os.Lstat(socketPath)
	require.NoError(t, statErr, "directory should not be removed")
	assert.True(t, fi.IsDir())
}

// TestStartListener_UnixSocket_ErrorOnSymlink verifies that socket-path
// inspection does not follow symlinks, even when the link target is itself a
// Unix socket.
func TestStartListener_UnixSocket_ErrorOnSymlink(t *testing.T) {
	defer goleak.VerifyNone(t)

	tempDir := unixTestTempDir(t)
	targetPath := unixTestSocketPath(t, tempDir, "target.sock")
	staleLn, err := net.Listen("unix", targetPath)
	require.NoError(t, err)
	staleLn.(*net.UnixListener).SetUnlinkOnClose(false)
	require.NoError(t, staleLn.Close())

	socketPath := unixTestSocketPath(t, tempDir, "link.sock")
	require.NoError(t, os.Symlink(targetPath, socketPath))

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
	err = cm.Start(context.Background())
	require.Error(t, err, "Start should fail when socket path is a symlink")
	assert.Contains(t, err.Error(), "exists and is not a unix socket")
	fi, statErr := os.Lstat(socketPath)
	require.NoError(t, statErr, "symlink should not be removed")
	assert.NotZero(t, fi.Mode()&os.ModeSymlink)
	targetInfo, statErr := os.Lstat(targetPath)
	require.NoError(t, statErr, "symlink target should not be removed")
	assert.NotZero(t, targetInfo.Mode()&os.ModeSocket)
}

// TestStartListener_UnixSocket_ErrorOnLiveSocket verifies that startup does
// not unlink and replace a socket owned by a running process.
func TestStartListener_UnixSocket_ErrorOnLiveSocket(t *testing.T) {
	defer goleak.VerifyNone(t)

	socketPath := unixTestSocketPath(t, unixTestTempDir(t), "live.sock")
	liveLn, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, liveLn.Close()) })

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
	err = cm.Start(context.Background())
	if err == nil {
		stopCtx, cancel := context.WithTimeout(
			context.Background(),
			2*time.Second,
		)
		defer cancel()
		require.NoError(t, cm.Stop(stopCtx))
	}
	require.Error(t, err, "Start should fail when socket path is live")
	assert.Contains(t, err.Error(), "live unix socket")

	conn, dialErr := net.DialTimeout("unix", socketPath, time.Second)
	require.NoError(t, dialErr, "incumbent live socket should remain reachable")
	require.NoError(t, conn.Close())
}

// TestStartListener_UnixSocket_PropagatesRemovalError verifies that a stale
// socket is not treated as successfully removed when the parent directory
// denies unlinking it.
func TestStartListener_UnixSocket_PropagatesRemovalError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can unlink from a non-writable directory")
	}
	defer goleak.VerifyNone(t)

	tempDir := unixTestTempDir(t)
	socketPath := unixTestSocketPath(t, tempDir, "stale.sock")
	staleLn, err := net.Listen("unix", socketPath)
	require.NoError(t, err)
	staleLn.(*net.UnixListener).SetUnlinkOnClose(false)
	require.NoError(t, staleLn.Close())

	require.NoError(t, os.Chmod(tempDir, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(tempDir, 0o700)) })

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
	err = cm.Start(context.Background())
	require.Error(t, err, "Start should propagate stale-socket removal errors")
	assert.Contains(t, err.Error(), "failed to remove existing socket file")
	require.NoError(t, os.Chmod(tempDir, 0o700))
	_, statErr := os.Lstat(socketPath)
	require.NoError(t, statErr, "socket should remain when removal fails")
}

func TestStartListener_UnixSocket_CloseSwapDuringTeardownPreservesReplacement(
	t *testing.T,
) {
	dir := unixTestTempDir(t)
	address := filepath.Join(dir, "node.sock")
	cm := NewConnectionManager(ConnectionManagerConfig{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PromRegistry: prometheus.NewRegistry(),
		Listeners: []ListenerConfig{{
			ListenNetwork:     "unix",
			ListenAddress:     address,
			UnixSocketMode:    0o600,
			UnixSocketModeSet: true,
		}},
	})
	require.NoError(t, cm.Start(t.Context()))

	beforeUnixSocketQuarantine = func() {
		require.NoError(t, os.Remove(address))
		require.NoError(t, os.WriteFile(address, []byte("replacement"), 0o600))
	}
	t.Cleanup(func() { beforeUnixSocketQuarantine = nil })

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, cm.Stop(stopCtx))
	contents, err := os.ReadFile(address)
	require.NoError(t, err)
	require.Equal(t, []byte("replacement"), contents)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "teardown left quarantine data")
}
