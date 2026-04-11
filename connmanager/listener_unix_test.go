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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestStartListener_UnixSocket_RemovesStaleSocketFile verifies that a stale
// Unix socket file is automatically removed before binding a new listener.
func TestStartListener_UnixSocket_RemovesStaleSocketFile(t *testing.T) {
	defer goleak.VerifyNone(t)

	socketPath := filepath.Join(t.TempDir(), "test.sock")

	// Create a stale socket file to simulate an unclean previous shutdown.
	// Use os.Create so the file remains after creation (net.Listen removes
	// the file on Close, so we use a plain file to simulate the stale case).
	f, err := os.Create(socketPath)
	require.NoError(t, err)
	f.Close()
	// Verify the file exists
	_, err = os.Stat(socketPath)
	require.NoError(t, err, "stale socket file should exist before test")

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

	socketPath := filepath.Join(t.TempDir(), "test.sock")
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

// TestStartListener_UnixSocket_RemoveFailsOnDirectory verifies that an error
// is surfaced when the socket path is a non-empty directory (cannot be removed).
func TestStartListener_UnixSocket_RemoveFailsOnDirectory(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Create a non-empty directory at the socket path so that os.Remove fails
	socketPath := filepath.Join(t.TempDir(), "stale-dir")
	require.NoError(t, os.MkdirAll(socketPath, 0o755))
	// Add a child entry to make it non-empty
	child, err := os.Create(filepath.Join(socketPath, "child"))
	require.NoError(t, err)
	child.Close()

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
	require.Error(t, err, "Start should fail when socket path cannot be removed")
	assert.Contains(t, err.Error(), "failed to remove existing socket file")
}
