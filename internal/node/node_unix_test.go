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

package node

import (
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// TestRunDoesNotPreRemoveSocketPath verifies that Run leaves socket-path
// handling to the connection manager at bind time. In particular, unrelated
// startup validation must not delete an existing filesystem entry first.
func TestRunDoesNotPreRemoveSocketPath(t *testing.T) {
	testCases := []struct {
		name   string
		create func(*testing.T, string)
	}{
		{
			name: "regular file",
			create: func(t *testing.T, path string) {
				t.Helper()
				require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
			},
		},
		{
			name: "symlink",
			create: func(t *testing.T, path string) {
				t.Helper()
				target := path + ".target"
				require.NoError(t, os.WriteFile(target, []byte("keep"), 0o600))
				require.NoError(t, os.Symlink(target, path))
			},
		},
		{
			name: "directory",
			create: func(t *testing.T, path string) {
				t.Helper()
				require.NoError(t, os.Mkdir(path, 0o700))
			},
		},
		{
			name: "live socket",
			create: func(t *testing.T, path string) {
				t.Helper()
				listener, err := net.Listen("unix", path)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, listener.Close()) })
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tempDir, err := os.MkdirTemp("", "dn*")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(tempDir)) })

			socketPath := filepath.Join(tempDir, "existing")
			testCase.create(t, socketPath)

			cfg := &config.Config{
				SocketPath: socketPath,
				CardanoConfig: filepath.Join(
					tempDir,
					"missing-config.json",
				),
				Network: "missing",
			}
			err = Run(
				cfg,
				slog.New(slog.NewTextHandler(io.Discard, nil)),
			)
			require.Error(t, err, "missing Cardano config should fail startup")
			_, statErr := os.Lstat(socketPath)
			require.NoError(
				t,
				statErr,
				"Run should not remove the configured socket path before binding",
			)
		})
	}
}
