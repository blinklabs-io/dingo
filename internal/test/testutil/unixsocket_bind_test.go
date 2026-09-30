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

package testutil

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The tests here bind real Unix-domain sockets. They are excluded on Windows
// for the same reason connmanager/listener_unix_test.go is: the project does
// not use this transport there. The length checks that need no socket live in
// unixsocket_test.go and run on every platform.

// TestUnixSocketPathIsPortablyShort pins the property the helper exists for.
// A socket path that fits here fits on every platform, so a test using it
// cannot pass on Linux and fail on Darwin or Windows for a reason that has
// nothing to do with what it is testing.
func TestUnixSocketPathIsPortablyShort(t *testing.T) {
	t.Parallel()

	path := UnixSocketPath(t)
	require.LessOrEqual(t, len(path), MaxPortableUnixSocketPathLen)

	// And it must actually be bindable here, not merely short.
	ln, err := net.Listen("unix", path)
	require.NoError(t, err)
	require.NoError(t, ln.Close())
}

// TestOverLongUnixSocketPathFailsToBind is the control behind the guard: it
// demonstrates the failure mode the guard prevents, by binding at a path past
// this platform's own sun_path limit and confirming the bind is refused
// outright. Without this, the guard would rest on a claim about other
// platforms that nothing here checks.
//
// The path is built past 108 bytes, which exceeds sun_path on every supported
// platform (104 on Darwin, 108 on Linux and Windows), so the refusal is
// reproduced rather than assumed. The assertion is that the bind fails and
// that it fails for length -- an EINVAL-class error, not a missing directory
// -- since the directory is created first.
func TestOverLongUnixSocketPathFailsToBind(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "sock")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	// Nest directories until the socket path is comfortably past 108 bytes.
	for len(dir) < 120 {
		dir = filepath.Join(dir, strings.Repeat("d", 20))
	}
	require.NoError(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, "s")
	require.Greater(t, len(path), 108)
	require.Error(t, CheckUnixSocketPathLen(path))

	ln, err := net.Listen("unix", path)
	if err == nil {
		_ = ln.Close()
		t.Fatalf(
			"bound a %d-byte socket path; expected the platform to refuse it",
			len(path),
		)
	}
	require.ErrorContains(t, err, "invalid argument")
}
