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

package connmanager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// maxPortableUnixSocketPathLen is the longest Unix-domain socket path that
// binds on every platform these tests run on.
//
// bind() takes a fixed-size address struct, so the path has to fit in its
// sun_path field with room for a NUL terminator. That field is 104 bytes on
// Darwin and 108 on Linux, making 103 the portable budget. Over it the bind
// fails with EINVAL, which Go surfaces as a bare "bind: invalid argument" --
// a message that reads like a permissions or kernel-support problem rather
// than a length one, and that is why this is asserted up front instead of
// being left to the bind.
const maxPortableUnixSocketPathLen = 103

// unixTestTempDir creates a short-lived temp directory suitable for Unix
// socket paths. t.TempDir() derives its name from the test's own name and
// appends a numbered subdirectory, so a descriptive test name costs 60 or
// more bytes on top of a temp root that is already about 49 bytes on Darwin
// (/var/folders/<2>/<30>/T/) against 5 for Linux's /tmp. A fixed short prefix
// keeps the result under the limit on every platform.
//
// This file carries no build constraint so that tests which skip Unix sockets
// at run time, rather than at compile time, can use the helper too.
func unixTestTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "dt*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// unixTestSocketPath joins name onto dir and fails the test with the length
// if the result would be too long to bind on any supported platform.
func unixTestSocketPath(t *testing.T, dir, name string) string {
	t.Helper()
	socketPath := filepath.Join(dir, name)
	require.LessOrEqual(
		t,
		len(socketPath),
		maxPortableUnixSocketPathLen,
		"unix socket path is %d bytes, over the %d-byte portable limit (sun_path is 104 bytes on Darwin, 108 on Linux); bind would fail with EINVAL: %s",
		len(socketPath),
		maxPortableUnixSocketPathLen,
		socketPath,
	)
	return socketPath
}
