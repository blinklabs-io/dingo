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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// MaxPortableUnixSocketPathLen is the longest Unix-domain socket path that
// binds on every platform this project's tests run on.
//
// The address a bind() takes is a fixed-size struct, not a pointer, so the
// path has to fit in its sun_path field with room for a NUL terminator. That
// field is 104 bytes on Darwin and 108 on Linux and on Windows
// (UNIX_PATH_MAX in afunix.h), so 103 is the portable budget. Over it, the
// bind fails immediately with EINVAL -- surfaced by Go as
// "bind: invalid argument" -- which reads like a permissions or support
// problem rather than a length one.
//
// t.TempDir() is what makes this easy to hit by accident: it derives a
// directory name from the test's own name and appends a numbered subdirectory,
// so a descriptive test name silently costs 60 or more bytes on top of a temp
// root that is already 49 bytes on Darwin (/var/folders/<2>/<30>/T/) and 36 on
// a Windows runner, against 5 for Linux's /tmp. That is why a socket test can
// pass on Linux and fail on nothing else.
const MaxPortableUnixSocketPathLen = 103

// CheckUnixSocketPathLen reports whether path is short enough to bind on every
// supported platform. It is separate from UnixSocketPath so a test can prove
// the guard rejects an over-long path without needing a platform that would
// actually refuse to bind it.
func CheckUnixSocketPathLen(path string) error {
	if len(path) > MaxPortableUnixSocketPathLen {
		return fmt.Errorf(
			"unix socket path is %d bytes, over the %d-byte portable limit (sun_path is 104 bytes on Darwin, 108 on Linux and Windows); bind would fail with EINVAL: %s",
			len(path),
			MaxPortableUnixSocketPathLen,
			path,
		)
	}
	return nil
}

// UnixSocketPath returns a path for a Unix-domain socket inside a temporary
// directory that is removed when the test ends, and fails the test loudly if
// the result would be too long to bind on any supported platform.
//
// It deliberately does not use t.TempDir(): that encodes the test name in the
// path, which is exactly what pushes these paths past sun_path on Darwin and
// Windows. The name here is a fixed short stem for the same reason -- a
// caller does not get to make it descriptive.
func UnixSocketPath(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sock")
	require.NoError(t, err, "create socket directory")
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "s")
	require.NoError(t, CheckUnixSocketPathLen(path))
	return path
}

// SkipIfBlockProducerUnsupported skips a test that exercises the block
// production path on a platform where Blink Labs does not support running a
// block producer.
//
// This is a product boundary, not a technical one: Windows can open the
// AF_UNIX sockets the KES agent uses, and Go supports them there. Block
// production is simply not a supported configuration on Windows, and the KES
// agent exists only to serve a block producer, so none of it applies. Anyone
// changing this should change the product decision first, not the skip.
func SkipIfBlockProducerUnsupported(t testing.TB) {
	t.Helper()
	if reason := blockProducerUnsupportedReason(runtime.GOOS); reason != "" {
		t.Skip(reason)
	}
}

// blockProducerUnsupportedReason returns why block production is unsupported
// on goos, or "" where it is supported. Taking goos as an argument rather than
// reading runtime.GOOS keeps the decision itself testable from any platform,
// which matters because the platform it excludes is the one no developer here
// runs.
func blockProducerUnsupportedReason(goos string) string {
	if goos == "windows" {
		return "block production is not a supported configuration on Windows, so the KES agent path does not apply there"
	}
	return ""
}
