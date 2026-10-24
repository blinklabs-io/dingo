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

package kesagent

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
)

// A Unix-domain socket address is a fixed-size struct, not a pointer, so the
// path has to fit in its sun_path field. That field is 104 bytes on Darwin
// and 108 on Linux, and a path over the limit is refused by the kernel with
// EINVAL, which Go surfaces as a bare "invalid argument" on connect. That
// error names neither the length nor the limit, so an operator who points
// --shelley-kes-agent-socket at a path a few bytes too long has nothing to go
// on; checking it here turns a startup mystery into a statement of the limit.
const (
	sunPathLenDarwin = 104
	sunPathLenOther  = 108
)

// socketPathLimit returns the longest socket path this platform accepts.
//
// A non-abstract path is NUL-terminated inside sun_path, costing one byte; an
// abstract address (leading "@" where Go rewrites it) is not, so it may use
// the full field. Getting that distinction right matters because an abstract
// address is a legitimate way to name a KES agent socket, and rejecting one
// byte of it would be a bug in this check rather than in the operator's
// configuration.
func socketPathLimit(path string) int {
	sunPath := sunPathLenOther
	if runtime.GOOS == "darwin" {
		sunPath = sunPathLenDarwin
	}
	if isAbstractSocketPath(path) {
		return sunPath
	}
	return sunPath - 1
}

// isAbstractSocketPath reports whether path is an abstract address on this
// platform.
func isAbstractSocketPath(path string) bool {
	return isAbstractSocketPathFor(runtime.GOOS, path)
}

// isAbstractSocketPathFor reports whether path is an abstract address on goos.
func isAbstractSocketPathFor(goos, path string) bool {
	return hasAbstractSocketNamespace(goos) && strings.HasPrefix(path, "@")
}

// hasAbstractSocketNamespace reports whether Go's syscall package reads a
// leading "@" on goos as an abstract address. It rewrites "@" on Linux (Android
// builds the same file), on Solaris (illumos builds the same file), and on
// Windows. Elsewhere "@name" is an ordinary filename relative to the working
// directory.
func hasAbstractSocketNamespace(goos string) bool {
	switch goos {
	case "linux", "android", "illumos", "solaris", "windows":
		return true
	default:
		return false
	}
}

// resolveSocketPath fixes a named socket to one filesystem location before
// the client starts reconnecting. Linux abstract socket names are already
// complete kernel addresses and must not be interpreted as filesystem paths.
func resolveSocketPath(path string) (string, error) {
	if strings.HasPrefix(path, "\x00") {
		return "", errors.New(
			"kesagent: NUL-prefixed socket addresses are not portable; use the @ prefix on Linux for an abstract socket",
		)
	}
	if isAbstractSocketPath(path) {
		return path, nil
	}
	resolved, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("kesagent: resolve socket path %s: %w", path, err)
	}
	return resolved, nil
}

// checkSocketPathLen is socketPathLimit's decision split out from the
// platform lookup, so the rejection can be exercised for a limit other than
// the running platform's.
func checkSocketPathLen(path string, limit int) error {
	if len(path) > limit {
		return fmt.Errorf(
			"kesagent: socket path is %d bytes, over this platform's %d-byte limit (a unix socket address stores the path in a fixed-size sun_path field: %d bytes on macOS, %d on Linux); use a shorter path: %s",
			len(path),
			limit,
			sunPathLenDarwin,
			sunPathLenOther,
			path,
		)
	}
	return nil
}

// validateSocketPath rejects a socket path this platform cannot connect to.
func validateSocketPath(path string) error {
	return checkSocketPathLen(path, socketPathLimit(path))
}
