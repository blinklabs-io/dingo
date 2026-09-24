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
	"fmt"
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
// A non-abstract path is NUL-terminated inside sun_path, costing one byte; a
// Linux abstract address (leading "@" or NUL) is not, so it may use the full
// field. Getting that distinction right matters because an abstract address
// is a legitimate way to name a KES agent socket on Linux, and rejecting one
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

func isAbstractSocketPath(path string) bool {
	return strings.HasPrefix(path, "@") || strings.HasPrefix(path, "\x00")
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
