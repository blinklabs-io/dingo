//go:build unix

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

package lifecycle

import (
	"os"
	"syscall"
	"testing"
)

// TestMain fixes the umask before any test runs. testing.T.TempDir creates
// its numbered subdirectories with mode 0o777, so under a umask of 0o002 they
// are group-writable, and the snapshot and restore work-parent checks
// correctly refuse an ancestor that is writable by group without the sticky
// bit. A test that needs a group- or other-writable directory sets that mode
// explicitly.
func TestMain(m *testing.M) {
	syscall.Umask(0o022)
	os.Exit(m.Run())
}
