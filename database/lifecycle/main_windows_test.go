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

//go:build windows

package lifecycle

import (
	"os"
	"testing"
)

// TestMain removes the shared fixtures built once per test binary; the Unix
// TestMain does the same after fixing the umask, which Windows has no
// equivalent of.
func TestMain(m *testing.M) {
	code := m.Run()
	runSharedFixtureCleanups()
	os.Exit(code)
}
