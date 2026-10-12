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

//go:build unix

package dingo

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
)

const restartSupported = true

// ReExec replaces the running process with a fresh instance of the same
// binary and arguments, keeping the process ID so a supervisor sees one
// continuous process. It returns only on failure.
func ReExec() error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate executable: %w", err)
	}
	// Linux appends this suffix to the path of an executable that has been
	// replaced on disk. Preserve a real executable whose name happens to end
	// with the same text.
	if _, statErr := os.Stat(exe); errors.Is(statErr, os.ErrNotExist) {
		exe = strings.TrimSuffix(exe, " (deleted)")
	}
	if err := syscall.Exec(exe, os.Args, os.Environ()); err != nil { //nolint:gosec // re-executing our own binary with our own arguments
		return fmt.Errorf("re-execute %s: %w", exe, err)
	}
	return nil
}
