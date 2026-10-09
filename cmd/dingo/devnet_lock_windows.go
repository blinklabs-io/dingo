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

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func acquireDevnetStateLock(runDir string) (func() error, error) {
	path := filepath.Join(runDir, devnetStateLock)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening devnet state lock %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("checking devnet state lock %q: %w", path, err), file.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(fmt.Errorf("devnet state lock %q is not a regular file", path), file.Close())
	}
	overlapped := new(windows.Overlapped)
	err = windows.LockFileEx(
		windows.Handle(file.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0,
		1,
		0,
		overlapped,
	)
	if err != nil {
		closeErr := file.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, errors.Join(fmt.Errorf("%w: %q", errDevnetStateInUse, runDir), closeErr)
		}
		return nil, errors.Join(fmt.Errorf("locking devnet state at %q: %w", runDir, err), closeErr)
	}
	return func() error {
		return errors.Join(
			windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, overlapped),
			file.Close(),
		)
	}, nil
}
