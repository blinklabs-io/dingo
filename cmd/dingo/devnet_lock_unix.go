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

package main

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func acquireDevnetStateLock(
	root *os.Root,
	runDir string,
) (func() error, error) {
	if linkInfo, err := root.Lstat(devnetStateLock); err == nil {
		if linkInfo.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("devnet state lock in %q is a symlink", runDir)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("checking devnet state lock in %q: %w", runDir, err)
	}
	file, err := root.OpenFile(devnetStateLock, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening devnet state lock in %q: %w", runDir, err)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("checking devnet state lock in %q: %w", runDir, err), file.Close())
	}
	if !info.Mode().IsRegular() {
		return nil, errors.Join(fmt.Errorf("devnet state lock in %q is not a regular file", runDir), file.Close())
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeErr := file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, errors.Join(fmt.Errorf("%w: %q", errDevnetStateInUse, runDir), closeErr)
		}
		return nil, errors.Join(fmt.Errorf("locking devnet state at %q: %w", runDir, err), closeErr)
	}
	return func() error {
		return errors.Join(
			unix.Flock(int(file.Fd()), unix.LOCK_UN),
			file.Close(),
		)
	}, nil
}
