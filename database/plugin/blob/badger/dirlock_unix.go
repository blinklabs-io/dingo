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

//go:build !windows

package badger

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

const (
	dirLockReleasePoll    = 2 * time.Millisecond
	dirLockReleaseTimeout = 5 * time.Second
)

// waitForDirLockRelease blocks until the flock Badger held on dir is free, or
// dirLockReleaseTimeout passes.
//
// Badger releases that lock only by closing its directory descriptor, and a
// child process forked while the descriptor was open holds a copy that keeps
// the lock until the child's exec completes. Any goroutine starting a process
// during DB.Close can therefore leave the directory locked for a short time
// after Close returns, so an immediate in-process reopen fails with "Cannot
// acquire directory lock". The probe unlocks explicitly before closing, which
// releases the lock even if its own descriptor is inherited by another fork.
// Giving up at the timeout is safe: a lock still held then belongs to someone
// else, and the next open reports it.
func waitForDirLockRelease(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	defer f.Close()
	fd := int(f.Fd()) //nolint:gosec // descriptor values fit in int
	deadline := time.Now().Add(dirLockReleaseTimeout)
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			_ = unix.Flock(fd, unix.LOCK_UN)
			return
		}
		retry := errors.Is(err, unix.EWOULDBLOCK) ||
			errors.Is(err, unix.EINTR)
		if !retry || time.Now().After(deadline) {
			return
		}
		time.Sleep(dirLockReleasePoll)
	}
}
