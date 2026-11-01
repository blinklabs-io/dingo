//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lifecycle

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// leaseRestoreDir takes an exclusive flock on the directory open at root.
// The lock belongs to the open file description, so it follows the directory
// through renames and the kernel drops it when the holding process exits by
// any means, including SIGKILL. With wait false, a lock held elsewhere
// reports acquired == false instead of blocking.
func leaseRestoreDir(root *os.Root, wait bool) (io.Closer, bool, error) {
	dir, err := root.Open(".")
	if err != nil {
		return nil, false, err
	}
	conn, err := dir.SyscallConn()
	if err != nil {
		_ = dir.Close()
		return nil, false, err
	}
	how := unix.LOCK_EX
	if !wait {
		how |= unix.LOCK_NB
	}
	var lockErr error
	if err := conn.Control(func(fd uintptr) {
		for {
			//nolint:gosec // descriptor values fit in int
			lockErr = unix.Flock(int(fd), how)
			if !errors.Is(lockErr, unix.EINTR) {
				return
			}
		}
	}); err != nil {
		_ = dir.Close()
		return nil, false, err
	}
	if lockErr != nil {
		_ = dir.Close()
		if errors.Is(lockErr, unix.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, lockErr
	}
	return dir, true, nil
}
