//go:build freebsd

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

package securefs

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	freeBSDRenameat2       = 602
	freeBSDRenameNoReplace = 1
)

func renameNoReplace(root *os.Root, oldName, newName string) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close() //nolint:errcheck // rename error is authoritative
	oldPtr, err := unix.BytePtrFromString(oldName)
	if err != nil {
		return err
	}
	newPtr, err := unix.BytePtrFromString(newName)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall6(
		freeBSDRenameat2,
		dir.Fd(), uintptr(unsafe.Pointer(oldPtr)),
		dir.Fd(), uintptr(unsafe.Pointer(newPtr)),
		freeBSDRenameNoReplace, 0,
	)
	if errno != 0 {
		if errno == syscall.ENOSYS || errno == syscall.EOPNOTSUPP {
			return renameNoReplacePrivateFreeBSD(
				root, dir, oldName, newName,
			)
		}
		return errno
	}
	return nil
}

func renameNoReplacePrivateFreeBSD(
	root *os.Root,
	dir *os.File,
	oldName, newName string,
) error {
	// FreeBSD 14 has renameat but no flagged rename. In a directory that no
	// untrusted principal can modify, flock plus an absence check gives Dingo
	// processes no-replace semantics without exposing a replacement window.
	// Shared writable directories fail closed because another principal could
	// ignore the advisory lock.
	var stat unix.Stat_t
	if err := unix.Fstat(int(dir.Fd()), &stat); err != nil {
		return err
	}
	if (stat.Uid != uint32(os.Geteuid()) && stat.Uid != 0) ||
		stat.Mode&(unix.S_IWGRP|unix.S_IWOTH) != 0 {
		return fmt.Errorf(
			"%w on this FreeBSD kernel for a mutable parent directory",
			ErrRenameNoReplaceUnsupported,
		)
	}
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_EX); err != nil {
		return err
	}
	defer unix.Flock(int(dir.Fd()), unix.LOCK_UN) //nolint:errcheck // rename result is authoritative
	if _, err := root.Lstat(newName); err == nil {
		return syscall.EEXIST
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return unix.Renameat(
		int(dir.Fd()), oldName,
		int(dir.Fd()), newName,
	)
}
