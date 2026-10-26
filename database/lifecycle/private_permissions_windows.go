//go:build windows

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
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

var reopenSnapshotDirectory = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReOpenFile")

func restrictPrivateSnapshotDirectory(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close() //nolint:errcheck // ACL operations report the relevant errors
	handle, err := reopenSnapshotDirectoryHandle(
		dir,
		windows.READ_CONTROL|windows.WRITE_DAC,
	)
	if err != nil {
		return fmt.Errorf("reopen directory for permission change: %w", err)
	}
	defer windows.CloseHandle(handle) //nolint:errcheck
	securityInfo, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read directory owner: %w", err)
	}
	owner, _, err := securityInfo.Owner()
	if err != nil {
		return fmt.Errorf("read directory owner SID: %w", err)
	}
	if owner == nil {
		return errors.New("directory security descriptor has no owner")
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		"D:P(A;OICI;GA;;;" + owner.String() + ")",
	)
	if err != nil {
		return fmt.Errorf("build owner-only security descriptor: %w", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("read owner-only DACL: %w", err)
	}
	if err := windows.SetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|
			windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		dacl,
		nil,
	); err != nil {
		return fmt.Errorf("apply owner-only DACL: %w", err)
	}
	return nil
}

func reopenSnapshotDirectoryHandle(
	f *os.File,
	access uint32,
) (windows.Handle, error) {
	handle, _, callErr := reopenSnapshotDirectory.Call(
		f.Fd(),
		uintptr(access),
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|
			windows.FILE_SHARE_DELETE,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
	)
	if windows.Handle(handle) != windows.InvalidHandle {
		return windows.Handle(handle), nil
	}
	if errors.Is(callErr, syscall.Errno(0)) {
		callErr = syscall.EINVAL
	}
	return windows.InvalidHandle, callErr
}
