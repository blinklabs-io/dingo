//go:build windows

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

package devnetkeys

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

var reOpenFile = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReOpenFile")

func restrictLocalTestKeyPath(f *os.File, _ os.FileMode) error {
	handle, err := reopenLocalTestKeyHandle(
		f,
		windows.READ_CONTROL|windows.WRITE_DAC,
	)
	if err != nil {
		return fmt.Errorf("reopening path for permission change: %w", err)
	}
	defer windows.CloseHandle(handle) //nolint:errcheck
	securityInfo, err := windows.GetSecurityInfo(
		handle,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("reading path owner: %w", err)
	}
	owner, _, err := securityInfo.Owner()
	if err != nil {
		return fmt.Errorf("reading path owner SID: %w", err)
	}
	if owner == nil {
		return errors.New("path security descriptor has no owner")
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		"D:P(A;;GA;;;" + owner.String() + ")",
	)
	if err != nil {
		return fmt.Errorf("building owner-only security descriptor: %w", err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("reading owner-only DACL: %w", err)
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
		return fmt.Errorf("applying owner-only DACL: %w", err)
	}
	return nil
}

func syncLocalTestKeyDirectory(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close() //nolint:errcheck // FlushFileBuffers reports the error
	handle, err := reopenLocalTestKeyHandle(
		dir,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
	)
	if err != nil {
		return fmt.Errorf("reopening directory for sync: %w", err)
	}
	defer windows.CloseHandle(handle) //nolint:errcheck
	return windows.FlushFileBuffers(handle)
}

func reopenLocalTestKeyHandle(f *os.File, access uint32) (windows.Handle, error) {
	handle, _, callErr := reOpenFile.Call(
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
