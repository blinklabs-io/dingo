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

package config

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

var reopenConfiguredSnapshotDirectory = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReOpenFile")

func restrictSnapshotDirectory(dir *os.File) error {
	writeHandle, err := reopenConfiguredSnapshotDirectoryHandle(
		dir,
		windows.FILE_WRITE_DATA,
	)
	if err != nil {
		return fmt.Errorf("directory is not writable: %w", err)
	}
	if err := windows.CloseHandle(writeHandle); err != nil {
		return fmt.Errorf("close directory writability handle: %w", err)
	}
	handle, err := reopenConfiguredSnapshotDirectoryHandle(
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
	if err != nil || owner == nil {
		return errors.New("directory security descriptor has no owner")
	}
	if err := requireSnapshotDirectoryOwner(owner); err != nil {
		return err
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

func requireSnapshotDirectoryOwner(owner *windows.SID) error {
	var token windows.Token
	if err := windows.OpenProcessToken(
		windows.CurrentProcess(), windows.TOKEN_QUERY, &token,
	); err != nil {
		return fmt.Errorf("open current process token: %w", err)
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		return fmt.Errorf("get current user: %w", err)
	}
	if windows.EqualSid(owner, user.User.Sid) {
		return nil
	}
	adminSID, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		return fmt.Errorf("get administrators SID: %w", err)
	}
	adminMember, err := token.IsMember(adminSID)
	if err != nil {
		return fmt.Errorf("check administrators membership: %w", err)
	}
	if adminMember && windows.EqualSid(owner, adminSID) {
		return nil
	}
	return fmt.Errorf(
		"directory owner %s does not control the current process", owner.String(),
	)
}

func reopenConfiguredSnapshotDirectoryHandle(
	f *os.File,
	access uint32,
) (windows.Handle, error) {
	handle, _, callErr := reopenConfiguredSnapshotDirectory.Call(
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
