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

package securefs

import (
	"errors"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func renameNoReplace(root *os.Root, oldName, newName string) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close() //nolint:errcheck // rename error is authoritative
	dirHandle := windows.Handle(dir.Fd())
	source, err := openForRename(dirHandle, oldName)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(source) //nolint:errcheck // rename error is authoritative
	info, err := renameInformation(dirHandle, newName)
	if err != nil {
		return err
	}
	var status windows.IO_STATUS_BLOCK
	err = windows.NtSetInformationFile(
		source,
		&status,
		&info[0],
		uint32(len(info)), //nolint:gosec // path components cannot approach 4 GiB
		windows.FileRenameInformation,
	)
	if ntStatus, ok := errors.AsType[windows.NTStatus](err); ok {
		return ntStatus.Errno()
	}
	return err
}

func openForRename(parent windows.Handle, name string) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	attrs := windows.OBJECT_ATTRIBUTES{
		RootDirectory: parent,
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}
	attrs.Length = uint32(unsafe.Sizeof(attrs))
	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(
		&handle,
		windows.DELETE|windows.SYNCHRONIZE|windows.FILE_READ_ATTRIBUTES,
		&attrs,
		&status,
		nil,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		windows.FILE_OPEN_REPARSE_POINT|
			windows.FILE_OPEN_FOR_BACKUP_INTENT|
			windows.FILE_SYNCHRONOUS_IO_NONALERT,
		0,
		0,
	)
	if err != nil {
		if ntStatus, ok := errors.AsType[windows.NTStatus](err); ok {
			return 0, ntStatus.Errno()
		}
		return 0, err
	}
	var byHandle windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &byHandle); err != nil {
		_ = windows.CloseHandle(handle)
		return 0, err
	}
	if byHandle.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(handle)
		return 0, syscallErrInvalidName
	}
	return handle, nil
}

var syscallErrInvalidName = windows.ERROR_INVALID_NAME

type renameInformationHeader struct {
	ReplaceIfExists bool
	RootDirectory   windows.Handle
	FileNameLength  uint32
}

func renameInformation(parent windows.Handle, name string) ([]byte, error) {
	name16, err := windows.UTF16FromString(name)
	if err != nil {
		return nil, err
	}
	name16 = name16[:len(name16)-1]
	nameBytes := len(name16) * 2
	var probe renameInformationHeader
	nameOffset := int(
		unsafe.Offsetof(probe.FileNameLength) +
			unsafe.Sizeof(probe.FileNameLength),
	)
	buf := make([]byte, nameOffset+nameBytes)
	header := (*renameInformationHeader)(unsafe.Pointer(&buf[0]))
	header.RootDirectory = parent
	header.FileNameLength = uint32(nameBytes) //nolint:gosec // path component size
	if nameBytes > 0 {
		copy(
			unsafe.Slice(
				(*uint16)(unsafe.Pointer(&buf[nameOffset])),
				len(name16),
			),
			name16,
		)
	}
	return buf, nil
}
