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

package mithril

import (
	"errors"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// This file gives the three Windows extraction operations (rename, file
// removal, directory removal) a way to address their target through a handle
// rather than a path, closing the gap described in issue #3228.
//
// MoveFile, DeleteFile and RemoveDirectory all take a string and resolve it
// themselves, which is a second, independent resolution of names
// openVerifiedParent already walked and verified — a component swapped in the
// instant between the walk and that second resolution is followed, not
// rejected. NtCreateFile can instead open a name as a single component
// relative to an already-open, already-verified directory handle
// (OBJECT_ATTRIBUTES.RootDirectory), and NtSetInformationFile can then rename
// or mark that same open object for deletion without ever handing the kernel
// a path to resolve on its own. That is the same guarantee unlinkat/renameat
// give on Unix, applied through the lower-level API Windows requires to get
// it: golang.org/x/sys/windows exposes NtCreateFile and NtSetInformationFile
// directly, but not the FILE_RENAME_INFORMATION/FILE_DISPOSITION_INFORMATION
// request buffers those take, which this file builds by hand.
//
// The open itself still resolves one name — the final component, under the
// verified parent — so it carries FILE_OPEN_REPARSE_POINT and an explicit
// reparse-point check afterward, the same rejection openVerifiedRoot applies
// to every component above it. A symlink planted at the leaf between the walk
// finishing and this open running is refused here rather than followed, which
// is the residual window the walk alone cannot close.

// rootDirHandle returns a raw handle to root's own directory, obtained by
// reopening "." relative to root's existing handle rather than by resolving
// root's name again — the same operation openVerifiedRoot already performs
// when it calls root.Stat("."). The returned file must be kept open, and
// closed, for as long as the handle is used.
func rootDirHandle(root *os.Root) (*os.File, windows.Handle, error) {
	f, err := root.Open(".")
	if err != nil {
		return nil, 0, err
	}
	return f, windows.Handle(f.Fd()), nil
}

// ntStatusErrno converts an NTSTATUS failure from NtCreateFile or
// NtSetInformationFile into the same syscall.Errno DeleteFile, MoveFile and
// RemoveDirectory would have returned, so callers that check errors.Is against
// fs.ErrNotExist / fs.ErrExist keep working unchanged.
func ntStatusErrno(err error) error {
	if status, ok := errors.AsType[windows.NTStatus](err); ok {
		return status.Errno()
	}
	return err
}

// openRelativeForDeletion opens name as a single component relative to dir,
// with DELETE access and no path of its own to resolve, then confirms the
// result is neither a reparse point nor the wrong type. typeOption is
// windows.FILE_NON_DIRECTORY_FILE or windows.FILE_DIRECTORY_FILE, matching the
// type refusal DeleteFile/RemoveDirectory each already carry.
func openRelativeForDeletion(
	dir windows.Handle,
	name string,
	typeOption uint32,
) (windows.Handle, error) {
	// FILE_OPEN_FOR_BACKUP_INTENT is what makes a minimal-access, handle-
	// relative open like this one work at all: without it, NtCreateFile
	// applies the normal traversal/listing access check a directory open
	// otherwise needs, which DELETE (and, for a directory, FILE_DIRECTORY_FILE
	// alone) does not satisfy. FILE_READ_ATTRIBUTES is requested alongside
	// DELETE so rejectReparsePoint's GetFileInformationByHandle call below has
	// what it needs. This mirrors the recipe Go's own os.Root uses internally
	// on Windows for the same operations (internal/syscall/windows,
	// Deleteat/Renameat) -- unimportable from here, but the reference this was
	// built against. Every entry this touches is one extraction created or is
	// about to replace, so the restrictive-ACL case that recipe's own
	// DELETE-only fallback exists for does not apply here.
	handle, err := openRelative(
		dir,
		name,
		windows.FILE_READ_ATTRIBUTES|windows.DELETE,
		windows.FILE_OPEN_REPARSE_POINT|windows.FILE_OPEN_FOR_BACKUP_INTENT|typeOption,
	)
	if err != nil {
		return 0, err
	}
	if err := rejectReparsePoint(handle); err != nil {
		_ = windows.CloseHandle(handle)
		return 0, err
	}
	return handle, nil
}

// openRelative opens name as a single component relative to dir using
// NtCreateFile, addressing the entry through dir's handle rather than
// resolving any part of a path. FILE_OPEN_FOR_BACKUP_INTENT and
// OBJ_CASE_INSENSITIVE are always applied: the former is required for a
// minimal-access open like this to succeed at all (see
// openRelativeForDeletion), and NT native APIs are case-sensitive by default,
// unlike every Win32 path-based API this replaces.
func openRelative(
	dir windows.Handle,
	name string,
	access uint32,
	options uint32,
) (windows.Handle, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, err
	}
	oa := windows.OBJECT_ATTRIBUTES{
		RootDirectory: dir,
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}
	oa.Length = uint32(unsafe.Sizeof(oa))
	var handle windows.Handle
	var iosb windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(
		&handle,
		access,
		&oa,
		&iosb,
		nil,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN,
		options,
		0,
		0,
	)
	if err != nil {
		return 0, ntStatusErrno(err)
	}
	return handle, nil
}

// rejectReparsePoint reports an error if handle refers to a reparse point
// (symlink, junction, or similar). openRelativeForDeletion and
// openRelativeForRename both pass FILE_OPEN_REPARSE_POINT so a reparse point
// at the target opens the link itself rather than being followed; this is
// what turns that non-following open into an outright refusal, matching what
// openVerifiedRoot already does for every component above the leaf.
func rejectReparsePoint(handle windows.Handle) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return ErrExtractUnsafePath
	}
	return nil
}

// setDeleteDisposition marks handle's underlying file or empty directory for
// deletion. The removal becomes visible once every handle on the object,
// including this one, is closed; closing it immediately after this call is
// what makes the deletion effective before the caller returns.
func setDeleteDisposition(handle windows.Handle) error {
	var info fileDispositionInformation
	info.DeleteFile = true
	var iosb windows.IO_STATUS_BLOCK
	err := windows.NtSetInformationFile(
		handle,
		&iosb,
		(*byte)(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
		windows.FileDispositionInformation,
	)
	if err != nil {
		return ntStatusErrno(err)
	}
	return nil
}

// fileDispositionInformation mirrors the classic (non-Ex) FILE_DISPOSITION_INFORMATION
// the FileDispositionInformation class expects: a single BOOLEAN. Available
// since Windows Vista, which covers every currently supported release,
// unlike the newer FileDispositionInformationEx this deliberately does not
// need.
type fileDispositionInformation struct {
	DeleteFile bool
}

// openRelativeForRename opens name as a single component relative to dir,
// with the access and options NtSetInformationFile's FileRenameInformation
// requires of the handle it renames, and refuses a reparse point at the leaf
// for the same reason openRelativeForDeletion does. The rename always moves a
// directory here (renameExtractedDirectory's only callers move the staging
// directory), so the open requires FILE_DIRECTORY_FILE. SYNCHRONIZE alongside
// DELETE, and FILE_SYNCHRONOUS_IO_NONALERT alongside
// FILE_OPEN_FOR_BACKUP_INTENT, mirror the access Renameat opens its rename
// source with; that reference has no reparse-point check of its own, so
// FILE_READ_ATTRIBUTES is added beyond it for the same reason
// openRelativeForDeletion needs it: rejectReparsePoint's
// GetFileInformationByHandle call below requires it.
func openRelativeForRename(
	dir windows.Handle,
	name string,
) (windows.Handle, error) {
	handle, err := openRelative(
		dir, name,
		windows.DELETE|windows.SYNCHRONIZE|windows.FILE_READ_ATTRIBUTES,
		windows.FILE_OPEN_REPARSE_POINT|
			windows.FILE_OPEN_FOR_BACKUP_INTENT|
			windows.FILE_SYNCHRONOUS_IO_NONALERT|
			windows.FILE_DIRECTORY_FILE,
	)
	if err != nil {
		return 0, err
	}
	if err := rejectReparsePoint(handle); err != nil {
		_ = windows.CloseHandle(handle)
		return 0, err
	}
	return handle, nil
}

// renameRelativeHandle renames the object source refers to, using
// destDir/destName as the new location. Both endpoints are handle-relative:
// source was opened through the verified parent above, and destDir is the
// verified destination parent's own handle, so nothing here resolves a path
// from the volume root. replaceIfExists mirrors MoveFile's behavior when
// false: the call fails rather than replacing an existing destination.
func renameRelativeHandle(
	source windows.Handle,
	destDir windows.Handle,
	destName string,
	replaceIfExists bool,
) error {
	info, err := buildRenameInformation(destDir, destName, replaceIfExists)
	if err != nil {
		return err
	}
	var iosb windows.IO_STATUS_BLOCK
	// A rename request buffer never approaches 4GiB.
	infoLen := uint32(len(info)) //nolint:gosec // G115
	err = windows.NtSetInformationFile(
		source,
		&iosb,
		&info[0],
		infoLen,
		windows.FileRenameInformation,
	)
	if err != nil {
		return ntStatusErrno(err)
	}
	return nil
}

// fileRenameInformationHeader mirrors the fixed portion of the classic
// (non-Ex) FILE_RENAME_INFORMATION the FileRenameInformation class expects,
// up to and including FileNameLength. The variable-length FileName that
// follows it in the real structure is appended separately by
// buildRenameInformation rather than declared here: Go, like the C struct
// this mirrors, would pad the type out to RootDirectory's 8-byte alignment if
// FileName were included, putting padding between FileNameLength and FileName
// instead of the flush layout NtSetInformationFile requires. Declaring only
// the fields up to FileNameLength and computing the FileName offset
// explicitly (via unsafe.Offsetof, not unsafe.Sizeof) avoids that trap.
type fileRenameInformationHeader struct {
	ReplaceIfExists bool
	RootDirectory   windows.Handle
	FileNameLength  uint32
}

// buildRenameInformation lays out a FILE_RENAME_INFORMATION request buffer:
// the fixed header immediately followed by newName encoded as UTF-16, with no
// gap between FileNameLength and the name it describes.
func buildRenameInformation(
	destDir windows.Handle,
	newName string,
	replaceIfExists bool,
) ([]byte, error) {
	name16, err := windows.UTF16FromString(newName)
	if err != nil {
		return nil, err
	}
	// UTF16FromString appends a trailing NUL; FileNameLength must not count it.
	name16 = name16[:len(name16)-1]
	nameBytes := len(name16) * 2

	var probe fileRenameInformationHeader
	nameOffset := int(
		unsafe.Offsetof(
			probe.FileNameLength,
		) + unsafe.Sizeof(
			probe.FileNameLength,
		),
	)
	buf := make([]byte, nameOffset+nameBytes)
	hdr := (*fileRenameInformationHeader)(unsafe.Pointer(&buf[0]))
	hdr.ReplaceIfExists = replaceIfExists
	hdr.RootDirectory = destDir
	//nolint:gosec // G115: nameBytes is a path component length, far below uint32 range
	hdr.FileNameLength = uint32(nameBytes)
	if nameBytes > 0 {
		dst := unsafe.Slice(
			(*uint16)(unsafe.Pointer(&buf[nameOffset])),
			len(name16),
		)
		copy(dst, name16)
	}
	return buf, nil
}
