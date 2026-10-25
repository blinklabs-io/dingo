//go:build windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package lifecycle

import (
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"

	"github.com/blinklabs-io/dingo/internal/securefs"
	"golang.org/x/sys/windows"
)

// createRestoreDirectory creates a directory with a protected DACL in the
// same native operation that makes it visible. The retained parent handle
// denies deletion so provider pathnames cannot be redirected afterward.
func createRestoreDirectory(
	parent *os.Root,
	parentPath string,
	name string,
) (io.Closer, os.FileInfo, error) {
	parentLock, err := securefs.LockWindowsPath(parentPath)
	if err != nil {
		return nil, nil, fmt.Errorf("lock restore parent %q: %w", parentPath, err)
	}
	closeOnError := func(err error) (io.Closer, os.FileInfo, error) {
		_ = parentLock.Close()
		return nil, nil, err
	}
	named, err := parent.Stat(".")
	if err != nil {
		return closeOnError(err)
	}
	opened := parentLock.Info()
	if !opened.IsDir() || !os.SameFile(named, opened) {
		return closeOnError(fmt.Errorf(
			"%w: restore parent %q changed while it was locked",
			errRestoreDirReplaced, parentPath,
		))
	}

	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return closeOnError(fmt.Errorf("get restore owner: %w", err))
	}
	owner := user.User.Sid.String()
	sd, err := windows.SecurityDescriptorFromString(
		"O:" + owner + "G:" + owner + "D:P" +
			"(A;OICI;FA;;;" + owner + ")" +
			"(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)",
	)
	if err != nil {
		return closeOnError(fmt.Errorf("build restore directory ACL: %w", err))
	}
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return closeOnError(err)
	}
	oa := &windows.OBJECT_ATTRIBUTES{
		Length:             uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory:      parentLock.Handle(),
		ObjectName:         objectName,
		Attributes:         windows.OBJ_CASE_INSENSITIVE,
		SecurityDescriptor: sd,
	}
	var (
		dirHandle windows.Handle
		iosb      windows.IO_STATUS_BLOCK
	)
	err = windows.NtCreateFile(
		&dirHandle,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE|windows.DELETE,
		oa,
		&iosb,
		nil,
		windows.FILE_ATTRIBUTE_DIRECTORY,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_CREATE,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT,
		0,
		0,
	)
	if err != nil {
		return closeOnError(fmt.Errorf("create protected restore directory: %w", err))
	}
	dirFile := os.NewFile(uintptr(dirHandle), name)
	created, err := dirFile.Stat()
	if err != nil {
		cleanupErr := deleteCreatedRestoreDirectory(dirHandle)
		closeErr := dirFile.Close()
		return closeOnError(errors.Join(
			fmt.Errorf("inspect created restore directory: %w", err),
			cleanupErr,
			closeErr,
		))
	}
	var cleanupHandle windows.Handle
	currentProcess := windows.CurrentProcess()
	if err := windows.DuplicateHandle(
		currentProcess,
		dirHandle,
		currentProcess,
		&cleanupHandle,
		0,
		false,
		windows.DUPLICATE_SAME_ACCESS,
	); err != nil {
		cleanupErr := deleteCreatedRestoreDirectory(dirHandle)
		closeErr := dirFile.Close()
		return closeOnError(errors.Join(
			fmt.Errorf("duplicate created restore directory handle: %w", err),
			cleanupErr,
			closeErr,
		))
	}
	if err := dirFile.Close(); err != nil {
		cleanupErr := deleteCreatedRestoreDirectory(cleanupHandle)
		cleanupCloseErr := windows.CloseHandle(cleanupHandle)
		return closeOnError(errors.Join(
			fmt.Errorf("close created restore directory: %w", err),
			cleanupErr,
			cleanupCloseErr,
		))
	}
	_ = windows.CloseHandle(cleanupHandle)
	return parentLock, created, nil
}

func deleteCreatedRestoreDirectory(handle windows.Handle) error {
	type disposition struct{ DeleteFile bool }
	info := disposition{DeleteFile: true}
	return windows.SetFileInformationByHandle(
		handle,
		windows.FileDispositionInfo,
		(*byte)(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	)
}
