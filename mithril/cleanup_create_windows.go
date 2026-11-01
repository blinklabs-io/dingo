//go:build windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package mithril

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/blinklabs-io/dingo/internal/securefs"
	"golang.org/x/sys/windows"
)

func canonicalBootstrapParent(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absPath), nil
}

func retainBootstrapDirectoryLocks(
	path string,
	_ *os.Root,
	_ *os.Root,
	opened os.FileInfo,
) (io.Closer, io.Closer, error) {
	lock, err := securefs.LockWindowsPath(path)
	if err != nil {
		return nil, nil, err
	}
	if !os.SameFile(opened, lock.Info()) {
		return nil, nil, errors.Join(
			fmt.Errorf("%w: bootstrap directory changed", errBootstrapCleanupIdentity),
			lock.Close(),
		)
	}
	return lock, nil, nil
}

func createPrivateBootstrapDirectory(
	parent *os.Root,
	parentPath string,
	name string,
) (bootstrapCreation, error) {
	parentLock, err := securefs.LockWindowsPath(parentPath)
	if err != nil {
		return bootstrapCreation{}, err
	}
	fail := func(primary error) (bootstrapCreation, error) {
		return bootstrapCreation{}, errors.Join(primary, parentLock.Close())
	}
	named, err := parent.Stat(".")
	if err != nil {
		return fail(err)
	}
	opened := parentLock.Info()
	if !os.SameFile(named, opened) {
		return fail(errors.Join(
			fmt.Errorf("%w: bootstrap parent changed", errBootstrapCleanupIdentity),
		))
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return fail(err)
	}
	owner := user.User.Sid.String()
	sd, err := windows.SecurityDescriptorFromString(
		"O:" + owner + "G:" + owner + "D:P" +
			"(A;OICI;FA;;;" + owner + ")" +
			"(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)",
	)
	if err != nil {
		return fail(err)
	}
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return fail(err)
	}
	oa := &windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: parentLock.Handle(), ObjectName: objectName,
		Attributes: windows.OBJ_CASE_INSENSITIVE, SecurityDescriptor: sd,
	}
	var handle windows.Handle
	var iosb windows.IO_STATUS_BLOCK
	if err := windows.NtCreateFile(
		&handle,
		windows.FILE_READ_ATTRIBUTES,
		oa, &iosb, nil, windows.FILE_ATTRIBUTE_DIRECTORY,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		windows.FILE_CREATE,
		windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT,
		0, 0,
	); err != nil {
		return fail(err)
	}
	file := os.NewFile(uintptr(handle), name)
	created, err := file.Stat()
	named, namedErr := parent.Lstat(name)
	if err != nil || namedErr != nil || created == nil ||
		named.Mode()&os.ModeSymlink != 0 || !named.IsDir() ||
		!os.SameFile(created, named) {
		closeErr := file.Close()
		cleanupErr := cleanupCreatedBootstrapDir(parent, name, filepath.Join(parentPath, name), named)
		return fail(errors.Join(
			err,
			namedErr,
			fmt.Errorf("%w: created bootstrap directory changed", errBootstrapCleanupIdentity),
			closeErr,
			cleanupErr,
		))
	}
	return bootstrapCreation{
		initialLock: file,
		parentLock:  parentLock,
		info:        created,
	}, nil
}
