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
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/internal/securefs"
	"golang.org/x/sys/windows"
)

type bootstrapFileGuard struct {
	file       *os.File
	parentLock *securefs.WindowsPathLock
}

func retainBootstrapFileGuard(
	path string,
	root *os.Root,
	expected os.FileInfo,
) (*bootstrapFileGuard, error) {
	parentLock, err := securefs.LockWindowsPath(filepath.Dir(filepath.Clean(path)))
	if err != nil {
		return nil, err
	}
	fail := func(primary error) (*bootstrapFileGuard, error) {
		return nil, errors.Join(primary, parentLock.Close())
	}
	openedParent, err := root.Stat(".")
	if err != nil || !os.SameFile(openedParent, parentLock.Info()) {
		return fail(errors.Join(
			fmt.Errorf("%w: archive parent changed", errBootstrapCleanupIdentity),
			err,
		))
	}
	dirFile, dir, err := rootDirHandle(root)
	if err != nil {
		return fail(err)
	}
	handle, err := openRelativeWithShare(
		dir,
		filepath.Base(filepath.Clean(path)),
		windows.FILE_READ_ATTRIBUTES|windows.DELETE,
		windows.FILE_OPEN_REPARSE_POINT|
			windows.FILE_OPEN_FOR_BACKUP_INTENT|
			windows.FILE_NON_DIRECTORY_FILE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
	)
	closeDirErr := dirFile.Close()
	if err != nil {
		return fail(errors.Join(err, closeDirErr))
	}
	file := os.NewFile(uintptr(handle), path)
	opened, statErr := file.Stat()
	if statErr != nil || expected == nil || !os.SameFile(expected, opened) {
		return fail(errors.Join(
			fmt.Errorf("%w: archive changed: %q", errBootstrapCleanupIdentity, path),
			statErr,
			file.Close(),
			closeDirErr,
		))
	}
	if closeDirErr != nil {
		return fail(errors.Join(closeDirErr, file.Close()))
	}
	return &bootstrapFileGuard{file: file, parentLock: parentLock}, nil
}

func (g *bootstrapFileGuard) remove(
	_ *os.Root,
	path string,
	_ os.FileInfo,
) error {
	if g == nil || g.file == nil {
		return fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path)
	}
	return setDeleteDisposition(windows.Handle(g.file.Fd()))
}

func (g *bootstrapFileGuard) Close() error {
	if g == nil {
		return nil
	}
	var errs []error
	if g.file != nil {
		errs = append(errs, g.file.Close())
		g.file = nil
	}
	if g.parentLock != nil {
		errs = append(errs, g.parentLock.Close())
		g.parentLock = nil
	}
	return errors.Join(errs...)
}
