//go:build windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package securefs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

// WindowsPathLock retains no-delete handles on every real directory from the
// volume root through a path. Pathname consumers remain bound to that ancestor
// chain until Close.
type WindowsPathLock struct {
	files []*os.File
	info  os.FileInfo
}

// LockWindowsPath locks every component of path against rename or deletion.
func LockWindowsPath(path string) (*WindowsPathLock, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	absPath = filepath.Clean(absPath)
	volume := filepath.VolumeName(absPath)
	if volume == "" {
		return nil, fmt.Errorf("path %q has no Windows volume", path)
	}
	root := volume + string(os.PathSeparator)
	rel, err := filepath.Rel(root, absPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return nil, errors.Join(fmt.Errorf("path %q is outside its volume", path), err)
	}
	paths := []string{root}
	current := root
	if rel != "." {
		for _, component := range strings.Split(rel, string(os.PathSeparator)) {
			current = filepath.Join(current, component)
			paths = append(paths, current)
		}
	}
	locked := &WindowsPathLock{}
	for _, current := range paths {
		pathPtr, err := windows.UTF16PtrFromString(current)
		if err != nil {
			return nil, errors.Join(err, locked.Close())
		}
		handle, err := windows.CreateFile(
			pathPtr,
			windows.FILE_READ_ATTRIBUTES,
			windows.FILE_SHARE_READ,
			nil,
			windows.OPEN_EXISTING,
			windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
			0,
		)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("lock path ancestor %q: %w", current, err),
				locked.Close(),
			)
		}
		file := os.NewFile(uintptr(handle), current)
		var nativeInfo windows.ByHandleFileInformation
		nativeErr := windows.GetFileInformationByHandle(handle, &nativeInfo)
		info, err := file.Stat()
		named, namedErr := os.Lstat(current)
		if nativeErr != nil ||
			nativeInfo.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 ||
			err != nil || namedErr != nil || !info.IsDir() ||
			info.Mode()&os.ModeSymlink != 0 ||
			!named.IsDir() || named.Mode()&os.ModeSymlink != 0 ||
			!os.SameFile(info, named) {
			closeErr := file.Close()
			return nil, errors.Join(
				fmt.Errorf("path ancestor %q is not a real directory", current),
				nativeErr,
				err,
				namedErr,
				closeErr,
				locked.Close(),
			)
		}
		locked.files = append(locked.files, file)
		locked.info = info
	}
	return locked, nil
}

// Handle returns the handle for the final directory in the locked path.
func (l *WindowsPathLock) Handle() windows.Handle {
	if l == nil || len(l.files) == 0 {
		return windows.InvalidHandle
	}
	return windows.Handle(l.files[len(l.files)-1].Fd())
}

// Info returns the identity captured from the final directory.
func (l *WindowsPathLock) Info() os.FileInfo {
	if l == nil {
		return nil
	}
	return l.info
}

// Close releases the path locks from leaf to root.
func (l *WindowsPathLock) Close() error {
	if l == nil {
		return nil
	}
	var errs []error
	for i := len(l.files) - 1; i >= 0; i-- {
		errs = append(errs, l.files[i].Close())
	}
	l.files = nil
	return errors.Join(errs...)
}
