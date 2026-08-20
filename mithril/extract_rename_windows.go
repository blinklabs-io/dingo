//go:build windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package mithril

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// MoveFile refuses an existing destination, unlike os.Root.Rename on
// Windows, which uses MoveFileEx with replacement semantics. Exclusive
// publication must never replace a non-directory destination implicitly.
func renameExtractedDirectory(root *os.Root, oldname, newname string) error {
	oldpath, err := windows.UTF16PtrFromString(filepath.Join(root.Name(), oldname))
	if err != nil {
		return err
	}
	newpath, err := windows.UTF16PtrFromString(filepath.Join(root.Name(), newname))
	if err != nil {
		return err
	}
	return windows.MoveFile(oldpath, newpath)
}
