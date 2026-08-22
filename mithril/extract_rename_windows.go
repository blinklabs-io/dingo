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
//
// Both names are resolved the way removeExtractedFile resolves its target,
// and for the same reason: MoveFile addresses paths, so handing it a path
// rebuilt from root.Name() lets it follow a reparse point substituted for any
// component after extraction's symlink checks ran. Each component is instead
// walked through its parent's handle and confirmed to be the entry the name
// denotes (openVerifiedParent), and those handles are held across the move.
//
// What that does and does not buy is worth stating exactly, because an earlier
// version of this comment overstated it. The walk rejects a symlink or reparse
// point that is already in place when it runs, which is the planted-component
// case. It does not make the parents unswappable: os.Root opens every handle
// with FILE_SHARE_DELETE (see internal/syscall/windows/at_windows.go), so
// holding them does not stop another process renaming or deleting a verified
// directory, and MoveFile resolves the parents' own names a second time.
//
// The race between the walk and the call therefore remains. Closing it needs
// NtSetInformationFile with FileRenameInformation against the parent handle,
// which is genuinely handle-relative; see issue #3228.
func renameExtractedDirectory(root *os.Root, oldname, newname string) error {
	oldParent, oldBase, releaseOld, err := openVerifiedParent(root, oldname)
	if err != nil {
		return err
	}
	defer releaseOld()
	newParent, newBase, releaseNew, err := openVerifiedParent(root, newname)
	if err != nil {
		return err
	}
	defer releaseNew()

	oldpath, err := windows.UTF16PtrFromString(
		filepath.Join(oldParent.Name(), oldBase),
	)
	if err != nil {
		return err
	}
	newpath, err := windows.UTF16PtrFromString(
		filepath.Join(newParent.Name(), newBase),
	)
	if err != nil {
		return err
	}
	return windows.MoveFile(oldpath, newpath)
}
