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
// denotes (openVerifiedParent), and those handles are held across the move --
// Windows refuses to rename a directory while handles are open on it, so the
// verified parents cannot be swapped underneath the call.
//
// The residual limit is the one removeExtractedFile carries: Windows offers no
// handle-relative rename, so the immediate parents' own names are resolved a
// second time by MoveFile. Closing that needs NtSetInformationFile with
// FileRenameInformation against the parent handle; see issue #3228.
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
