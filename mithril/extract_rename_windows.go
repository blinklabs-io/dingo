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
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// Exclusive publication must never replace a non-directory destination
// implicitly, so this refuses an existing destination the way MoveFile does,
// unlike os.Root.Rename on Windows, which uses MoveFileEx with replacement
// semantics.
//
// Both endpoints are resolved the way removeExtractedFile resolves its
// target, and for the same reason: a path handed to the kernel to resolve on
// its own would let it follow a reparse point substituted for any component
// after extraction's symlink checks ran. Each component is instead walked
// through its parent's handle and confirmed to be the entry the name denotes
// (openVerifiedParent), and those handles are held across the move.
//
// The rename itself is then handle-relative rather than path-based:
// NtSetInformationFile's FileRenameInformation renames the object an
// already-open handle refers to, naming the destination by its verified
// parent's own handle and a single component rather than a path the kernel
// resolves again — see extract_handlerelative_windows.go. That closes the gap
// an earlier version of this comment described and issue #3228 tracked: this
// no longer resolves either parent's name a second time, so a directory
// renamed or deleted out from under a held parent handle no longer redirects
// the move.
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

	oldDirFile, oldDir, err := rootDirHandle(oldParent)
	if err != nil {
		return fmt.Errorf("opening rename source directory: %w", err)
	}
	defer oldDirFile.Close()
	newDirFile, newDir, err := rootDirHandle(newParent)
	if err != nil {
		return fmt.Errorf("opening rename destination directory: %w", err)
	}
	defer newDirFile.Close()

	source, err := openRelativeForRename(oldDir, oldBase)
	if err != nil {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: err}
	}
	defer func() { _ = windows.CloseHandle(source) }()

	if err := renameRelativeHandle(source, newDir, newBase, false); err != nil {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: err}
	}
	return nil
}
