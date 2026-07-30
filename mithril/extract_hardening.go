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

package mithril

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Errors reported when an extraction destination cannot be trusted. The
// archive-content checks (Zip Slip, symlink entries) guard what the archive
// asks for; these guard the filesystem the archive is written into, which an
// attacker may have reached first.
var (
	// ErrExtractDestinationNotEmpty reports an exclusive extraction whose
	// destination already holds content.
	ErrExtractDestinationNotEmpty = errors.New(
		"mithril: extraction destination is not empty",
	)
	// ErrExtractUnsafePath reports a destination path component that is a
	// symlink, or an existing non-directory where a directory is required.
	ErrExtractUnsafePath = errors.New(
		"mithril: unsafe extraction path",
	)
)

// extractConfig holds the resolved destination policy for one extraction.
type extractConfig struct {
	merge   bool
	replace bool
}

// ExtractOption configures how ExtractArchive treats its destination.
type ExtractOption func(*extractConfig)

// WithMergeIntoDestination extracts directly into the destination, adding to
// whatever is already there.
//
// This exists for destinations that several archives populate together — the
// parallel immutable-archive download builds one directory from many
// archives, so it can neither refuse a non-empty destination nor swap the
// directory out from under a concurrent extraction. Merging forgoes the
// private-staging guarantee, so every write still goes through the
// per-component symlink checks below.
func WithMergeIntoDestination() ExtractOption {
	return func(c *extractConfig) { c.merge = true }
}

// WithReplaceDestination allows an exclusive extraction to proceed when the
// destination already holds content, replacing it with the freshly extracted
// tree instead of refusing.
//
// This is the recovery path for a destination left behind by an interrupted
// or superseded run. Replacement is a swap of a directory staged elsewhere,
// never a write into the existing one, so pre-existing content is discarded
// rather than merged with or written through.
func WithReplaceDestination() ExtractOption {
	return func(c *extractConfig) { c.replace = true }
}

func newExtractConfig(opts []ExtractOption) extractConfig {
	var cfg extractConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// assertSafeExtractRoot checks the extraction root itself, rejecting a root
// that is a symlink or an existing non-directory.
//
// Only the root entry is inspected, never its ancestors. Directories above the
// root are chosen by the operator and are outside the threat this guards
// against, which is content planted inside the destination. Walking to the
// filesystem root would also reject ordinary layouts: on macOS every temporary
// path sits under /var, which is a symlink to /private/var.
func assertSafeExtractRoot(root string) error {
	info, err := os.Lstat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("inspecting %s: %w", root, err)
	}
	return assertRealDir(root, info)
}

// assertRealDir rejects a path that is a symlink or is not a directory.
func assertRealDir(path string, info os.FileInfo) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf(
			"%w: %s is a symlink", ErrExtractUnsafePath, path,
		)
	}
	if !info.IsDir() {
		return fmt.Errorf(
			"%w: %s is not a directory", ErrExtractUnsafePath, path,
		)
	}
	return nil
}

// dirIsEmpty reports whether dir exists and contains no entries. A missing
// directory counts as empty.
func dirIsEmpty(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	return len(entries) == 0, nil
}

// rootEntryIsEmptyDir reports whether name under root is absent or an empty
// directory, read through the handle so the answer describes the same entry
// the caller acted on.
//
// This classifies a failed rename; it is never used to decide whether
// removing something is safe, because that decision cannot be separated from
// the removal without a race.
func rootEntryIsEmptyDir(root *os.Root, name string) (bool, error) {
	dir, err := root.Open(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}
	defer dir.Close()
	entries, err := dir.ReadDir(1)
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	return len(entries) == 0, nil
}

// prepareExtractDestination applies the destination policy and returns a
// directory handle extraction must write through, plus a publish function to
// run on success and a cleanup function to run unconditionally.
//
// The handle is what makes the guarantee hold under mutation. Every write is
// resolved relative to it rather than by re-walking a pathname, so a directory
// swapped after a check cannot redirect a later write, and an entry resolving
// outside the root is refused by the runtime rather than by an inspection that
// raced. Checking components and then opening by path cannot offer that.
//
// In exclusive mode the handle is on a freshly created 0700 staging directory
// alongside the destination, so a partial extraction is never visible at the
// destination path. Publishing renames the staging directory into place.
func prepareExtractDestination(
	destDir string,
	cfg extractConfig,
) (root *os.Root, publish func() error, cleanup func(), err error) {
	cleanDest := filepath.Clean(destDir)

	// Checked before anything is created or read, so a destination that is
	// itself a symlink never gets followed.
	if err := assertSafeExtractRoot(cleanDest); err != nil {
		return nil, nil, nil, err
	}

	if cfg.merge {
		if err := os.MkdirAll(cleanDest, 0o750); err != nil {
			return nil, nil, nil, fmt.Errorf(
				"creating extraction directory: %w", err,
			)
		}
		// Re-check after creation: MkdirAll succeeds against a symlink
		// that already resolves to a directory.
		if err := assertSafeExtractRoot(cleanDest); err != nil {
			return nil, nil, nil, err
		}
		mergeRoot, err := os.OpenRoot(cleanDest)
		if err != nil {
			return nil, nil, nil, fmt.Errorf(
				"%w: opening extraction root %s: %w",
				ErrExtractUnsafePath, cleanDest, err,
			)
		}
		return mergeRoot,
			func() error { return nil },
			func() { _ = mergeRoot.Close() },
			nil
	}

	if !cfg.replace {
		empty, err := dirIsEmpty(cleanDest)
		if err != nil {
			return nil, nil, nil, fmt.Errorf(
				"inspecting extraction destination: %w", err,
			)
		}
		if !empty {
			return nil, nil, nil, fmt.Errorf(
				"%w: %s", ErrExtractDestinationNotEmpty, cleanDest,
			)
		}
	}

	parent := filepath.Dir(cleanDest)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return nil, nil, nil, fmt.Errorf(
			"creating extraction parent directory: %w", err,
		)
	}
	// The parent is held open for the whole extraction so publication can be
	// performed relative to this handle. Renaming and removing are otherwise
	// pathname-based, and a parent replaced after any check would redirect
	// them however recently that check ran.
	parentRoot, err := os.OpenRoot(parent)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(
			"%w: opening extraction parent %s: %w",
			ErrExtractUnsafePath, parent, err,
		)
	}

	// Staged alongside the destination so publishing is a rename within one
	// filesystem. MkdirTemp creates with 0700 and never reuses a name.
	staging, err := os.MkdirTemp(parent, ".extract-*")
	if err != nil {
		_ = parentRoot.Close()
		return nil, nil, nil, fmt.Errorf(
			"creating extraction staging directory: %w", err,
		)
	}
	stagingName := filepath.Base(staging)
	destName := filepath.Base(cleanDest)
	stagingRoot, err := parentRoot.OpenRoot(stagingName)
	if err != nil {
		_ = os.RemoveAll(staging)
		_ = parentRoot.Close()
		return nil, nil, nil, fmt.Errorf(
			"%w: opening staging root %s: %w",
			ErrExtractUnsafePath, staging, err,
		)
	}

	cleanup = func() {
		_ = stagingRoot.Close()
		// Removal goes through the parent handle for the same reason
		// publication does.
		_ = parentRoot.RemoveAll(stagingName)
		_ = parentRoot.Close()
	}
	publish = func() error {
		// Release the handle before moving the directory it refers to.
		if err := stagingRoot.Close(); err != nil {
			return fmt.Errorf("closing staging root: %w", err)
		}
		// Replacing is destructive by request, so the existing destination
		// goes first. RemoveAll unlinks a symlink at the destination rather
		// than following it, and resolving through the parent handle means a
		// swapped parent cannot redirect it.
		if cfg.replace {
			if err := parentRoot.RemoveAll(destName); err != nil {
				return fmt.Errorf(
					"clearing extraction destination: %w", err,
				)
			}
		}
		// Without an explicit replacement request nothing is deleted at all.
		// Checking that the destination is empty and then removing it can
		// never be made safe, however narrow the gap: a writer populating it
		// in between loses their content. Renaming onto the destination
		// instead lets the kernel decide atomically — it refuses when
		// anything occupies the name, and succeeds when the destination is
		// absent or an empty directory, so no content can be destroyed by
		// this path.
		if err := parentRoot.Rename(stagingName, destName); err != nil {
			if !cfg.replace {
				occupied, checkErr := rootEntryIsEmptyDir(parentRoot, destName)
				if checkErr == nil && !occupied {
					return fmt.Errorf(
						"%w: %s", ErrExtractDestinationNotEmpty, cleanDest,
					)
				}
			}
			return fmt.Errorf("publishing extraction: %w", err)
		}
		return nil
	}
	return stagingRoot, publish, cleanup, nil
}

// createExtractedFile opens name for writing relative to the extraction root.
//
// The root refuses any name resolving outside it, so containment does not
// depend on the lstat below. That check exists to reject writing through a
// symlink that sits inside the root, which an archive should never do, and to
// give a clearer error than the runtime would; O_NOFOLLOW makes the final
// component race-free where the platform supports it.
func createExtractedFile(root *os.Root, name string) (*os.File, error) {
	info, err := root.Lstat(name)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf(
				"%w: %s is a symlink", ErrExtractUnsafePath, name,
			)
		}
	case !errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf(
			"%w: inspecting %s: %w", ErrExtractUnsafePath, name, err,
		)
	}
	file, err := root.OpenFile(
		name,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC|openNoFollowFlag,
		0o640,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: opening %s: %w", ErrExtractUnsafePath, name, err,
		)
	}
	return file, nil
}

// mkdirExtracted creates a directory relative to the extraction root.
//
// A name resolving outside the root is refused by the root itself. A symlink
// at the name is rejected here so extraction never populates a directory the
// archive did not create, even one pointing back inside the root.
func mkdirExtracted(root *os.Root, name string) error {
	if name == "." || name == "" {
		return nil
	}
	info, err := root.Lstat(name)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf(
				"%w: %s is a symlink", ErrExtractUnsafePath, name,
			)
		}
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf(
			"%w: inspecting %s: %w", ErrExtractUnsafePath, name, err,
		)
	}
	if err := root.MkdirAll(name, 0o750); err != nil {
		return fmt.Errorf(
			"%w: creating %s: %w", ErrExtractUnsafePath, name, err,
		)
	}
	return nil
}
