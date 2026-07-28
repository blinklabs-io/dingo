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
	"os"
	"path/filepath"
	"strings"
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

// assertSafeDescendant rejects a symlink or non-directory at any component of
// target below root. Extraction resolves target through those components, so
// one symlink among them relocates the write outside root even when the
// archive's own entry paths are clean.
//
// Components that do not exist yet are fine: this process creates them, and
// the check runs again for every path written.
func assertSafeDescendant(root, target string) error {
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return fmt.Errorf(
			"%w: %s is not under %s", ErrExtractUnsafePath, target, root,
		)
	}
	if rel == "." {
		return nil
	}
	if rel == ".." || strings.HasPrefix(
		rel, ".."+string(filepath.Separator),
	) {
		return fmt.Errorf(
			"%w: %s escapes %s", ErrExtractUnsafePath, target, root,
		)
	}

	current := root
	for component := range strings.SplitSeq(rel, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				// Nothing below an absent component can exist either.
				return nil
			}
			return fmt.Errorf("inspecting %s: %w", current, err)
		}
		if err := assertRealDir(current, info); err != nil {
			return err
		}
	}
	return nil
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

// prepareExtractDestination applies the destination policy and returns the
// directory extraction should write into, plus a publish function to run on
// success and a cleanup function to run unconditionally.
//
// In exclusive mode the returned directory is a freshly created 0700 staging
// directory alongside the destination, so a partial extraction is never
// visible at the destination path and a pre-existing entry there can never
// be written through. Publishing renames the staging directory into place.
func prepareExtractDestination(
	destDir string,
	cfg extractConfig,
) (workDir string, publish func() error, cleanup func(), err error) {
	cleanDest := filepath.Clean(destDir)

	// Checked before anything is created or read, so a destination that is
	// itself a symlink never gets followed.
	if err := assertSafeExtractRoot(cleanDest); err != nil {
		return "", nil, nil, err
	}

	if cfg.merge {
		if err := os.MkdirAll(cleanDest, 0o750); err != nil {
			return "", nil, nil, fmt.Errorf(
				"creating extraction directory: %w", err,
			)
		}
		// Re-check after creation: MkdirAll succeeds against a symlink
		// that already resolves to a directory.
		if err := assertSafeExtractRoot(cleanDest); err != nil {
			return "", nil, nil, err
		}
		return cleanDest, func() error { return nil }, func() {}, nil
	}

	if !cfg.replace {
		empty, err := dirIsEmpty(cleanDest)
		if err != nil {
			return "", nil, nil, fmt.Errorf(
				"inspecting extraction destination: %w", err,
			)
		}
		if !empty {
			return "", nil, nil, fmt.Errorf(
				"%w: %s", ErrExtractDestinationNotEmpty, cleanDest,
			)
		}
	}

	parent := filepath.Dir(cleanDest)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return "", nil, nil, fmt.Errorf(
			"creating extraction parent directory: %w", err,
		)
	}

	// Staged alongside the destination so publishing is a rename within one
	// filesystem. MkdirTemp creates with 0700 and never reuses a name.
	staging, err := os.MkdirTemp(parent, ".extract-*")
	if err != nil {
		return "", nil, nil, fmt.Errorf(
			"creating extraction staging directory: %w", err,
		)
	}

	cleanup = func() { _ = os.RemoveAll(staging) }
	publish = func() error {
		// Remove whatever occupies the destination. This discards a
		// pre-existing symlink rather than following it, because RemoveAll
		// unlinks the symlink itself.
		if err := os.RemoveAll(cleanDest); err != nil {
			return fmt.Errorf("clearing extraction destination: %w", err)
		}
		if err := os.Rename(staging, cleanDest); err != nil {
			return fmt.Errorf("publishing extraction: %w", err)
		}
		return nil
	}
	return staging, publish, cleanup, nil
}

// createExtractedFile opens target for writing, refusing to follow a symlink
// at the final component and rejecting an unsafe parent chain first.
func createExtractedFile(root, target string) (*os.File, error) {
	if err := assertSafeDescendant(root, filepath.Dir(target)); err != nil {
		return nil, err
	}
	// An existing symlink at the target itself is rejected outright, which
	// also covers platforms where the no-follow open flag is unavailable.
	info, err := os.Lstat(target)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf(
				"%w: %s is a symlink", ErrExtractUnsafePath, target,
			)
		}
	case !os.IsNotExist(err):
		return nil, fmt.Errorf("inspecting %s: %w", target, err)
	}
	file, err := os.OpenFile( //nolint:gosec // caller validated target against the destination root
		target,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC|openNoFollowFlag,
		0o640,
	)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// mkdirExtracted creates an extracted directory after checking that no
// component of its path is a symlink.
func mkdirExtracted(root, target string) error {
	if err := assertSafeDescendant(root, target); err != nil {
		return err
	}
	if err := os.MkdirAll(target, 0o750); err != nil { //nolint:gosec // caller validated target against the destination root
		return err
	}
	// MkdirAll is a no-op against a symlink that already resolves to a
	// directory, so confirm what now exists is a real directory.
	return assertSafeDescendant(root, target)
}
