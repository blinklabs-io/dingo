//go:build unix

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package mithril

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/internal/securefs"
)

// bootstrapFileGuard records that every directory which can redirect an
// archive pathname is owned by the current user or root and is not writable
// by another user. The retained parent root then keeps all archive operations
// relative to the directory whose identity was checked.
type bootstrapFileGuard struct{}

func retainBootstrapFileGuard(
	path string,
	root *os.Root,
	expected os.FileInfo,
) (*bootstrapFileGuard, error) {
	parent, err := canonicalBootstrapParent(filepath.Dir(filepath.Clean(path)))
	if err != nil {
		return nil, err
	}
	if err := validateBootstrapAncestorChain(
		parent,
		uint32(os.Geteuid()), //nolint:gosec // Unix kernel UIDs are uint32 values.
		os.Lstat,
	); err != nil {
		return nil, err
	}
	namedParent, err := os.Lstat(parent)
	if err != nil {
		return nil, err
	}
	openedParent, err := root.Stat(".")
	if err != nil || !os.SameFile(namedParent, openedParent) {
		return nil, errors.Join(
			fmt.Errorf("%w: archive parent changed", errBootstrapCleanupIdentity),
			err,
		)
	}
	if expected == nil {
		return nil, fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path)
	}
	return &bootstrapFileGuard{}, nil
}

func (g *bootstrapFileGuard) remove(
	root *os.Root,
	path string,
	expected os.FileInfo,
) error {
	name := filepath.Base(filepath.Clean(path))
	named, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if named.Mode()&os.ModeSymlink != 0 || !named.Mode().IsRegular() ||
		!os.SameFile(expected, named) {
		return fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path)
	}
	quarantine := ".dingo-mithril-cleanup-" + rand.Text()
	if err := securefs.RenameNoReplace(root, name, quarantine); err != nil {
		return err
	}
	moved, err := root.Lstat(quarantine)
	if err != nil || moved.Mode()&os.ModeSymlink != 0 ||
		!moved.Mode().IsRegular() || !os.SameFile(expected, moved) {
		identityErr := fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path)
		if err != nil {
			identityErr = fmt.Errorf(
				"%w: inspect %q: %w", errBootstrapCleanupIdentity, path, err,
			)
		}
		rollbackErr := securefs.RenameNoReplace(root, quarantine, name)
		if rollbackErr != nil {
			rollbackErr = fmt.Errorf("restore archive name: %w", rollbackErr)
		}
		return errors.Join(identityErr, rollbackErr)
	}
	// The verified parent chain excludes untrusted renames while the archive is
	// live, so this non-recursive unlink still names the retained object.
	return removeExtractedFile(root, quarantine, path)
}

func (g *bootstrapFileGuard) Close() error { return nil }
