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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

func canonicalBootstrapParent(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve bootstrap parent %q: %w", path, err)
	}
	resolved, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return "", fmt.Errorf("resolve bootstrap parent %q: %w", path, err)
	}
	return filepath.Clean(resolved), nil
}

func retainBootstrapDirectoryLocks(
	path string,
	_ *os.Root,
	root *os.Root,
	opened os.FileInfo,
) (io.Closer, io.Closer, error) {
	canonical, err := canonicalBootstrapParent(path)
	if err != nil {
		return nil, nil, err
	}
	if err := validateBootstrapAncestorChain(
		canonical,
		uint32(os.Geteuid()), //nolint:gosec // Unix kernel UIDs are uint32 values.
		os.Lstat,
	); err != nil {
		return nil, nil, err
	}
	named, err := os.Lstat(canonical)
	if err != nil || !os.SameFile(named, opened) {
		return nil, nil, errors.Join(
			fmt.Errorf("%w: bootstrap directory changed", errBootstrapCleanupIdentity),
			err,
		)
	}
	reopened, err := root.Stat(".")
	if err != nil || !os.SameFile(opened, reopened) {
		return nil, nil, errors.Join(
			fmt.Errorf("%w: bootstrap directory changed", errBootstrapCleanupIdentity),
			err,
		)
	}
	return nil, nil, nil
}

func createPrivateBootstrapDirectory(
	parent *os.Root,
	parentPath string,
	name string,
) (bootstrapCreation, error) {
	if err := validateBootstrapAncestorChain(
		parentPath,
		uint32(os.Geteuid()), //nolint:gosec // Unix kernel UIDs are uint32 values.
		os.Lstat,
	); err != nil {
		return bootstrapCreation{}, err
	}
	namedParent, err := os.Lstat(filepath.Clean(parentPath))
	if err != nil {
		return bootstrapCreation{}, err
	}
	openedParent, err := parent.Stat(".")
	if err != nil || !os.SameFile(namedParent, openedParent) {
		return bootstrapCreation{}, errors.Join(
			fmt.Errorf("%w: bootstrap parent changed", errBootstrapCleanupIdentity),
			err,
		)
	}
	if err := parent.Mkdir(name, 0o700); err != nil {
		return bootstrapCreation{}, err
	}
	created, err := parent.Lstat(name)
	if err != nil {
		return bootstrapCreation{}, errors.Join(err, parent.Remove(name))
	}
	return bootstrapCreation{info: created}, nil
}

func validateBootstrapAncestorChain(
	path string,
	euid uint32,
	lstat func(string) (os.FileInfo, error),
) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve bootstrap parent %q: %w", path, err)
	}
	for current := filepath.Clean(absPath); ; current = filepath.Dir(current) {
		info, err := lstat(current)
		if err != nil {
			return fmt.Errorf("inspect bootstrap parent ancestor %q: %w", current, err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf(
				"bootstrap parent ancestor %q is not a real directory", current,
			)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("inspect bootstrap parent ancestor %q owner", current)
		}
		if stat.Uid != euid && stat.Uid != 0 {
			return fmt.Errorf(
				"bootstrap parent ancestor %q is owned by UID %d", current, stat.Uid,
			)
		}
		if info.Mode().Perm()&0o022 != 0 && info.Mode()&os.ModeSticky == 0 {
			return fmt.Errorf(
				"bootstrap parent ancestor %q is writable by group or other without the sticky bit",
				current,
			)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
	}
}
