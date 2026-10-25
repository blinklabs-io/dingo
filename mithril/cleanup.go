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
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

var errBootstrapCleanupIdentity = errors.New(
	"mithril cleanup directory no longer has its verified identity",
)

type bootstrapFile struct {
	path  string
	root  *os.Root
	info  os.FileInfo
	guard *bootstrapFileGuard
}

type bootstrapDir struct {
	parent     *os.Root
	root       *os.Root
	info       os.FileInfo
	name       string
	path       string
	childLock  io.Closer
	parentLock io.Closer
}

type bootstrapCreation struct {
	initialLock io.Closer
	parentLock  io.Closer
	info        os.FileInfo
}

func retainBootstrapDir(path string) (*bootstrapDir, error) {
	return retainBootstrapDirExpected(path, nil)
}

func retainBootstrapDirExpected(
	path string,
	expected os.FileInfo,
) (*bootstrapDir, error) {
	path = filepath.Clean(path)
	parent, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	name := filepath.Base(path)
	named, err := parent.Lstat(name)
	if err != nil || named.Mode()&os.ModeSymlink != 0 || !named.IsDir() {
		_ = parent.Close()
		return nil, errors.Join(
			fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path), err,
		)
	}
	if expected != nil && !os.SameFile(expected, named) {
		_ = parent.Close()
		return nil, fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path)
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		_ = parent.Close()
		return nil, err
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(named, opened) {
		_ = root.Close()
		_ = parent.Close()
		return nil, errors.Join(
			fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path), err,
		)
	}
	childLock, parentLock, err := retainBootstrapDirectoryLocks(
		path, parent, root, opened,
	)
	if err != nil {
		_ = root.Close()
		_ = parent.Close()
		return nil, err
	}
	return &bootstrapDir{
		parent: parent, root: root, info: opened, name: name, path: path,
		childLock: childLock, parentLock: parentLock,
	}, nil
}

func createBootstrapDir(parent, pattern string) (string, *bootstrapDir, error) {
	return createBootstrapDirWithCreator(
		parent, pattern, createPrivateBootstrapDirectory,
	)
}

func createBootstrapDirNamed(
	parent, name string,
	create func(*os.Root, string, string) (bootstrapCreation, error),
) (string, *bootstrapDir, error) {
	return createBootstrapDirWithNameCreator(parent, name, create)
}

func createBootstrapDirWithCreator(
	parent, pattern string,
	create func(*os.Root, string, string) (bootstrapCreation, error),
) (string, *bootstrapDir, error) {
	name := strings.TrimSuffix(pattern, "*") + rand.Text()
	return createBootstrapDirWithNameCreator(parent, name, create)
}

func createBootstrapDirWithNameCreator(
	parent, name string,
	create func(*os.Root, string, string) (bootstrapCreation, error),
) (string, *bootstrapDir, error) {
	if parent == "" {
		parent = os.TempDir()
	}
	var err error
	parent, err = canonicalBootstrapParent(parent)
	if err != nil {
		return "", nil, err
	}
	parentRoot, err := os.OpenRoot(parent)
	if err != nil {
		return "", nil, err
	}
	if name == "" || name != filepath.Base(name) || name == "." || name == ".." {
		_ = parentRoot.Close()
		return "", nil, fmt.Errorf("invalid bootstrap directory name %q", name)
	}
	path := filepath.Join(parent, name)
	creation, err := create(parentRoot, parent, name)
	if err != nil {
		_ = parentRoot.Close()
		return "", nil, err
	}
	root, openErr := parentRoot.OpenRoot(name)
	var opened os.FileInfo
	if openErr == nil {
		opened, openErr = root.Stat(".")
	}
	if openErr != nil || creation.info == nil || opened == nil ||
		opened.Mode()&os.ModeSymlink != 0 ||
		!opened.IsDir() || !os.SameFile(creation.info, opened) {
		if root != nil {
			_ = root.Close()
		}
		lockErr := closeBootstrapCreationLock(creation.initialLock)
		cleanupErr := cleanupCreatedBootstrapDir(
			parentRoot, name, path, creation.info,
		)
		parentLockErr := closeBootstrapCreationLock(creation.parentLock)
		_ = parentRoot.Close()
		return "", nil, errors.Join(
			fmt.Errorf("%w: %q changed during creation", errBootstrapCleanupIdentity, path),
			openErr, lockErr, cleanupErr, parentLockErr,
		)
	}
	return path, &bootstrapDir{
		parent: parentRoot, root: root, info: opened, name: name, path: path,
		childLock: creation.initialLock, parentLock: creation.parentLock,
	}, nil
}

func retainOrCreateBootstrapDir(path string) (string, *bootstrapDir, error) {
	owned, err := retainBootstrapDir(path)
	if err == nil {
		return owned.path, owned, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", nil, err
	}
	clean := filepath.Clean(path)
	return createBootstrapDirNamed(
		filepath.Dir(clean), filepath.Base(clean), createPrivateBootstrapDirectory,
	)
}

func closeBootstrapCreationLock(lock io.Closer) error {
	if lock == nil {
		return nil
	}
	return lock.Close()
}

func cleanupCreatedBootstrapDir(
	parent *os.Root,
	name, path string,
	created os.FileInfo,
) error {
	return cleanupCreatedBootstrapDirWithRemover(
		parent, name, path, created, removeEmptyExtractDir,
	)
}

func cleanupCreatedBootstrapDirWithRemover(
	parent *os.Root,
	name, path string,
	created os.FileInfo,
	remove emptyDirRemover,
) error {
	named, err := parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if created == nil || named.Mode()&os.ModeSymlink != 0 || !named.IsDir() ||
		!os.SameFile(created, named) {
		return fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path)
	}
	err = remove(parent, name, path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (d *bootstrapDir) cleanup() error {
	return d.cleanupKeeping(nil)
}

func (d *bootstrapDir) close() error {
	if d == nil {
		return nil
	}
	var rootErr error
	if d.root != nil {
		rootErr = d.root.Close()
		d.root = nil
	}
	return errors.Join(rootErr, d.closeParentHandles())
}

func (d *bootstrapDir) cleanupKeeping(keep map[string]struct{}) error {
	if d == nil {
		return nil
	}
	err := cleanupRetainedDirKeeping(
		d.parent, &d.root, d.info, d.name, d.path, keep, d.closeChildLock,
	)
	return errors.Join(err, d.closeParentHandles())
}

func (d *bootstrapDir) closeChildLock() error {
	if d == nil || d.childLock == nil {
		return nil
	}
	err := d.childLock.Close()
	d.childLock = nil
	return err
}

func (d *bootstrapDir) closeParentHandles() error {
	if d == nil {
		return nil
	}
	var errs []error
	errs = append(errs, d.closeChildLock())
	if d.parentLock != nil {
		errs = append(errs, d.parentLock.Close())
		d.parentLock = nil
	}
	if d.parent != nil {
		errs = append(errs, d.parent.Close())
		d.parent = nil
	}
	return errors.Join(errs...)
}

func retainBootstrapFile(path string, root *os.Root) (*bootstrapFile, error) {
	info, err := root.Lstat(filepath.Base(filepath.Clean(path)))
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		_ = root.Close()
		return nil, fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path)
	}
	guard, err := retainBootstrapFileGuard(path, root, info)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	return &bootstrapFile{path: path, root: root, info: info, guard: guard}, nil
}

func (f *bootstrapFile) cleanup() error {
	if f == nil {
		return nil
	}
	return cleanupBootstrapFile(&f.root, f.path, f.info, &f.guard)
}

func (f *bootstrapFile) close() error {
	if f == nil {
		return nil
	}
	var errs []error
	if f.guard != nil {
		errs = append(errs, f.guard.Close())
		f.guard = nil
	}
	if f.root != nil {
		errs = append(errs, f.root.Close())
		f.root = nil
	}
	return errors.Join(errs...)
}

func cleanupVettedDir(dir *vettedDir) error {
	if dir == nil {
		return nil
	}
	return cleanupBootstrapDir(&dir.root, dir.path, nil)
}

func cleanupRetainedDir(
	parent *os.Root,
	root **os.Root,
	info os.FileInfo,
	name, path string,
) error {
	return cleanupRetainedDirKeeping(parent, root, info, name, path, nil, nil)
}

func cleanupRetainedDirKeeping(
	parent *os.Root,
	root **os.Root,
	info os.FileInfo,
	name, path string,
	keep map[string]struct{},
	beforeRemove func() error,
) error {
	return cleanupRetainedDirKeepingWithRemover(
		parent, root, info, name, path, keep, beforeRemove,
		removeEmptyExtractDir,
	)
}

type emptyDirRemover func(*os.Root, string, string) error

func cleanupRetainedDirKeepingWithRemover(
	parent *os.Root,
	root **os.Root,
	info os.FileInfo,
	name, path string,
	keep map[string]struct{},
	beforeRemove func() error,
	remove emptyDirRemover,
) error {
	if parent == nil || root == nil || *root == nil || info == nil {
		return fmt.Errorf("%w: %q has no retained identity", errBootstrapCleanupIdentity, path)
	}
	openedRoot := *root
	entries, err := fs.ReadDir(openedRoot.FS(), ".")
	if err == nil {
		for _, entry := range entries {
			if _, ok := keep[entry.Name()]; ok {
				continue
			}
			if removeErr := openedRoot.RemoveAll(entry.Name()); removeErr != nil {
				err = removeErr
				break
			}
		}
	}
	closeErr := openedRoot.Close()
	*root = nil
	var beforeRemoveErr error
	if beforeRemove != nil {
		beforeRemoveErr = beforeRemove()
	}
	if err != nil {
		return errors.Join(err, closeErr, beforeRemoveErr)
	}
	return errors.Join(
		closeErr,
		beforeRemoveErr,
		removeRetainedDirShell(parent, info, name, path, remove),
	)
}

func removeRetainedDirShell(
	parent *os.Root,
	info os.FileInfo,
	name, path string,
	remove emptyDirRemover,
) error {
	named, err := parent.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf(
			"%w: inspect %q: %w", errBootstrapCleanupIdentity, path, err,
		)
	}
	if named.Mode()&os.ModeSymlink != 0 || !named.IsDir() ||
		!os.SameFile(info, named) {
		return fmt.Errorf("%w: %q", errBootstrapCleanupIdentity, path)
	}
	err = remove(parent, name, path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func cleanupBootstrapFile(
	root **os.Root,
	path string,
	expected os.FileInfo,
	guard **bootstrapFileGuard,
) error {
	if root == nil || *root == nil || expected == nil ||
		guard == nil || *guard == nil {
		return fmt.Errorf(
			"%w: %q has no retained identity",
			errBootstrapCleanupIdentity,
			path,
		)
	}
	openedRoot := *root
	removeErr := (*guard).remove(openedRoot, path, expected)
	guardErr := (*guard).Close()
	*guard = nil
	rootErr := openedRoot.Close()
	*root = nil
	return errors.Join(removeErr, guardErr, rootErr)
}

// cleanupBootstrapDir recursively removes entries through the handle retained
// from vetting. The saved pathname is used only after the recursive operation,
// to remove the empty directory when it still identifies the opened object.
func cleanupBootstrapDir(
	root **os.Root,
	dir string,
	keep map[string]struct{},
) error {
	return cleanupBootstrapDirWithRemover(root, dir, keep, removeEmptyExtractDir)
}

func cleanupBootstrapDirWithRemover(
	root **os.Root,
	dir string,
	keep map[string]struct{},
	remove emptyDirRemover,
) error {
	if root == nil || *root == nil {
		return fmt.Errorf("%w: %q has no retained handle", errBootstrapCleanupIdentity, dir)
	}
	openedRoot := *root
	opened, err := openedRoot.Stat(".")
	if err != nil {
		*root = nil
		return errors.Join(err, openedRoot.Close())
	}
	entries, err := fs.ReadDir(openedRoot.FS(), ".")
	if err != nil {
		*root = nil
		return errors.Join(err, openedRoot.Close())
	}
	for _, entry := range entries {
		if _, ok := keep[entry.Name()]; ok {
			continue
		}
		if err := openedRoot.RemoveAll(entry.Name()); err != nil {
			*root = nil
			return errors.Join(err, openedRoot.Close())
		}
	}
	if err := openedRoot.Close(); err != nil {
		*root = nil
		return err
	}
	*root = nil

	parent, err := os.OpenRoot(filepath.Dir(filepath.Clean(dir)))
	if err != nil {
		return err
	}
	defer parent.Close() //nolint:errcheck // removal error is returned below
	// Recursive deletion is complete. Remove only the empty shell, so a final
	// substitution cannot make cleanup descend into a different tree.
	return removeRetainedDirShell(
		parent,
		opened,
		filepath.Base(filepath.Clean(dir)),
		dir,
		remove,
	)
}
