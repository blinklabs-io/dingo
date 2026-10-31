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

package lifecycle

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/internal/securefs"
)

var (
	errRestoreDirReplaced      = errors.New("restore directory was replaced")
	errRestoreLeaseUnsupported = errors.New(
		"restore directory lease unsupported",
	)
)

// restoreDir keeps the directory restore created or accepted tied to the
// object opened through parent. Its pathname remains useful for provider APIs,
// but it is never used for recursive cleanup.
type restoreDir struct {
	parent          *os.Root
	root            *os.Root
	lock            io.Closer
	info            os.FileInfo
	name            string
	path            string
	renameNoReplace func(*os.Root, string, string) error
	removeName      func(string) error
}

type restoreDirectory interface {
	rename(string, string) error
	cleanup() error
	disposeEmpty() error
}

type ownedRestoreDir struct {
	dir        *restoreDir
	parent     *os.Root
	parentLock io.Closer
	// lease marks the directory as held by a running operation, so
	// stale-work cleanup in another process leaves it alone and reclaims it
	// as soon as this process exits without cleaning up.
	lease io.Closer
}

func createOwnedRestoreDir(parentPath, prefix string) (*ownedRestoreDir, error) {
	if parentPath == "" {
		parentPath = os.TempDir()
	}
	parent, err := os.OpenRoot(parentPath)
	if err != nil {
		return nil, err
	}
	name := prefix + rand.Text()
	path := filepath.Join(parentPath, name)
	parentLock, created, err := createRestoreDirectory(parent, parentPath, name)
	if err != nil {
		_ = parent.Close()
		return nil, err
	}
	owned, err := retainCreatedRestoreDir(
		parent, parentLock, created, name, path,
	)
	if err != nil {
		return nil, err
	}
	// Nothing is written into the directory before the lease is held, so
	// stale-work cleanup that finds it empty and unheld leaves it alone.
	// Waiting covers that cleanup's brief probe of the new directory.
	lease, _, err := leaseRestoreDir(owned.dir.root, true)
	if err != nil && !errors.Is(err, errRestoreLeaseUnsupported) {
		return nil, errors.Join(
			fmt.Errorf("lease restore directory %q: %w", path, err),
			owned.cleanup(),
		)
	}
	owned.lease = lease
	return owned, nil
}

func retainCreatedRestoreDir(
	parent *os.Root,
	parentLock io.Closer,
	created os.FileInfo,
	name, path string,
) (*ownedRestoreDir, error) {
	dir, err := openRestoreDir(parent, name, path)
	if err != nil {
		_ = removeCreatedRestoreDir(parent, name, created)
		if parentLock != nil {
			_ = parentLock.Close()
		}
		_ = parent.Close()
		return nil, err
	}
	if !os.SameFile(created, dir.info) {
		_ = dir.closeRoot()
		cleanupErr := removeCreatedRestoreDir(parent, name, created)
		if parentLock != nil {
			_ = parentLock.Close()
		}
		_ = parent.Close()
		return nil, errors.Join(
			fmt.Errorf("%w: created restore directory changed", errRestoreDirReplaced),
			cleanupErr,
		)
	}
	return &ownedRestoreDir{
		dir: dir, parent: parent, parentLock: parentLock,
	}, nil
}

func createOwnedRestoreDirAt(path string) (*ownedRestoreDir, error) {
	path = filepath.Clean(path)
	parentPath := filepath.Dir(path)
	parent, err := os.OpenRoot(parentPath)
	if err != nil {
		return nil, err
	}
	name := filepath.Base(path)
	parentLock, created, err := createRestoreDirectory(parent, parentPath, name)
	if err != nil {
		_ = parent.Close()
		return nil, err
	}
	return retainCreatedRestoreDir(
		parent, parentLock, created, name, path,
	)
}

func removeCreatedRestoreDir(
	parent *os.Root,
	name string,
	created os.FileInfo,
) error {
	named, err := parent.Lstat(name)
	if err != nil {
		return err
	}
	if created == nil || named.Mode()&os.ModeSymlink != 0 || !named.IsDir() ||
		!os.SameFile(created, named) {
		return fmt.Errorf("%w: created restore directory changed", errRestoreDirReplaced)
	}
	return parent.Remove(name)
}

func openOwnedRestoreDir(path string) (*ownedRestoreDir, error) {
	path = filepath.Clean(path)
	parentPath := filepath.Dir(path)
	parent, err := os.OpenRoot(parentPath)
	if err != nil {
		return nil, err
	}
	dir, err := openRestoreDir(parent, filepath.Base(path), path)
	if err != nil {
		_ = parent.Close()
		return nil, err
	}
	return &ownedRestoreDir{dir: dir, parent: parent}, nil
}

func (d *ownedRestoreDir) cleanup() error {
	if d == nil {
		return nil
	}
	// The lease is released only after removal, so no other process can
	// treat a partly removed directory as abandoned and race this one.
	return errors.Join(
		d.dir.cleanup(), d.releaseLease(), closeIfSet(d.parentLock),
		d.parent.Close(),
	)
}

func (d *ownedRestoreDir) close() error {
	if d == nil {
		return nil
	}
	return errors.Join(
		d.dir.closeRoot(), d.releaseLease(), closeIfSet(d.parentLock),
		d.parent.Close(),
	)
}

func (d *ownedRestoreDir) releaseLease() error {
	lease := d.lease
	d.lease = nil
	return closeIfSet(lease)
}

func closeIfSet(closer io.Closer) error {
	if closer == nil {
		return nil
	}
	return closer.Close()
}

func openRestoreDir(
	parent *os.Root,
	name, path string,
) (*restoreDir, error) {
	named, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if named.Mode()&os.ModeSymlink != 0 || !named.IsDir() {
		return nil, fmt.Errorf("%w: %q is not a directory", errRestoreDirReplaced, path)
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	opened, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if !os.SameFile(named, opened) {
		_ = root.Close()
		return nil, fmt.Errorf("%w: %q changed while it was opened", errRestoreDirReplaced, path)
	}
	lock, err := lockRestorePath(path, opened)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	return &restoreDir{
		parent:          parent,
		root:            root,
		lock:            lock,
		info:            opened,
		name:            name,
		path:            path,
		renameNoReplace: securefs.RenameNoReplace,
	}, nil
}

func (d *restoreDir) checkNamed() error {
	if d == nil {
		return nil
	}
	named, err := d.parent.Lstat(d.name)
	if err != nil {
		return fmt.Errorf("%w: inspect %q: %w", errRestoreDirReplaced, d.path, err)
	}
	if named.Mode()&os.ModeSymlink != 0 || !named.IsDir() ||
		!os.SameFile(d.info, named) {
		return fmt.Errorf("%w: %q", errRestoreDirReplaced, d.path)
	}
	return nil
}

func (d *restoreDir) closeRoot() error {
	if d == nil {
		return nil
	}
	var errs []error
	if d.root != nil {
		errs = append(errs, d.root.Close())
		d.root = nil
	}
	if d.lock != nil {
		errs = append(errs, d.lock.Close())
		d.lock = nil
	}
	return errors.Join(errs...)
}

func (d *restoreDir) reopenNamed() error {
	if d.root != nil {
		return nil
	}
	if err := d.checkNamed(); err != nil {
		return err
	}
	root, err := d.parent.OpenRoot(d.name)
	if err != nil {
		return err
	}
	opened, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return err
	}
	if !os.SameFile(d.info, opened) {
		_ = root.Close()
		return fmt.Errorf("%w: %q changed while it was reopened", errRestoreDirReplaced, d.path)
	}
	lock, err := lockRestorePath(d.path, opened)
	if err != nil {
		_ = root.Close()
		return err
	}
	d.root = root
	d.lock = lock
	return nil
}

func (d *restoreDir) removeContents() error {
	if err := d.reopenNamed(); err != nil {
		return err
	}
	entries, err := fs.ReadDir(d.root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := d.root.RemoveAll(entry.Name()); err != nil {
			return err
		}
	}
	return nil
}

// cleanup recursively clears the opened directory, then removes its empty
// pathname only while that pathname still identifies the same object. A
// replacement at the saved name is never traversed.
func (d *restoreDir) cleanup() error {
	if d == nil {
		return nil
	}
	if err := d.removeContents(); err != nil {
		_ = d.closeRoot()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := d.closeRoot(); err != nil {
		return err
	}
	if err := d.checkNamed(); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	// The recursive operation is complete before returning to the name. Remove
	// can only remove the now-empty directory (or a single replacement entry),
	// never descend through a substituted tree.
	remove := d.parent.Remove
	if d.removeName != nil {
		remove = d.removeName
	}
	err := remove(d.name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (d *restoreDir) disposeEmpty() error {
	if d == nil {
		return nil
	}
	if err := d.reopenNamed(); err != nil {
		return err
	}
	entries, err := fs.ReadDir(d.root.FS(), ".")
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("restore directory %q is no longer empty", d.path)
	}
	if err := d.closeRoot(); err != nil {
		return err
	}
	if err := d.checkNamed(); err != nil {
		return err
	}
	return d.parent.Remove(d.name)
}

func (d *restoreDir) rename(newName, newPath string) error {
	if d == nil {
		return nil
	}
	if err := d.closeRoot(); err != nil {
		return err
	}
	oldName := d.name
	oldPath := d.path
	quarantine := ".dingo-restore-move-" + rand.Text()
	if err := d.renameNoReplace(d.parent, oldName, quarantine); err != nil {
		return err
	}
	d.name = quarantine
	d.path = filepath.Join(filepath.Dir(oldPath), quarantine)
	if err := d.checkNamed(); err != nil {
		rollbackErr := d.renameNoReplace(d.parent, quarantine, oldName)
		if rollbackErr == nil {
			d.name = oldName
			d.path = oldPath
		}
		return errors.Join(err, rollbackRenameError(rollbackErr))
	}
	if err := d.renameNoReplace(d.parent, quarantine, newName); err != nil {
		rollbackErr := d.renameNoReplace(d.parent, quarantine, oldName)
		if rollbackErr == nil {
			d.name = oldName
			d.path = oldPath
		}
		return errors.Join(err, rollbackRenameError(rollbackErr))
	}
	named, err := d.parent.Lstat(newName)
	if err != nil || named.Mode()&os.ModeSymlink != 0 || !named.IsDir() ||
		!os.SameFile(d.info, named) {
		// The destination is unverified, so do not move or remove it. Keep the
		// last confirmed name for diagnostic and recovery cleanup.
		if err == nil {
			err = fmt.Errorf("%w: %q", errRestoreDirReplaced, newPath)
		} else {
			err = fmt.Errorf(
				"%w: inspect %q: %w", errRestoreDirReplaced, newPath, err,
			)
		}
		return err
	}
	d.name = newName
	d.path = newPath
	return nil
}

func rollbackRenameError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("restore directory rename rollback: %w", err)
}

func cleanupStaleRestoreDir(dir *restoreDir, path string) error {
	err := dir.cleanup()
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("remove stale restore work directory %q: %w", path, err)
	}
	return nil
}

func activateRestoreDirs(
	staging restoreDirectory,
	originalTarget restoreDirectory,
	workParent, targetName, targetPath string,
) (activated bool, err error) {
	rollbackOriginal := false
	if originalTarget != nil {
		heldTargetName := ".dingo-restore-target-" + rand.Text()
		heldTargetPath := filepath.Join(workParent, heldTargetName)
		if err := originalTarget.rename(heldTargetName, heldTargetPath); err != nil {
			return false, fmt.Errorf("hold original restore target: %w", err)
		}
		rollbackOriginal = true
		defer func() {
			if err == nil || !rollbackOriginal {
				return
			}
			if rollbackErr := originalTarget.rename(
				targetName,
				targetPath,
			); rollbackErr != nil {
				err = errors.Join(
					err,
					fmt.Errorf(
						"restore original target directory: %w",
						rollbackErr,
					),
				)
			}
		}()
		if err := originalTarget.disposeEmpty(); err != nil {
			return false, fmt.Errorf(
				"remove original empty restore target: %w",
				err,
			)
		}
		rollbackOriginal = false
	}
	if err := staging.rename(targetName, targetPath); err != nil {
		return false, fmt.Errorf("activate restored data directory: %w", err)
	}
	return true, nil
}
