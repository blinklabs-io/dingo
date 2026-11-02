//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package lifecycle

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func createRestoreDirectory(
	parent *os.Root,
	_ string,
	name string,
) (io.Closer, os.FileInfo, error) {
	if err := parent.Mkdir(name, 0o700); err != nil {
		return nil, nil, err
	}
	createdRoot, err := parent.OpenRoot(name)
	if err != nil {
		return nil, nil, errors.Join(
			err, removeCreatedRestoreDirOnly(parent, name),
		)
	}
	info, statErr := createdRoot.Stat(".")
	closeErr := createdRoot.Close()
	if statErr != nil || closeErr != nil {
		var cleanupErr error
		if info != nil {
			cleanupErr = removeCreatedRestoreDir(parent, name, info)
		} else {
			cleanupErr = removeCreatedRestoreDirOnly(parent, name)
		}
		return nil, nil, errors.Join(statErr, closeErr, cleanupErr)
	}
	return nil, info, nil
}

func removeCreatedRestoreDirOnly(parent *os.Root, name string) error {
	dir, err := parent.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close() //nolint:errcheck // removal error is returned
	conn, err := dir.SyscallConn()
	if err != nil {
		return err
	}
	var removeErr error
	if err := conn.Control(func(fd uintptr) {
		removeErr = unix.Unlinkat(int(fd), name, unix.AT_REMOVEDIR)
	}); err != nil {
		return err
	}
	if removeErr != nil {
		return fmt.Errorf("remove created restore directory %q: %w", name, removeErr)
	}
	return nil
}
