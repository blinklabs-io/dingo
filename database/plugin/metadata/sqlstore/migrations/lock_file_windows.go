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

//go:build windows

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

type fileLocker struct {
	path string
}

// NewFileLocker returns the cross-process lock used by file-backed SQLite.
func NewFileLocker(path string) Locker {
	return &fileLocker{path: path}
}

func (l *fileLocker) Acquire(
	ctx context.Context,
	_ *sql.Conn,
) (func() error, error) {
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open metadata migration lock: %w", err)
	}
	var overlapped windows.Overlapped
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		err = windows.LockFileEx(
			windows.Handle(file.Fd()),
			windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
			0,
			1,
			0,
			&overlapped,
		)
		if err == nil {
			break
		}
		if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			_ = file.Close()
			return nil, fmt.Errorf("lock metadata migrations: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
	var released bool
	return func() error {
		if released {
			return nil
		}
		released = true
		unlockErr := windows.UnlockFileEx(
			windows.Handle(file.Fd()),
			0,
			1,
			0,
			&overlapped,
		)
		closeErr := file.Close()
		return errors.Join(unlockErr, closeErr)
	}, nil
}
