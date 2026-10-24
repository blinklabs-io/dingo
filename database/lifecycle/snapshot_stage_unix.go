//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lifecycle

import (
	"context"
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// installStagedSnapshotFile renames the staged file into the opened snapshot
// directory by directory handle, so a snapshot path replaced after OpenRoot
// cannot redirect it, and the payload is not written a second time. A staging
// directory on another filesystem falls back to a copy through root.
func installStagedSnapshotFile(
	ctx context.Context,
	root *os.Root,
	stageDir string,
	name string,
) error {
	src, err := os.Open(stageDir)
	if err != nil {
		return err
	}
	defer src.Close() //nolint:errcheck // read-only directory handle
	dst, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dst.Close() //nolint:errcheck // read-only directory handle
	err = unix.Renameat(
		int(src.Fd()), //nolint:gosec // descriptor values fit in int
		name,
		int(dst.Fd()), //nolint:gosec // descriptor values fit in int
		name,
	)
	if errors.Is(err, unix.EXDEV) {
		return copyStagedSnapshotFile(ctx, root, stageDir, name)
	}
	return err
}
