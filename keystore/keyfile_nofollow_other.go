//go:build !unix

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

package keystore

import (
	"fmt"
	"os"
)

func openFileNoFollow(path string) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"key file %q is not a regular file (mode %s): %w",
			path, before.Mode(), ErrNotRegularFile,
		)
	}
	f, err := openFileForValidation(path)
	if err != nil {
		return nil, err
	}
	opened, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to stat key file %q: %w", path, err)
	}
	named, err := os.Lstat(path)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if named.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(before, opened) ||
		!os.SameFile(named, opened) {
		_ = f.Close()
		return nil, fmt.Errorf(
			"key file %q changed while it was opened: %w",
			path, ErrNotRegularFile,
		)
	}
	return f, nil
}
