//go:build windows

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
	"fmt"
	"io"
	"os"

	"github.com/blinklabs-io/dingo/internal/securefs"
)

func lockRestorePath(path string, expected os.FileInfo) (io.Closer, error) {
	locked, err := securefs.LockWindowsPath(path)
	if err != nil {
		return nil, fmt.Errorf("lock restore directory %q: %w", path, err)
	}
	opened := locked.Info()
	if !opened.IsDir() || !os.SameFile(expected, opened) {
		_ = locked.Close()
		return nil, fmt.Errorf("%w: %q", errRestoreDirReplaced, path)
	}
	return locked, nil
}
