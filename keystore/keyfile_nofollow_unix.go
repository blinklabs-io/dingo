//go:build unix

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
	"errors"
	"fmt"
	"os"
	"syscall"
)

func openFileNoFollow(path string) (*os.File, error) {
	f, err := os.OpenFile( // #nosec G304 -- operator-configured key path
		path,
		os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW,
		0,
	)
	if errors.Is(err, syscall.ELOOP) {
		return nil, fmt.Errorf(
			"key file %q is a symlink: %w", path, ErrNotRegularFile,
		)
	}
	return f, err
}
