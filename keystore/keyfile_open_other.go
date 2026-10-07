//go:build !unix && !windows

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

func openFileForValidation(path string) (*os.File, error) {
	// Platforms without a nonblocking open preflight the path type, and
	// OpenRegularFile validates the returned handle again after opening.
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"key file %q is not a regular file (mode %s): %w",
			path, info.Mode(), ErrNotRegularFile,
		)
	}
	return os.Open(path) // #nosec G304 -- operator-configured key path
}
