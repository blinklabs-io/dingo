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

// Package secretfile reads a configuration value from a file. It backs the
// "_FILE" alternatives to settings that would otherwise put a secret in a
// command line or environment variable, where other local users and process
// listings can read it.
package secretfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// MaxBytes bounds a value file, so a path naming a device or a large file
// fails instead of being read into memory.
const MaxBytes = 64 << 10

// Read returns the contents of the file at path with trailing line endings
// removed. The file must be a regular file whose contents are not entirely
// whitespace. Errors never include the file's contents.
func Read(path string) (string, error) {
	if path == "" {
		return "", errors.New("value file path is empty")
	}
	// Stat before opening: opening a FIFO with no writer blocks, which would
	// hang startup on a mistyped path. Stat follows symlinks, so mounted
	// secrets that link to a regular file are accepted.
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("stat value file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("value file %s is not a regular file", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open value file: %w", err)
	}
	defer f.Close()
	buf, err := io.ReadAll(io.LimitReader(f, MaxBytes+1))
	if err != nil {
		return "", fmt.Errorf("read value file %s: %w", path, err)
	}
	if len(buf) > MaxBytes {
		return "", fmt.Errorf(
			"value file %s exceeds %d bytes",
			path,
			MaxBytes,
		)
	}
	value := strings.TrimRight(string(buf), "\r\n")
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("value file %s is empty", path)
	}
	return value, nil
}
