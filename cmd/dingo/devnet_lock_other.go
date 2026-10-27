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

//go:build !unix && !windows

package main

import (
	"errors"
	"fmt"
	"os"
)

func acquireDevnetStateLock(
	root *os.Root,
	runDir string,
) (func() error, error) {
	file, err := root.OpenFile(
		devnetStateLock,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("%w: %q", errDevnetStateInUse, runDir)
	}
	if err != nil {
		return nil, fmt.Errorf("creating devnet state lock in %q: %w", runDir, err)
	}
	if err := file.Close(); err != nil {
		_ = root.Remove(devnetStateLock)
		return nil, fmt.Errorf("closing devnet state lock in %q: %w", runDir, err)
	}
	return func() error { return root.Remove(devnetStateLock) }, nil
}
