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
	"fmt"
	"os"
	"syscall"
)

func checkOpenFileOwner(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat key file %q: %w", f.Name(), err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf(
			"key file %q owner is unavailable: %w",
			f.Name(), ErrInsecureFileOwner,
		)
	}
	uid := uint64(stat.Uid)
	euid := uint64(os.Geteuid()) // #nosec G115 -- Effective UIDs are nonnegative.
	if uid != euid {
		return fmt.Errorf(
			"key file %q is owned by UID %d, current UID is %d: %w",
			f.Name(), uid, euid, ErrInsecureFileOwner,
		)
	}
	return nil
}
