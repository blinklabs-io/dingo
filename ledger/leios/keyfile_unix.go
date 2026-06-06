//go:build !windows

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

package leios

import (
	"fmt"
	"os"
)

// checkVoteKeyFilePermissions verifies permissions on an already-opened
// vote signing key file using fstat to avoid TOCTOU races between
// permission check and read (same posture as keystore).
func checkVoteKeyFilePermissions(f *os.File) error {
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf(
			"failed to stat vote signing key file %q: %w",
			f.Name(),
			err,
		)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf(
			"vote signing key file %q has mode %04o, group/other access not permitted",
			f.Name(),
			fi.Mode().Perm(),
		)
	}
	return nil
}
