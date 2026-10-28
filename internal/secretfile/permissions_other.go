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

//go:build !windows

package secretfile

import (
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/keystore"
)

// checkPermissions rejects a value file that users other than its owner and
// group can access, then applies the platform's path ownership checks. A
// signing key must be owner-only, but a value file is commonly delivered by an
// orchestrator that grants its group read access, such as a Kubernetes secret
// volume with mode 0440 and an fsGroup; the group is the boundary that delivery
// chose, while other access reaches every local user.
func checkPermissions(path string, f *os.File) error {
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat value file %s: %w", f.Name(), err)
	}
	if perm := fi.Mode().Perm(); perm&0o007 != 0 {
		return fmt.Errorf(
			"value file %s has mode %04o; access by other users is not permitted: %w",
			f.Name(),
			perm,
			keystore.ErrInsecureFileMode,
		)
	}
	return checkTrustedPath(path, fi)
}
