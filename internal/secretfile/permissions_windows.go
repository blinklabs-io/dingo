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

package secretfile

import (
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/keystore"
)

// checkPermissions applies the key file DACL check. It is stricter than the
// Unix rule: the ACL may grant access only to the file's owner,
// Administrators, SYSTEM, CREATOR OWNER and OWNER RIGHTS, so a grant to any
// other user or group is rejected, including the group access Unix allows.
// The shared check's own message still says "key file"; the wrapper names the
// value file so the error points at the setting that was read.
func checkPermissions(_ string, f *os.File) error {
	if err := keystore.CheckOpenFilePermissions(f); err != nil {
		return fmt.Errorf("value file %s: %w", f.Name(), err)
	}
	return nil
}
