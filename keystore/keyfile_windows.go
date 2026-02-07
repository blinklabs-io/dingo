//go:build windows

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
	"strings"

	"golang.org/x/sys/windows"
)

// insecureSIDs maps SDDL SID abbreviations and full SID strings to
// human-readable names for groups that must not have access to key
// files.
var insecureSIDs = map[string]string{
	"WD":           "Everyone",
	"S-1-1-0":      "Everyone",
	"BU":           "BUILTIN\\Users",
	"S-1-5-32-545": "BUILTIN\\Users",
	"AU":           "Authenticated Users",
	"S-1-5-11":     "Authenticated Users",
}

// checkFilePermissions verifies that a key file has appropriate
// access controls on Windows. It converts the file's DACL to an
// SDDL string and rejects files that grant access to Everyone,
// the BUILTIN\Users group, or Authenticated Users.
//
// The implementation intentionally avoids the unsafe package to
// prevent heap corruption caused by Go 1.24+ GC interacting with
// uintptr-based SID handles (see https://go.dev/issue/73199).
func checkFilePermissions(path string) error {
	sd, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to get security info for %q: %w",
			path,
			err,
		)
	}
	// sd is Windows-allocated (LocalAlloc). Freeing it requires
	// unsafe.Pointer, which we avoid due to Go 1.24+ heap
	// corruption (go.dev/issue/73199). The ~200 B leak per call
	// is acceptable: checkFilePermissions runs only at startup
	// for a handful of key files.

	sddl := sd.String()
	if sddl == "" {
		return fmt.Errorf(
			"failed to read security descriptor for %q",
			path,
		)
	}

	return checkSDDL(path, sddl)
}

// checkOpenFilePermissions verifies permissions on an already-opened file.
// On Windows, NTFS prevents replacing a file that is held open, so using
// the file path from the open handle is safe against TOCTOU races.
func checkOpenFilePermissions(f *os.File) error {
	return checkFilePermissions(f.Name())
}

// checkSDDL parses an SDDL string and returns an error if the DACL
// contains any allow ACEs granting access to well-known insecure
// groups.
func checkSDDL(path, sddl string) error {
	// Extract the DACL portion ("D:" up to the next section).
	daclIdx := strings.Index(sddl, "D:")
	if daclIdx < 0 {
		// No DACL means unrestricted access.
		return fmt.Errorf(
			"key file %q has no DACL (unrestricted access): %w",
			path,
			ErrInsecureFileMode,
		)
	}
	daclStr := sddl[daclIdx+2:]
	// Trim at the SACL section if present.
	if idx := strings.Index(daclStr, "S:"); idx >= 0 {
		daclStr = daclStr[:idx]
	}

	// Walk each ACE (parenthesised entries).
	for {
		start := strings.IndexByte(daclStr, '(')
		if start < 0 {
			break
		}
		end := strings.IndexByte(daclStr[start:], ')')
		if end < 0 {
			break
		}
		ace := daclStr[start+1 : start+end]
		daclStr = daclStr[start+end+1:]

		// ACE: type;flags;rights;object;inherit;trustee
		fields := strings.Split(ace, ";")
		if len(fields) < 6 {
			continue
		}

		// Only inspect ACCESS_ALLOWED ACEs (type "A").
		if fields[0] != "A" {
			continue
		}

		trustee := fields[5]
		if name, ok := insecureSIDs[trustee]; ok {
			return fmt.Errorf(
				"key file %q grants access to %s: %w",
				path,
				name,
				ErrInsecureFileMode,
			)
		}
	}

	return nil
}
