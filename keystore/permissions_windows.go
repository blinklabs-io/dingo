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
	"log/slog"
	"os"
	"strings"
)

// Environment variable to bypass Windows ACL checks.
// Set DINGO_ALLOW_INSECURE_KEY_PERMS=true to skip permission verification.
// WARNING: Only use this in development/testing environments where you have
// manually verified the key file ACLs are properly restricted.
const envAllowInsecureKeyPerms = "DINGO_ALLOW_INSECURE_KEY_PERMS"

// checkKeyFilePermissions on Windows verifies file permissions.
// Windows uses ACLs which require platform-specific APIs to check properly.
// By default, this function fails-closed (returns an error) since ACL
// verification is not implemented. Set DINGO_ALLOW_INSECURE_KEY_PERMS=true
// to bypass this check after manually verifying file ACLs.
func checkKeyFilePermissions(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Check for explicit override
	if allowInsecure := os.Getenv(envAllowInsecureKeyPerms); strings.EqualFold(allowInsecure, "true") {
		slog.Warn(
			"Windows ACL verification bypassed via environment variable; "+
				"ensure key file has restrictive permissions manually",
			"path", path,
			"env_var", envAllowInsecureKeyPerms,
		)
		return nil
	}

	// Fail-closed: Windows ACL checking requires platform-specific APIs
	// (e.g., GetFileSecurity, GetSecurityDescriptorDacl from golang.org/x/sys/windows).
	// Until proper ACL checks are implemented, reject by default for security.
	return fmt.Errorf(
		"%w: Windows ACL verification not implemented; "+
			"set %s=true to bypass after manually verifying file permissions",
		ErrInsecureFileMode,
		envAllowInsecureKeyPerms,
	)
}
