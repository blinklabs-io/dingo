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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// currentUserSIDString returns the SID string (e.g.
// "S-1-5-21-…-1001") for the current process user.
func currentUserSIDString(t *testing.T) string {
	t.Helper()

	var token windows.Token
	err := windows.OpenProcessToken(
		windows.CurrentProcess(),
		windows.TOKEN_QUERY,
		&token,
	)
	require.NoError(t, err)
	defer token.Close()

	tokenUser, err := token.GetTokenUser()
	require.NoError(t, err)

	return tokenUser.User.Sid.String()
}

// setOwnerOnlyDACL sets a protected DACL on the file that grants
// access only to the current user. It uses SDDL to avoid unsafe
// pointer operations that cause heap corruption on Go 1.24+.
func setOwnerOnlyDACL(t *testing.T, path string) {
	t.Helper()

	userSID := currentUserSIDString(t)
	// D:P = protected DACL (no inheritance from parent).
	// (A;;GA;;;SID) = allow GENERIC_ALL to the given SID.
	sddl := fmt.Sprintf("D:P(A;;GA;;;%s)", userSID)

	sd, err := windows.SecurityDescriptorFromString(sddl)
	require.NoError(t, err)

	dacl, _, err := sd.DACL()
	require.NoError(t, err)

	err = windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|
			windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil,
	)
	require.NoError(t, err)
}

func TestInsecureFileModeWindows(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.skey")

	require.NoError(
		t,
		os.WriteFile(testFile, []byte("test"), 0o600),
	)

	// Build a DACL that grants Everyone read access via SDDL.
	sddl := "D:(A;;GR;;;WD)"
	sd, err := windows.SecurityDescriptorFromString(sddl)
	require.NoError(t, err)

	dacl, _, err := sd.DACL()
	require.NoError(t, err)

	err = windows.SetNamedSecurityInfo(
		testFile,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil,
	)
	require.NoError(t, err)

	// checkFilePermissions should detect the insecure ACL.
	err = checkFilePermissions(testFile)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInsecureFileMode)
	assert.Contains(t, err.Error(), "Everyone")
}

func TestInsecureFileModeWindowsBuiltinUsers(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.skey")

	require.NoError(
		t,
		os.WriteFile(testFile, []byte("test"), 0o600),
	)

	// Build a DACL that grants BUILTIN\Users read access via SDDL.
	sddl := "D:(A;;GR;;;BU)"
	sd, err := windows.SecurityDescriptorFromString(sddl)
	require.NoError(t, err)

	dacl, _, err := sd.DACL()
	require.NoError(t, err)

	err = windows.SetNamedSecurityInfo(
		testFile,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil,
	)
	require.NoError(t, err)

	err = checkFilePermissions(testFile)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInsecureFileMode)
	assert.Contains(t, err.Error(), "BUILTIN\\Users")
}

func TestInsecureFileModeWindowsAuthenticatedUsers(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.skey")

	require.NoError(
		t,
		os.WriteFile(testFile, []byte("test"), 0o600),
	)

	// Build a DACL that grants Authenticated Users read access via SDDL.
	sddl := "D:(A;;GR;;;AU)"
	sd, err := windows.SecurityDescriptorFromString(sddl)
	require.NoError(t, err)

	dacl, _, err := sd.DACL()
	require.NoError(t, err)

	err = windows.SetNamedSecurityInfo(
		testFile,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil,
	)
	require.NoError(t, err)

	err = checkFilePermissions(testFile)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInsecureFileMode)
	assert.Contains(t, err.Error(), "Authenticated Users")
}

func TestSecureFileModeWindows(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.skey")

	require.NoError(
		t,
		os.WriteFile(testFile, []byte("test"), 0o600),
	)

	// Explicitly set owner-only DACL. Default Windows ACLs
	// inherit from the parent directory and typically include
	// BUILTIN\Users, which checkFilePermissions rejects.
	setOwnerOnlyDACL(t, testFile)

	err := checkFilePermissions(testFile)
	assert.NoError(t, err)
}
