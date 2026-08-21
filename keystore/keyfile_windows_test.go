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
	file, err := os.Open(testFile)
	require.NoError(t, err)
	defer file.Close()
	err = checkOpenFilePermissions(file)
	assert.ErrorIs(t, err, ErrInsecureFileMode)
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
	file, err := os.Open(testFile)
	require.NoError(t, err)
	defer file.Close()
	assert.NoError(t, checkOpenFilePermissions(file))
}

func TestAdministratorAccountACEAcceptedWindows(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.skey")
	require.NoError(t, os.WriteFile(testFile, []byte("test"), 0o600))

	// Owned by the Administrators group with an ACE for the Administrator
	// account, which is the combination GitHub's Windows runners produce and
	// which a host administered that way also produces. Owner and trustee are
	// then different principals, so the owner comparison does not cover it.
	sd, err := windows.SecurityDescriptorFromString("O:BAD:P(A;;GA;;;LA)")
	require.NoError(t, err)
	dacl, _, err := sd.DACL()
	require.NoError(t, err)
	ownerSID, _, err := sd.Owner()
	require.NoError(t, err)
	require.NoError(t, windows.SetNamedSecurityInfo(
		testFile,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION,
		ownerSID, nil, dacl, nil,
	))

	file, err := os.Open(testFile)
	require.NoError(t, err)
	defer file.Close()
	assert.NoError(t, checkOpenFilePermissions(file))
}

func TestAdministratorAccountACERejectedWhenOwnerIsNotAdministratorsWindows(t *testing.T) {
	// The LA allowance is conditional on the owner being Built-in
	// Administrators. A file owned by an ordinary principal must not gain an
	// LA ace for free.
	assert.ErrorIs(t, checkOpenDACL(
		"test.skey",
		"S-1-5-21-999999999-888888888-777777777-1001",
		"(A;;GR;;;LA)",
	), ErrInsecureFileMode)
}

func TestNullDACLFileModeWindows(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.skey")
	require.NoError(t, os.WriteFile(testFile, []byte("test"), 0o600))
	require.NoError(t, windows.SetNamedSecurityInfo(
		testFile,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
		nil, nil, nil, nil,
	))

	file, err := os.Open(testFile)
	require.NoError(t, err)
	defer file.Close()
	assert.ErrorIs(t, checkOpenFilePermissions(file), ErrInsecureFileMode)
}

func TestAccessAllowedACEFormsWindows(t *testing.T) {
	for _, aceType := range []string{"A", "OA", "XA", "ZA"} {
		t.Run(aceType, func(t *testing.T) {
			dacl := fmt.Sprintf("(%s;;GR;;;WD)", aceType)
			err := checkOpenDACL("test.skey", "SY", dacl)
			assert.ErrorIs(t, err, ErrInsecureFileMode)
			assert.Contains(t, err.Error(), "WD")
			assert.ErrorIs(
				t,
				checkSDDL("test.skey", "O:SYD:"+dacl),
				ErrInsecureFileMode,
			)
		})
	}
}

func TestUnsupportedACETypeFailsClosedWindows(t *testing.T) {
	err := checkOpenDACL("test.skey", "SY", "(XX;;GR;;;SY)")
	assert.ErrorIs(t, err, ErrInsecureFileMode)
	assert.Contains(t, err.Error(), "unsupported DACL ACE type")

	err = checkSDDL("test.skey", "D:(XX;;GR;;;SY)")
	assert.ErrorIs(t, err, ErrInsecureFileMode)
	assert.Contains(t, err.Error(), "unsupported DACL ACE type")
}

// TestOwnerAliasMatchesCanonicalOwnerWindows pins both halves of the alias
// comparison: an alias ace must satisfy the owner it actually denotes on this
// machine, and must not satisfy a different principal that merely shares its
// RID.
//
// The owner is resolved here rather than written literally. An earlier version
// asserted against a synthetic "...-500" SID, which passed only because the
// comparison matched on the RID suffix — the hole this pins shut, since RID 500
// is the built-in Administrator of every domain, so a domain Administrator
// owner satisfied a local-administrator ace.
func TestOwnerAliasMatchesCanonicalOwnerWindows(t *testing.T) {
	sd, err := windows.SecurityDescriptorFromString("O:LA")
	require.NoError(t, err)
	localAdmin, _, err := sd.Owner()
	require.NoError(t, err)

	assert.NoError(t, checkOpenDACL(
		"test.skey", localAdmin.String(), "(A;;GR;;;LA)",
	))

	// Same RID, different domain: not the same principal, so the ace must not
	// be accepted as the owner's.
	assert.ErrorIs(t, checkOpenDACL(
		"test.skey",
		"S-1-5-21-111111111-222222222-333333333-500",
		"(A;;GR;;;LA)",
	), ErrInsecureFileMode)
}
