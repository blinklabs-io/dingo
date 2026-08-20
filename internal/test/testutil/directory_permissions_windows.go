//go:build windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package testutil

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

// MakeDirectoryUnwritable denies the current user access to a test directory
// and restores an owner-only DACL before the test's temporary-directory
// cleanup runs. Windows does not use Unix mode bits for access checks.
func MakeDirectoryUnwritable(t testing.TB, path string) {
	t.Helper()
	userSID := currentUserSID(t)
	setDirectoryDACL(t, path, fmt.Sprintf(
		"D:P(D;;FA;;;%s)(A;;FA;;;SY)", userSID,
	))
	t.Cleanup(func() {
		setDirectoryDACL(t, path, fmt.Sprintf(
			"D:P(A;;FA;;;%s)", userSID,
		))
	})
}

func currentUserSID(t testing.TB) string {
	t.Helper()
	var token windows.Token
	require.NoError(t, windows.OpenProcessToken(
		windows.CurrentProcess(), windows.TOKEN_QUERY, &token,
	))
	defer token.Close()
	tokenUser, err := token.GetTokenUser()
	require.NoError(t, err)
	return tokenUser.User.Sid.String()
}

func setDirectoryDACL(t testing.TB, path, sddl string) {
	t.Helper()
	sd, err := windows.SecurityDescriptorFromString(sddl)
	require.NoError(t, err)
	dacl, _, err := sd.DACL()
	require.NoError(t, err)
	require.NoError(t, windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|
			windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil,
	))
}
