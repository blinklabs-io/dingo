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
)

// MakeDirectoryUnwritable denies the current user access to a test directory
// and restores an owner-only DACL before the test's temporary-directory
// cleanup runs. Windows does not use Unix mode bits for access checks.
func MakeDirectoryUnwritable(t testing.TB, path string) {
	t.Helper()
	userSID := currentUserSID(t)
	require.NoError(t, applyDACL(path, fmt.Sprintf(
		"D:P(D;;FA;;;%s)(A;;FA;;;SY)", userSID,
	)))
	t.Cleanup(func() {
		// Logged rather than required: a failure here must not abort the
		// cleanup loop, or the temporary directory's own removal never runs.
		if err := applyDACL(path, fmt.Sprintf(
			"D:P(A;;FA;;;%s)", userSID,
		)); err != nil {
			t.Logf("restoring DACL on %s: %v", path, err)
		}
	})
}
