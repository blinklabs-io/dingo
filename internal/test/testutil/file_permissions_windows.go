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

package testutil

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// RestrictFileToCurrentUser applies a protected, owner-only DACL to a test
// fixture so inherited temp-directory permissions cannot make it insecure.
func RestrictFileToCurrentUser(t testing.TB, path string) {
	t.Helper()
	require.NoError(t, applyDACL(path, fmt.Sprintf(
		"D:P(A;;GA;;;%s)", currentUserSID(t),
	)))
}
