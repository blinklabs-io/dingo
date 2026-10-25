//go:build !windows

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lifecycle

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateRestoreWorkParentRejectsMutableDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o770))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	err := validateRestoreWorkParent(dir)
	require.Error(t, err)
	require.ErrorContains(t, err, dir)
}
