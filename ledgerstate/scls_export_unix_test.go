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

//go:build unix

package ledgerstate

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExportSCLSFileModeFollowsTheUmask requires the export, which is public
// ledger state, to take the umask rather than an owner-only mode.
//
// Not t.Parallel: it sets the process-wide umask.
func TestExportSCLSFileModeFollowsTheUmask(t *testing.T) {
	old := syscall.Umask(0o022)
	t.Cleanup(func() { syscall.Umask(old) })
	f := newSCLSFixture(t, EraConway, sclsRefs()...)
	out := filepath.Join(t.TempDir(), "state.scls")
	_, err := ExportSCLS(t.Context(), SCLSExportConfig{
		Source: f.source(t), Output: out, Tool: "dingo test",
	})
	require.NoError(t, err)
	info, err := os.Stat(out)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
}
