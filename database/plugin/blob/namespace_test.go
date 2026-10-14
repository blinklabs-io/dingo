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

package blob_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/stretchr/testify/require"
)

func TestNamespaceMarkerRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	legacy, err := blob.ReadNamespace(dir)
	require.NoError(t, err)
	require.Empty(t, legacy)

	namespace, err := blob.NewNamespace()
	require.NoError(t, err)
	require.NoError(t, blob.WriteNamespace(dir, namespace))
	got, err := blob.ReadNamespace(dir)
	require.NoError(t, err)
	require.Equal(t, namespace, got)
	require.Equal(
		t,
		"configured/.dingo-namespaces/"+namespace+"/",
		blob.NamespacePrefix("configured/", got),
	)
}

func TestEmptyDataDirUsesLegacyNamespace(t *testing.T) {
	t.Parallel()
	namespace, err := blob.ReadNamespace("")
	require.NoError(t, err)
	require.Empty(t, namespace)
}

func TestNamespaceMarkerRejectsMalformedInput(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"short\n", "ABCDEF0123456789ABCDEF0123456789\n", "../../outside___________________\n"} {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, blob.NamespaceFileName), []byte(value), 0o600,
		))
		_, err := blob.ReadNamespace(dir)
		require.Error(t, err)
	}
}

func TestWriteNamespaceReplacesSymlinkWithoutFollowingIt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	victim := filepath.Join(t.TempDir(), "victim")
	require.NoError(t, os.WriteFile(victim, []byte("unchanged"), 0o600))
	marker := filepath.Join(dir, blob.NamespaceFileName)
	if err := os.Symlink(victim, marker); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	namespace := "0123456789abcdef0123456789abcdef"
	require.NoError(t, blob.WriteNamespace(dir, namespace))
	got, err := blob.ReadNamespace(dir)
	require.NoError(t, err)
	require.Equal(t, namespace, got)
	victimData, err := os.ReadFile(victim)
	require.NoError(t, err)
	require.Equal(t, "unchanged", string(victimData))
	info, err := os.Lstat(marker)
	require.NoError(t, err)
	require.Zero(t, info.Mode()&os.ModeSymlink)
}
