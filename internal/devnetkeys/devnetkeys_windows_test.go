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

package devnetkeys

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/keystore"
	"github.com/stretchr/testify/require"
)

func TestInstallLocalTestKeysRestrictsAndReplacesFiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.Mkdir(dir, 0o700))
	for _, name := range []string{"vrf.skey", "kes.skey", "opcert.cert"} {
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, name), []byte("old"), 0o600,
		))
	}

	require.NoError(t, InstallLocalTestKeys(dir))
	dirFile, err := os.Open(dir)
	require.NoError(t, err)
	require.NoError(t, keystore.CheckOpenFilePermissions(dirFile))
	require.NoError(t, dirFile.Close())

	for _, name := range []string{"vrf.skey", "kes.skey", "opcert.cert"} {
		want, err := localKeys.ReadFile("keys/" + name)
		require.NoError(t, err)
		path := filepath.Join(dir, name)
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, got)
		f, err := os.Open(path)
		require.NoError(t, err)
		require.NoError(t, keystore.CheckOpenFilePermissions(f))
		require.NoError(t, f.Close())
	}
}
