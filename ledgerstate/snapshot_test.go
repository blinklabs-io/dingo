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

package ledgerstate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindLedgerStateFileLegacy(t *testing.T) {
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	// Create a legacy .lstate file
	lstatePath := filepath.Join(ledgerDir, "12345.lstate")
	err = os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, lstatePath, found)
}

func TestFindLedgerStateFileUTxOHD(t *testing.T) {
	dir := t.TempDir()
	slotDir := filepath.Join(dir, "ledger", "67890")
	err := os.MkdirAll(slotDir, 0o750)
	require.NoError(t, err)

	statePath := filepath.Join(slotDir, "state")
	err = os.WriteFile(statePath, []byte("data"), 0o640)
	require.NoError(t, err)

	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, statePath, found)
}

func TestFindLedgerStateFilePreferUTxOHD(t *testing.T) {
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	// Create both legacy and UTxO-HD files
	lstatePath := filepath.Join(ledgerDir, "12345.lstate")
	err = os.WriteFile(lstatePath, []byte("legacy"), 0o640)
	require.NoError(t, err)

	slotDir := filepath.Join(ledgerDir, "67890")
	err = os.MkdirAll(slotDir, 0o750)
	require.NoError(t, err)

	statePath := filepath.Join(slotDir, "state")
	err = os.WriteFile(statePath, []byte("utxohd"), 0o640)
	require.NoError(t, err)

	// Should prefer UTxO-HD format
	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, statePath, found)
}

func TestFindLedgerStateFileDBSubdir(t *testing.T) {
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "db", "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	lstatePath := filepath.Join(ledgerDir, "55555.lstate")
	err = os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)
	require.Equal(t, lstatePath, found)
}

func TestFindLedgerStateFileNotFound(t *testing.T) {
	dir := t.TempDir()
	_, err := FindLedgerStateFile(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ledger directory not found")
}

func TestFindUTxOTableFile(t *testing.T) {
	dir := t.TempDir()
	tablesDir := filepath.Join(dir, "ledger", "99999", "tables")
	err := os.MkdirAll(tablesDir, 0o750)
	require.NoError(t, err)

	tvarPath := filepath.Join(tablesDir, "tvar")
	err = os.WriteFile(tvarPath, []byte("data"), 0o640)
	require.NoError(t, err)

	found := FindUTxOTableFile(dir)
	require.Equal(t, tvarPath, found)
}

func TestFindUTxOTableFileNotFound(t *testing.T) {
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	err := os.MkdirAll(ledgerDir, 0o750)
	require.NoError(t, err)

	found := FindUTxOTableFile(dir)
	require.Empty(t, found)
}

func TestIsLedgerStateFile(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected bool
	}{
		{"numeric slot", "12345", true},
		{"checksum file", "12345.checksum", false},
		{"lock file", "12345.lock", false},
		{"tmp file", "12345.tmp", false},
		{"non-numeric", "state", false},
		{"empty", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(
				t,
				tt.expected,
				isLedgerStateFile(tt.filename),
			)
		})
	}
}

func TestEraName(t *testing.T) {
	require.Equal(t, "Byron", EraName(EraByron))
	require.Equal(t, "Shelley", EraName(EraShelley))
	require.Equal(t, "Allegra", EraName(EraAllegra))
	require.Equal(t, "Mary", EraName(EraMary))
	require.Equal(t, "Alonzo", EraName(EraAlonzo))
	require.Equal(t, "Babbage", EraName(EraBabbage))
	require.Equal(t, "Conway", EraName(EraConway))
	require.Contains(t, EraName(99), "Unknown")
}
