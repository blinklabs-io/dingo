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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
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

func TestFindLedgerStateFileUTxOHDHighestSlot(t *testing.T) {
	dir := t.TempDir()

	// Create two UTxO-HD slot subdirectories with state files
	for _, slot := range []string{"100", "200"} {
		slotDir := filepath.Join(dir, "ledger", slot)
		err := os.MkdirAll(slotDir, 0o750)
		require.NoError(t, err)

		statePath := filepath.Join(slotDir, "state")
		err = os.WriteFile(statePath, []byte("data"), 0o640)
		require.NoError(t, err)
	}

	// FindLedgerStateFile must return the higher-numbered slot
	found, err := FindLedgerStateFile(dir)
	require.NoError(t, err)

	expectedPath := filepath.Join(dir, "ledger", "200", "state")
	require.Equal(t, expectedPath, found)
}

func TestFindLedgerStateFileNotFound(t *testing.T) {
	dir := t.TempDir()
	_, err := FindLedgerStateFile(dir)
	require.ErrorIs(t, err, ErrLedgerDirNotFound)
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

func TestVerifySnapshotDigest(t *testing.T) {
	content := []byte("test snapshot content for hashing")
	h := sha256.Sum256(content)
	expectedDigest := hex.EncodeToString(h[:])

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, content, 0o640)
	require.NoError(t, err)

	err = VerifySnapshotDigest(archivePath, expectedDigest)
	require.NoError(t, err)
}

func TestVerifySnapshotDigestMismatch(t *testing.T) {
	content := []byte("test snapshot content")

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.tar.zst")
	err := os.WriteFile(archivePath, content, 0o640)
	require.NoError(t, err)

	err = VerifySnapshotDigest(
		archivePath,
		"0000000000000000000000000000000"+
			"000000000000000000000000000000000",
	)
	require.ErrorContains(t, err, "mismatch")
}

func TestVerifyChecksumFileNoChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// No .checksum file - should succeed (not an error)
	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}

func TestVerifyChecksumFileEmptyChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(checksumPath, []byte("  \n"), 0o640)
	require.NoError(t, err)

	// Empty checksum - should succeed (skip verification)
	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}

func TestVerifyChecksumFileMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("00000000"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.ErrorContains(t, err, "mismatch")
}

func TestVerifyChecksumFileWrongLength(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// Valid hex but wrong length (5 bytes instead of 4)
	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("aabbccdd00"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 8")
}

func TestVerifyChecksumFileInvalidHex(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	err := os.WriteFile(lstatePath, []byte("data"), 0o640)
	require.NoError(t, err)

	// Valid length but invalid hex characters
	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte("GGGGGGGG"),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.ErrorContains(t, err, "invalid hex")
}

func TestVerifyChecksumFileValid(t *testing.T) {
	tmpDir := t.TempDir()
	lstatePath := filepath.Join(tmpDir, "12345.lstate")
	content := []byte("test data for crc32")
	err := os.WriteFile(lstatePath, content, 0o640)
	require.NoError(t, err)

	// Compute the actual CRC32 for the content
	h := crc32.NewIEEE()
	_, err = h.Write(content)
	require.NoError(t, err)
	checksum := fmt.Sprintf("%08x", h.Sum32())

	checksumPath := lstatePath + ".checksum"
	err = os.WriteFile(
		checksumPath,
		[]byte(checksum),
		0o640,
	)
	require.NoError(t, err)

	err = VerifyChecksumFile(lstatePath)
	require.NoError(t, err)
}
