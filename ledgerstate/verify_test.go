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
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch")
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
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch")
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
