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
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
)

// VerifySnapshotDigest computes the SHA-256 digest of a snapshot
// archive file and compares it against the expected digest from the
// Mithril aggregator.
func VerifySnapshotDigest(
	archivePath string,
	expectedDigest string,
) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf(
			"opening archive for digest verification: %w",
			err,
		)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf(
			"computing archive digest: %w",
			err,
		)
	}

	actualDigest := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(actualDigest, expectedDigest) {
		return fmt.Errorf(
			"snapshot digest mismatch: expected %s, got %s",
			expectedDigest,
			actualDigest,
		)
	}

	return nil
}

// VerifyChecksumFile verifies the CRC32 checksum of a ledger state
// file against its companion .checksum file.
func VerifyChecksumFile(lstatePath string) error {
	checksumPath := lstatePath + ".checksum"

	// Read expected checksum
	checksumData, err := os.ReadFile(checksumPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No checksum file is not an error - older snapshots
			// may not have one
			return nil
		}
		return fmt.Errorf(
			"reading checksum file: %w",
			err,
		)
	}

	expectedHex := strings.TrimSpace(string(checksumData))
	if expectedHex == "" {
		return nil // Empty checksum file, skip verification
	}
	decoded, err := hex.DecodeString(expectedHex)
	if err != nil {
		return fmt.Errorf(
			"invalid hex in checksum file %s: %w",
			checksumPath, err,
		)
	}
	if len(decoded) != 4 {
		return fmt.Errorf(
			"checksum file %s has %d hex chars, expected 8 (CRC32)",
			checksumPath, len(expectedHex),
		)
	}

	// Compute actual CRC32
	f, err := os.Open(lstatePath)
	if err != nil {
		return fmt.Errorf(
			"opening lstate for checksum: %w",
			err,
		)
	}
	defer f.Close()

	h := crc32.NewIEEE()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("computing CRC32: %w", err)
	}

	actualHex := fmt.Sprintf("%08x", h.Sum32())
	if !strings.EqualFold(actualHex, expectedHex) {
		return fmt.Errorf(
			"lstate CRC32 mismatch: expected %s, got %s",
			expectedHex,
			actualHex,
		)
	}

	return nil
}
