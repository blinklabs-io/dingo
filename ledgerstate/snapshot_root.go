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

// Snapshot discovery and reading resolved through an open directory handle
// rather than by pathname.
//
// The pathname functions alongside these are fine where the tree is the
// operator's own and nobody else can write to it. They are not fine where a
// caller vetted a tree and then has to read it: the vetting is about a
// directory, the pathname is about whatever occupies a name, and between the
// two a concurrent writer can make those different things. Mithril bootstrap
// extracts into a download area where that is the whole threat, so it discovers
// and reads through one handle held across both.
//
// Paths here are relative to the handle and always slash-separated, the way
// io/fs names are; they are converted at the point they reach an os.Root
// method.

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// rootStat stats a slash-separated path relative to root.
func rootStat(root *os.Root, rel string) (fs.FileInfo, error) {
	return root.Stat(filepath.FromSlash(rel))
}

// RootOpen opens a slash-separated path relative to root. The caller closes the
// returned file.
func RootOpen(root *os.Root, rel string) (*os.File, error) {
	return root.Open(filepath.FromSlash(rel))
}

// findLedgerDirIn is findLedgerDir resolved through root, returning a
// slash-separated path relative to it.
func findLedgerDirIn(root *os.Root) (string, error) {
	for _, c := range []string{"ledger", "db/ledger"} {
		info, err := rootStat(root, c)
		if err == nil && info.IsDir() {
			return c, nil
		}
	}
	return "", fmt.Errorf(
		"%w under %s (checked ledger/ and db/ledger/)",
		ErrLedgerDirNotFound,
		root.Name(),
	)
}

// FindLedgerStateAtOrBefore is FindLedgerStateFileAtOrBefore resolved through
// an open handle on the extracted snapshot directory. It returns the state
// file's path relative to that handle, which is what the caller must read it
// through — an absolute name reassembled from it would be resolved afresh, and
// so would not be bound to the tree this searched.
func FindLedgerStateAtOrBefore(
	root *os.Root,
	maxSlot uint64,
) (string, error) {
	ledgerDir, err := findLedgerDirIn(root)
	if err != nil {
		return "", err
	}

	entries, err := fs.ReadDir(root.FS(), ledgerDir)
	if err != nil {
		return "", fmt.Errorf("reading ledger directory: %w", err)
	}

	var utxoHDDirs []string
	var legacyFiles []string

	for _, e := range entries {
		name := e.Name()
		slot, parseErr := strconv.ParseUint(
			stripLedgerSuffix(name),
			10,
			64,
		)
		if parseErr != nil || slot > maxSlot {
			continue
		}
		if e.IsDir() {
			// UTxO-HD format: directory named by slot number
			statePath := path.Join(ledgerDir, name, "state")
			if _, err := rootStat(root, statePath); err == nil {
				utxoHDDirs = append(utxoHDDirs, name)
			}
			continue
		}
		// Legacy format: .lstate files or numeric slot filenames
		if strings.HasSuffix(name, ".lstate") ||
			strings.HasSuffix(name, "_snapshot") ||
			isLedgerStateFile(name) {
			legacyFiles = append(legacyFiles, name)
		}
	}

	// Prefer UTxO-HD format (newer)
	utxoHDDirs = sortNumericDesc(utxoHDDirs)
	if len(utxoHDDirs) > 0 {
		return path.Join(ledgerDir, utxoHDDirs[0], "state"), nil
	}

	legacyFiles = sortNumericSuffixDesc(legacyFiles)
	if len(legacyFiles) > 0 {
		return path.Join(ledgerDir, legacyFiles[0]), nil
	}

	return "", fmt.Errorf(
		"no ledger state files at or before slot %d found under %s",
		maxSlot,
		root.Name(),
	)
}

// FindUTxOTableForState is FindUTxOTableFileForState resolved through root,
// taking and returning paths relative to it. Legacy ledger-state files embed
// their UTxO table and return an empty path.
func FindUTxOTableForState(root *os.Root, stateRel string) string {
	if path.Base(stateRel) != "state" {
		return ""
	}
	slotDir := path.Dir(stateRel)
	for _, c := range []string{
		path.Join(slotDir, "tables"),
		path.Join(slotDir, "tables", "tvar"),
	} {
		info, err := rootStat(root, c)
		if err == nil && !info.IsDir() {
			return c
		}
	}
	return ""
}

// ParseSnapshotFile parses a ledger state snapshot from an already-open file,
// so the bytes come from the file the caller opened rather than from a name
// re-resolved here. The caller closes f.
func ParseSnapshotFile(f *os.File) (*RawLedgerState, error) {
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("reading snapshot file: %w", err)
	}
	return parseSnapshotData(data)
}
