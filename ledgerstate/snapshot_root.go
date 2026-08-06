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

// Snapshot discovery and reading for trees the caller does not trust.
//
// The pathname functions alongside these are fine where the tree is the
// operator's own and nobody else can write to it. They are not fine where a
// caller vetted a tree and then has to read it. Two things go wrong there, and
// both are closed here:
//
//   - Names get re-resolved. A search that returns "ledger/100/state" and a
//     read that opens that name later are two questions, and between them a
//     concurrent writer can make the answers differ. So discovery *opens* what
//     it selects and hands back the open files; nothing downstream resolves a
//     name again.
//
//   - Symlinks inside the tree are followed. os.Root confines traversal to the
//     root, but it still follows a link that stays inside it, and a link is not
//     something extraction ever writes — it is evidence somebody else did. So
//     every component is opened through its parent's handle and confirmed to be
//     the entry the name denotes, which rejects a symlink and a substitution
//     with the same check.
//
// Paths here are relative to a handle and always slash-separated, the way io/fs
// names are; they are converted where they reach an os.Root method.

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"
)

// ErrUnsafeSnapshotPath reports an entry in a snapshot tree that is a symlink,
// or that was replaced between being opened and being checked.
var ErrUnsafeSnapshotPath = errors.New("unsafe snapshot path")

// openVerifiedChild opens a directory directly beneath parent and confirms the
// handle refers to the entry that name refers to.
//
// Opening cannot be made to reject a symlink outright — Root follows one whose
// target stays inside the root — so the handle is compared against the entry
// afterwards instead. A writer who substitutes the name between the open and
// the comparison leaves the two disagreeing, which is what this rejects; a
// symlink present beforehand is caught by the same comparison, because lstat
// describes the link and the open describes its target.
func openVerifiedChild(parent *os.Root, name string) (*os.Root, error) {
	child, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	opened, err := child.Stat(".")
	if err != nil {
		_ = child.Close()
		return nil, err
	}
	named, err := parent.Lstat(name)
	if err != nil {
		_ = child.Close()
		return nil, err
	}
	if named.Mode()&os.ModeSymlink != 0 || !os.SameFile(named, opened) {
		_ = child.Close()
		return nil, fmt.Errorf(
			"%w: %s is a symlink or was substituted",
			ErrUnsafeSnapshotPath, name,
		)
	}
	return child, nil
}

// openVerifiedDirPath walks a slash-separated path of directories, verifying
// each component. The caller closes the result.
func openVerifiedDirPath(root *os.Root, rel string) (*os.Root, error) {
	current, err := root.OpenRoot(".")
	if err != nil {
		return nil, err
	}
	if rel == "." || rel == "" {
		return current, nil
	}
	for name := range strings.SplitSeq(rel, "/") {
		next, err := openVerifiedChild(current, name)
		_ = current.Close()
		if err != nil {
			return nil, err
		}
		current = next
	}
	return current, nil
}

// openVerifiedFile opens a file directly beneath dir on the same terms as
// openVerifiedChild: the open handle must describe the entry the name denotes,
// which a symlink or a substitution does not. The caller closes the result.
func openVerifiedFile(dir *os.Root, name string) (*os.File, error) {
	f, err := dir.Open(name)
	if err != nil {
		return nil, err
	}
	opened, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	named, err := dir.Lstat(name)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if named.Mode()&os.ModeSymlink != 0 || !os.SameFile(named, opened) {
		_ = f.Close()
		return nil, fmt.Errorf(
			"%w: %s is a symlink or was substituted",
			ErrUnsafeSnapshotPath, name,
		)
	}
	return f, nil
}

// isVerifiedEntry reports whether rel, relative to root, is an entry of the
// wanted kind reachable without crossing a symlink. It is the predicate form of
// the opens above, for deciding between candidates before committing to one.
func isVerifiedEntry(root *os.Root, rel string, wantDir bool) bool {
	dirRel, name := path.Split(rel)
	dir, err := openVerifiedDirPath(root, path.Clean(dirRel))
	if err != nil {
		return false
	}
	defer dir.Close()
	info, err := dir.Lstat(name)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	return info.IsDir() == wantDir
}

// SnapshotFiles is a ledger state snapshot opened during discovery: the files
// themselves, not names for them.
//
// Handing back open files is the point. A name would be resolved again by
// whoever reads it, and the tree this searched is only guaranteed to be the
// tree that gets read if no resolution happens in between.
type SnapshotFiles struct {
	// State is the open ledger state file.
	State *os.File
	// StatePath is the slash-separated path State was found at, for messages.
	StatePath string
	// Table is the open UTxO-HD table, or nil for legacy snapshots that embed
	// their UTxO set in the state file.
	Table *os.File
	// TablePath is the slash-separated path Table was found at, for messages.
	TablePath string
}

// Close releases the open files.
func (s *SnapshotFiles) Close() {
	if s == nil {
		return
	}
	if s.State != nil {
		_ = s.State.Close()
		s.State = nil
	}
	if s.Table != nil {
		_ = s.Table.Close()
		s.Table = nil
	}
}

// findLedgerDirIn is findLedgerDir resolved through root, returning the
// slash-separated path of the ledger directory relative to it.
func findLedgerDirIn(root *os.Root) (string, error) {
	for _, c := range []string{"ledger", "db/ledger"} {
		if isVerifiedEntry(root, c, true) {
			return c, nil
		}
	}
	return "", fmt.Errorf(
		"%w under %s (checked ledger/ and db/ledger/)",
		ErrLedgerDirNotFound,
		root.Name(),
	)
}

// OpenSnapshotAtOrBefore is FindLedgerStateFileAtOrBefore for an untrusted
// tree: it searches through an open handle on the extracted snapshot directory
// and returns the selected files already open, together with the UTxO-HD table
// belonging to the same state.
//
// Every component is verified on the way down, so a symlinked ledger
// directory, slot directory, state file, or table is refused rather than
// followed. Extraction never writes a symlink, so one here is planted.
//
// The caller closes the result.
func OpenSnapshotAtOrBefore(
	root *os.Root,
	maxSlot uint64,
) (*SnapshotFiles, error) {
	ledgerRel, err := findLedgerDirIn(root)
	if err != nil {
		return nil, err
	}
	ledgerRoot, err := openVerifiedDirPath(root, ledgerRel)
	if err != nil {
		return nil, fmt.Errorf("opening ledger directory: %w", err)
	}
	defer ledgerRoot.Close()

	entries, err := fs.ReadDir(ledgerRoot.FS(), ".")
	if err != nil {
		return nil, fmt.Errorf("reading ledger directory: %w", err)
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
			if isVerifiedEntry(
				ledgerRoot, path.Join(name, "state"), false,
			) {
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
	if utxoHDDirs = sortNumericDesc(utxoHDDirs); len(utxoHDDirs) > 0 {
		return openUTxOHDSnapshot(ledgerRoot, ledgerRel, utxoHDDirs[0])
	}

	if legacyFiles = sortNumericSuffixDesc(legacyFiles); len(legacyFiles) > 0 {
		state, err := openVerifiedFile(ledgerRoot, legacyFiles[0])
		if err != nil {
			return nil, fmt.Errorf(
				"opening ledger state %s: %w", legacyFiles[0], err,
			)
		}
		return &SnapshotFiles{
			State:     state,
			StatePath: path.Join(ledgerRel, legacyFiles[0]),
		}, nil
	}

	return nil, fmt.Errorf(
		"no ledger state files at or before slot %d found under %s",
		maxSlot,
		root.Name(),
	)
}

// openUTxOHDSnapshot opens ledger/<slot>/state and the UTxO table beside it,
// both through a handle on the slot directory, so the state and the table are
// taken from one directory rather than from a name resolved twice.
func openUTxOHDSnapshot(
	ledgerRoot *os.Root,
	ledgerRel string,
	slotDir string,
) (*SnapshotFiles, error) {
	slotRoot, err := openVerifiedChild(ledgerRoot, slotDir)
	if err != nil {
		return nil, fmt.Errorf(
			"opening ledger state directory %s: %w", slotDir, err,
		)
	}
	defer slotRoot.Close()

	state, err := openVerifiedFile(slotRoot, "state")
	if err != nil {
		return nil, fmt.Errorf(
			"opening ledger state %s/state: %w", slotDir, err,
		)
	}
	files := &SnapshotFiles{
		State:     state,
		StatePath: path.Join(ledgerRel, slotDir, "state"),
	}

	// Current snapshots store the table as ledger/<slot>/tables, older exports
	// as ledger/<slot>/tables/tvar. Absent means a legacy snapshot that embeds
	// its UTxO set in the state file.
	//
	// A symlink at either name is refused rather than treated as absent.
	// Silently dropping planted content is how it gets loaded anyway later, and
	// the caller cannot tell "this snapshot has no table" from "this snapshot's
	// table is somebody else's".
	info, err := slotRoot.Lstat("tables")
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return files, nil
		}
		files.Close()
		return nil, fmt.Errorf(
			"inspecting UTxO table %s/tables: %w", slotDir, err,
		)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		files.Close()
		return nil, fmt.Errorf(
			"%w: %s/tables is a symlink", ErrUnsafeSnapshotPath, slotDir,
		)
	}

	tableDir, tableName, tableRel := slotRoot, "tables", "tables"
	if info.IsDir() {
		// Older layout: the table is tables/tvar.
		nested, err := openVerifiedChild(slotRoot, "tables")
		if err != nil {
			files.Close()
			return nil, fmt.Errorf(
				"opening UTxO table directory %s/tables: %w", slotDir, err,
			)
		}
		defer nested.Close()
		tableDir, tableName, tableRel = nested, "tvar", "tables/tvar"
	}

	table, err := openVerifiedFile(tableDir, tableName)
	if err != nil {
		files.Close()
		return nil, fmt.Errorf(
			"opening UTxO table %s/%s: %w", slotDir, tableRel, err,
		)
	}
	files.Table = table
	files.TablePath = path.Join(ledgerRel, slotDir, tableRel)
	return files, nil
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
