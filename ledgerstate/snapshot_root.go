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
	"cmp"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
)

// ErrUnsafeSnapshotPath reports an entry in a snapshot tree that is a symlink,
// that is not the kind of thing it should be, or that was replaced between
// being opened and being checked.
var ErrUnsafeSnapshotPath = errors.New("unsafe snapshot path")

// errAbsent reports a name that nothing occupies.
//
// It exists so that choosing between candidates can never be influenced by
// planted content. An entry that is present but unusable has to be
// distinguishable from one that was never there, or making a candidate
// unusable becomes a way of selecting the next one.
var errAbsent = errors.New("no such entry")

// ErrNoUsableLedgerState reports a tree that holds no ledger state a caller can
// use: no ledger directory at all, or none at or below the slot asked for.
//
// It is the only outcome a caller searching several trees may treat as "look
// somewhere else". Everything else means the tree holds something unusable, and
// moving on from that would let planted content pick which tree gets imported.
var ErrNoUsableLedgerState = errors.New("no usable ledger state")

// openVerifiedChild opens a directory directly beneath parent and confirms the
// handle refers to the entry that name refers to.
//
// Opening cannot be made to reject a symlink outright — Root follows one whose
// target stays inside the root — so the handle is compared against the entry
// afterwards instead. A writer who substitutes the name between the open and
// the comparison leaves the two disagreeing, which is what this rejects; a
// symlink present beforehand is caught by the same comparison, because lstat
// describes the link and the open describes its target.
// It reports errAbsent when nothing occupies name, so a caller choosing between
// candidate layouts can tell "this one is not here" from "this one is here and
// unusable" — the second must never be skipped over.
func openVerifiedChild(parent *os.Root, name string) (*os.Root, error) {
	// Absence is settled on the entry, not on the outcome of opening it.
	// Opening fails the same way for an absent name, a dangling symlink, and a
	// symlink to a non-directory, and only the first is benign.
	info, err := parent.Lstat(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", errAbsent, name)
		}
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf(
			"%w: %s is a symlink", ErrUnsafeSnapshotPath, name,
		)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf(
			"%w: %s is not a directory", ErrUnsafeSnapshotPath, name,
		)
	}
	// Anything from here on is a refusal, including a name that has since
	// vanished: it existed a moment ago, so its disappearance is a change under
	// us rather than the benign absence handled above.
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

// openVerifiedFile opens a regular file directly beneath dir on the same terms
// as openVerifiedChild: the open handle must describe the entry the name
// denotes, which a symlink or a substitution does not. The caller closes the
// result.
//
// Anything that is not a regular file is refused rather than handed back. A
// directory opens perfectly well, and a caller that went on to read it would
// get an error from somewhere further away, about something else.
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
	if !named.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf(
			"%w: %s is not a regular file", ErrUnsafeSnapshotPath, name,
		)
	}
	return f, nil
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

// openLedgerDirIn is findLedgerDir resolved through root, returning the ledger
// directory open rather than a name for it, plus that name for messages.
//
// Each candidate is opened to decide it, not inspected and then opened. Asking
// whether "ledger" is a directory and later opening "ledger" are two questions,
// and a writer between them can make the answers describe different
// directories — the second would still be verified, just not the one the choice
// was made about. The caller closes the result.
//
// Only an absent candidate moves on to the next. One that exists but is a
// symlink, is not a directory, or cannot be opened fails the lookup, because
// falling through would let a planted `ledger` entry choose the `db/ledger`
// layout — the same trick as making the newest slot directory unusable, applied
// to which layout gets read.
func openLedgerDirIn(root *os.Root) (*os.Root, string, error) {
	for _, rel := range []string{"ledger", "db/ledger"} {
		dir, err := openVerifiedDirPath(root, rel)
		if err == nil {
			return dir, rel, nil
		}
		if errors.Is(err, errAbsent) {
			continue
		}
		return nil, "", fmt.Errorf(
			"inspecting ledger directory %s: %w", rel, err,
		)
	}
	return nil, "", fmt.Errorf(
		"%w: %w under %s (checked ledger/ and db/ledger/)",
		ErrNoUsableLedgerState,
		ErrLedgerDirNotFound,
		root.Name(),
	)
}

// slotCandidate is one entry in a ledger directory that names a slot, together
// with what kind of thing it is — a UTxO-HD slot directory or a legacy state
// file. Both layouts go into one ordered list so that selection is by slot
// rather than by layout.
type slotCandidate struct {
	name string
	slot uint64
	dir  bool
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
	ledgerRoot, ledgerRel, err := openLedgerDirIn(root)
	if err != nil {
		return nil, err
	}
	defer ledgerRoot.Close()

	entries, err := fs.ReadDir(ledgerRoot.FS(), ".")
	if err != nil {
		return nil, fmt.Errorf("reading ledger directory: %w", err)
	}

	// Every entry naming a slot becomes a candidate or a refusal. None is
	// dropped, because dropping one is indistinguishable from it never having
	// been there — and that is exactly the difference a planted entry trades
	// on. A name that parses as a slot is already a legitimate state name:
	// stripLedgerSuffix only removes the two state suffixes, so `.checksum`,
	// `.lock` and `.tmp` companions fail the parse rather than needing to be
	// excluded by name here.
	candidates := make([]slotCandidate, 0, len(entries))
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
		switch {
		case e.IsDir():
			// UTxO-HD format: directory named by slot number. Whether it
			// actually holds a state is settled by opening it below, not by a
			// check here that the open would then have to repeat.
			candidates = append(
				candidates, slotCandidate{name: name, slot: slot, dir: true},
			)
		case e.Type().IsRegular():
			// Legacy format: the state is the file itself.
			candidates = append(
				candidates, slotCandidate{name: name, slot: slot},
			)
		default:
			// Present, named like a slot, and neither of the two things a slot
			// is ever written as. ReadDir reports a symlink as a symlink
			// rather than as whatever it points at, so this is where one
			// lands — and extraction never writes one, which makes it evidence
			// somebody else did.
			//
			// Refused rather than ignored. Ignoring it reads as "there is no
			// slot 200", so a real slot 200 replaced by a link would hand the
			// import to slot 100 and report nothing wrong.
			return nil, fmt.Errorf(
				"%w: %s is neither a slot directory nor a state file",
				ErrUnsafeSnapshotPath, path.Join(ledgerRel, name),
			)
		}
	}

	// Newest slot first across both layouts, UTxO-HD winning a tie on the same
	// slot number.
	//
	// Preferring UTxO-HD is a tie-break between layouts, not a licence to
	// import an older state than the tree holds. Draining the directories
	// first and only then looking at the files would let a numeric *file* at a
	// newer slot be pre-empted by a directory at an older one — the same
	// demotion the refusal above prevents, arrived at without planting
	// anything the enumeration would reject.
	slices.SortFunc(candidates, func(a, b slotCandidate) int {
		if a.slot != b.slot {
			return cmp.Compare(b.slot, a.slot)
		}
		if a.dir != b.dir {
			if a.dir {
				return -1
			}
			return 1
		}
		return strings.Compare(a.name, b.name)
	})

	// A slot directory that never had a state is not a candidate and the next
	// one down is tried; anything else — a symlink, a substitution, a state or
	// table that exists but cannot be read — fails the snapshot rather than
	// quietly selecting an older one. Falling through on those would let
	// planted content decide which ledger state gets imported, by making the
	// newest one unusable.
	//
	// Only errNoStateEntry means "never had one". Skipping on ErrNotExist
	// instead would read a dangling symlink as an absent state, since opening
	// one fails exactly the same way — and a dangling symlink is planted
	// content, which is the thing that must not get to choose.
	for _, candidate := range candidates {
		if !candidate.dir {
			state, err := openVerifiedFile(ledgerRoot, candidate.name)
			if err != nil {
				return nil, fmt.Errorf(
					"opening ledger state %s: %w", candidate.name, err,
				)
			}
			return &SnapshotFiles{
				State:     state,
				StatePath: path.Join(ledgerRel, candidate.name),
			}, nil
		}
		files, err := openUTxOHDSnapshot(ledgerRoot, ledgerRel, candidate.name)
		if err == nil {
			return files, nil
		}
		if errors.Is(err, errNoStateEntry) {
			continue
		}
		return nil, err
	}

	return nil, fmt.Errorf(
		"%w: no ledger state files at or before slot %d found under %s",
		ErrNoUsableLedgerState,
		maxSlot,
		root.Name(),
	)
}

// errNoStateEntry reports a slot directory with no state entry at all — an
// extraction that was interrupted before writing one, which is ordinary and
// means the slot is simply not a candidate.
//
// It is deliberately not ErrNotExist. Opening a dangling symlink fails with
// ErrNotExist too, and the two must not be confused: one is a slot that never
// had a state, the other is one whose state somebody replaced with a link to
// nothing.
var errNoStateEntry = errors.New("slot directory holds no ledger state")

// openUTxOHDSnapshot opens ledger/<slot>/state and the UTxO table beside it,
// both through a handle on the slot directory, so the state and the table are
// taken from one directory rather than from a name resolved twice.
//
// It returns errNoStateEntry when the slot holds no state entry, and any other
// error means the caller must refuse rather than try an older slot.
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

	// Whether a state entry exists is settled by lstat, which describes the
	// entry rather than whatever it points at. Deciding this by trying to open
	// it would make a dangling symlink indistinguishable from an absent state,
	// and an attacker could then pick the imported state by pointing the newest
	// one at nothing.
	//
	// Past this point the entry exists, so every failure is a refusal.
	if _, err := slotRoot.Lstat("state"); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, errNoStateEntry
		}
		return nil, fmt.Errorf(
			"inspecting ledger state %s/state: %w", slotDir, err,
		)
	}

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
