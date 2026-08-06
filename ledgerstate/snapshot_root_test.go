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
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// requireSymlink creates a symlink, skipping on platforms where an
// unprivileged process cannot.
func requireSymlink(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("cannot create symlinks unprivileged: %s", err)
		}
		t.Fatalf("unexpected error: %s", err)
	}
}

// writeUTxOHDSnapshot lays out ledger/<slot>/{state,tables} with the given
// contents and returns the tree's root directory.
func writeUTxOHDSnapshot(t *testing.T, slot, state, table string) string {
	t.Helper()
	dir := t.TempDir()
	slotDir := filepath.Join(dir, "ledger", slot)
	if err := os.MkdirAll(slotDir, 0o750); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	for name, content := range map[string]string{
		"state":  state,
		"tables": table,
	} {
		if err := os.WriteFile(
			filepath.Join(slotDir, name), []byte(content), 0o640,
		); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}
	return dir
}

func openTree(t *testing.T, dir string) *os.Root {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root
}

// TestOpenSnapshotAtOrBeforeReturnsOpenFiles pins the ordinary case: discovery
// hands back the state and its UTxO table already open, from one slot
// directory.
func TestOpenSnapshotAtOrBeforeReturnsOpenFiles(t *testing.T) {
	dir := writeUTxOHDSnapshot(t, "100", "state bytes", "table bytes")

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer files.Close()

	for _, tc := range []struct {
		name string
		file *os.File
		want string
	}{
		{"state", files.State, "state bytes"},
		{"table", files.Table, "table bytes"},
	} {
		if tc.file == nil {
			t.Fatalf("%s was not opened", tc.name)
		}
		got, err := io.ReadAll(tc.file)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if string(got) != tc.want {
			t.Fatalf(
				"%s: expected %q, got %q", tc.name, tc.want, string(got),
			)
		}
	}
}

// TestOpenSnapshotAtOrBeforeSurvivesFileSwap covers the window discovery used
// to leave open: it returned a name, and whoever read that name later got
// whatever occupied it by then.
//
// Nothing resolves the name now — the file is opened as it is selected — so a
// replacement dropped in afterwards is not what gets parsed. The swap is
// staged rather than raced because the window is interior to the import.
func TestOpenSnapshotAtOrBeforeSurvivesFileSwap(t *testing.T) {
	dir := writeUTxOHDSnapshot(t, "100", "ours", "our table")

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer files.Close()

	// A writer replaces both files after discovery selected them, in the
	// window before the import reads them.
	slotDir := filepath.Join(dir, "ledger", "100")
	for name, content := range map[string]string{
		"state":  "theirs",
		"tables": "their table",
	} {
		replacement := filepath.Join(slotDir, name+".new")
		if err := os.WriteFile(
			replacement, []byte(content), 0o640,
		); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := os.Rename(
			replacement, filepath.Join(slotDir, name),
		); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	// The premise: the names denote the writer's files now.
	byName, err := os.ReadFile(filepath.Join(slotDir, "state"))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if string(byName) != "theirs" {
		t.Fatal(
			"the substitution must be observable through the name, or " +
				"this test proves nothing",
		)
	}

	for _, tc := range []struct {
		name string
		file *os.File
		want string
	}{
		{"state", files.State, "ours"},
		{"table", files.Table, "our table"},
	} {
		got, err := io.ReadAll(tc.file)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if string(got) != tc.want {
			t.Fatalf(
				"%s must come from the file discovery opened: "+
					"expected %q, got %q",
				tc.name, tc.want, string(got),
			)
		}
	}
}

// TestOpenSnapshotAtOrBeforeRefusesSymlinks covers every component of the
// discovered path.
//
// os.Root confines traversal to the root but still follows a symlink whose
// target stays inside it. Extraction never writes a symlink, so one in an
// extracted tree is evidence of tampering, and following it would import a
// ledger state somebody else selected.
func TestOpenSnapshotAtOrBeforeRefusesSymlinks(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(t *testing.T) string
	}{
		{
			name: "symlinked ledger directory",
			build: func(t *testing.T) string {
				dir := writeUTxOHDSnapshot(t, "100", "theirs", "theirs")
				moved := filepath.Join(dir, "real")
				if err := os.Rename(
					filepath.Join(dir, "ledger"), moved,
				); err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				requireSymlink(t, "real", filepath.Join(dir, "ledger"))
				return dir
			},
		},
		{
			name: "symlinked slot directory",
			build: func(t *testing.T) string {
				dir := writeUTxOHDSnapshot(t, "100", "theirs", "theirs")
				ledger := filepath.Join(dir, "ledger")
				if err := os.Rename(
					filepath.Join(ledger, "100"),
					filepath.Join(ledger, "real"),
				); err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				requireSymlink(t, "real", filepath.Join(ledger, "100"))
				return dir
			},
		},
		{
			name: "symlinked state file",
			build: func(t *testing.T) string {
				dir := writeUTxOHDSnapshot(t, "100", "theirs", "theirs")
				slotDir := filepath.Join(dir, "ledger", "100")
				if err := os.Rename(
					filepath.Join(slotDir, "state"),
					filepath.Join(slotDir, "real"),
				); err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
				requireSymlink(t, "real", filepath.Join(slotDir, "state"))
				return dir
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := tc.build(t)
			files, err := OpenSnapshotAtOrBefore(
				openTree(t, dir), ^uint64(0),
			)
			if err == nil {
				files.Close()
				t.Fatal("expected a symlinked component to be refused")
			}
		})
	}
}

// TestOpenSnapshotAtOrBeforeRefusesSymlinkedTable keeps the UTxO table on the
// same footing as the state: a snapshot whose table is planted is refused
// outright rather than imported without its UTxO set.
func TestOpenSnapshotAtOrBeforeRefusesSymlinkedTable(t *testing.T) {
	dir := writeUTxOHDSnapshot(t, "100", "ours", "theirs")
	slotDir := filepath.Join(dir, "ledger", "100")
	if err := os.Rename(
		filepath.Join(slotDir, "tables"),
		filepath.Join(slotDir, "real"),
	); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	requireSymlink(t, "real", filepath.Join(slotDir, "tables"))

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err == nil {
		files.Close()
		t.Fatal("expected a symlinked UTxO table to be refused")
	}
	if !errors.Is(err, ErrUnsafeSnapshotPath) {
		t.Fatalf("expected ErrUnsafeSnapshotPath, got %v", err)
	}
}

// TestOpenSnapshotAtOrBeforeHonoursMaxSlot pins that the trust boundary still
// applies: a ledger state above the certified immutable tip is not selected.
func TestOpenSnapshotAtOrBeforeHonoursMaxSlot(t *testing.T) {
	dir := writeUTxOHDSnapshot(t, "100", "older", "older table")
	newer := filepath.Join(dir, "ledger", "200")
	if err := os.MkdirAll(newer, 0o750); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := os.WriteFile(
		filepath.Join(newer, "state"), []byte("newer"), 0o640,
	); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), 150)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer files.Close()
	got, err := io.ReadAll(files.State)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if string(got) != "older" {
		t.Fatalf("expected the state at or below maxSlot, got %q", string(got))
	}
}

// TestOpenSnapshotAtOrBeforeReadsLegacyTvarTable pins the older UTxO-HD layout,
// where the table is ledger/<slot>/tables/tvar rather than a file at tables.
func TestOpenSnapshotAtOrBeforeReadsLegacyTvarTable(t *testing.T) {
	dir := t.TempDir()
	tablesDir := filepath.Join(dir, "ledger", "100", "tables")
	if err := os.MkdirAll(tablesDir, 0o750); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	for path, content := range map[string]string{
		filepath.Join(dir, "ledger", "100", "state"): "state bytes",
		filepath.Join(tablesDir, "tvar"):             "tvar bytes",
	} {
		if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	}

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer files.Close()
	if files.Table == nil {
		t.Fatal("the legacy tvar table was not opened")
	}
	got, err := io.ReadAll(files.Table)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if string(got) != "tvar bytes" {
		t.Fatalf("expected the tvar table, got %q", string(got))
	}
}

// TestOpenSnapshotAtOrBeforeLegacyStateHasNoTable pins that a snapshot with no
// table at all is not an error: legacy states embed their UTxO set.
func TestOpenSnapshotAtOrBeforeLegacyStateHasNoTable(t *testing.T) {
	dir := t.TempDir()
	ledgerDir := filepath.Join(dir, "ledger")
	if err := os.MkdirAll(ledgerDir, 0o750); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := os.WriteFile(
		filepath.Join(ledgerDir, "100.lstate"), []byte("legacy"), 0o640,
	); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer files.Close()
	if files.Table != nil {
		t.Fatal("a legacy snapshot must not report a UTxO table")
	}
	got, err := io.ReadAll(files.State)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if string(got) != "legacy" {
		t.Fatalf("expected the legacy state, got %q", string(got))
	}
}

// TestOpenSnapshotAtOrBeforeSkipsStatelessSlotDir pins that a slot directory
// holding no state is not a candidate, and the next one down is used. Opening
// as we select has to keep that behaviour: it is what the old predicate pass
// decided, and a run of incomplete slot directories is ordinary.
func TestOpenSnapshotAtOrBeforeSkipsStatelessSlotDir(t *testing.T) {
	dir := writeUTxOHDSnapshot(t, "100", "older", "older table")
	// A newer slot directory that was never finished.
	if err := os.MkdirAll(
		filepath.Join(dir, "ledger", "200"), 0o750,
	); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer files.Close()
	got, err := io.ReadAll(files.State)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if string(got) != "older" {
		t.Fatalf("expected the complete older state, got %q", string(got))
	}
}

// TestOpenSnapshotAtOrBeforeRefusesRatherThanFallingBack pins the other half:
// a newest slot directory whose state is planted fails the snapshot instead of
// silently selecting an older one.
//
// Falling back would hand the choice of ledger state to whoever planted it —
// making the newest unusable is then enough to pick which state gets imported,
// which is a decision no attacker should get to make.
func TestOpenSnapshotAtOrBeforeRefusesRatherThanFallingBack(t *testing.T) {
	dir := writeUTxOHDSnapshot(t, "100", "older", "older table")
	newer := filepath.Join(dir, "ledger", "200")
	if err := os.MkdirAll(newer, 0o750); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := os.WriteFile(
		filepath.Join(newer, "real"), []byte("theirs"), 0o640,
	); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	requireSymlink(t, "real", filepath.Join(newer, "state"))

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err == nil {
		files.Close()
		t.Fatal(
			"a planted newest state must fail the snapshot, not fall back " +
				"to an older one",
		)
	}
	if !errors.Is(err, ErrUnsafeSnapshotPath) {
		t.Fatalf("expected ErrUnsafeSnapshotPath, got %v", err)
	}
}

// TestOpenSnapshotAtOrBeforeRefusesDanglingNewestState covers the case that
// makes "skip when the state is missing" dangerous.
//
// Opening a dangling symlink fails with ErrNotExist, exactly as opening an
// absent file does. Deciding the slot is not a candidate on that basis would
// let an attacker choose the imported ledger state by pointing the newest one
// at nothing — the fallback does the rest. Whether the entry exists is settled
// by lstat, which describes the link rather than its missing target.
func TestOpenSnapshotAtOrBeforeRefusesDanglingNewestState(t *testing.T) {
	dir := writeUTxOHDSnapshot(t, "100", "older", "older table")
	newer := filepath.Join(dir, "ledger", "200")
	if err := os.MkdirAll(newer, 0o750); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	requireSymlink(t, "missing", filepath.Join(newer, "state"))

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err == nil {
		files.Close()
		t.Fatal(
			"a dangling newest state must fail the snapshot, not read as " +
				"absent and fall back to an older slot",
		)
	}
	if errors.Is(err, errNoStateEntry) {
		t.Fatal("a dangling symlink is planted content, not an absent state")
	}
}

// TestOpenSnapshotAtOrBeforeRefusesMalformedNewestTable is the same argument
// one file over: a UTxO-HD slot whose tables directory holds no tvar is
// malformed, and skipping it would again let the newest slot be made unusable
// on purpose.
func TestOpenSnapshotAtOrBeforeRefusesMalformedNewestTable(t *testing.T) {
	dir := writeUTxOHDSnapshot(t, "100", "older", "older table")
	newer := filepath.Join(dir, "ledger", "200")
	if err := os.MkdirAll(filepath.Join(newer, "tables"), 0o750); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := os.WriteFile(
		filepath.Join(newer, "state"), []byte("newer"), 0o640,
	); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err == nil {
		files.Close()
		t.Fatal(
			"a newest slot with an empty tables directory must fail the " +
				"snapshot, not fall back to an older slot",
		)
	}
}

// writeDBLedgerSnapshot lays out db/ledger/<slot>/state, the alternate layout
// tried when a top-level ledger/ is absent.
func writeDBLedgerSnapshot(t *testing.T, dir, slot, state string) {
	t.Helper()
	slotDir := filepath.Join(dir, "db", "ledger", slot)
	if err := os.MkdirAll(slotDir, 0o750); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := os.WriteFile(
		filepath.Join(slotDir, "state"), []byte(state), 0o640,
	); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

// TestOpenSnapshotAtOrBeforeRefusesUnsafePreferredLayout covers layout
// selection with both layouts present.
//
// ledger/ is preferred over db/ledger/. Moving on from a ledger/ that exists
// but is unusable would let a planted entry choose the layout: an attacker who
// cannot write a convincing ledger/ can make the real one unopenable and have
// their db/ledger/ read instead. Only an absent ledger/ moves on.
func TestOpenSnapshotAtOrBeforeRefusesUnsafePreferredLayout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		plant   func(t *testing.T, dir string)
		wantErr error
	}{
		{
			name: "symlinked ledger",
			plant: func(t *testing.T, dir string) {
				requireSymlink(t, "db/ledger", filepath.Join(dir, "ledger"))
			},
			wantErr: ErrUnsafeSnapshotPath,
		},
		{
			name: "ledger is a regular file",
			plant: func(t *testing.T, dir string) {
				if err := os.WriteFile(
					filepath.Join(dir, "ledger"), []byte("x"), 0o640,
				); err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
			},
			wantErr: ErrUnsafeSnapshotPath,
		},
		{
			name: "dangling ledger symlink",
			plant: func(t *testing.T, dir string) {
				requireSymlink(t, "missing", filepath.Join(dir, "ledger"))
			},
			wantErr: ErrUnsafeSnapshotPath,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeDBLedgerSnapshot(t, dir, "100", "fallback")
			tc.plant(t, dir)

			files, err := OpenSnapshotAtOrBefore(
				openTree(t, dir), ^uint64(0),
			)
			if err == nil {
				files.Close()
				t.Fatal(
					"an unusable ledger/ must fail the lookup, not hand " +
						"layout selection to whoever planted it",
				)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected %v, got %v", tc.wantErr, err)
			}
		})
	}
}

// TestOpenSnapshotAtOrBeforeUsesDBLedgerWhenLedgerAbsent is the control: the
// fallback layout is still reached when ledger/ genuinely is not there, which
// is what makes the refusals above about planted content rather than about the
// fallback being unreachable.
func TestOpenSnapshotAtOrBeforeUsesDBLedgerWhenLedgerAbsent(t *testing.T) {
	dir := t.TempDir()
	writeDBLedgerSnapshot(t, dir, "100", "fallback")

	files, err := OpenSnapshotAtOrBefore(openTree(t, dir), ^uint64(0))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer files.Close()
	got, err := io.ReadAll(files.State)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if string(got) != "fallback" {
		t.Fatalf("expected the db/ledger state, got %q", string(got))
	}
}
