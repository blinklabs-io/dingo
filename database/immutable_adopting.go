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

package database

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/internal/fsyncdir"
)

// immutableAdoptingFile names the sidecar in the ImmutableDB directory that
// records the chunk a bulk import is moving in. Its name is not a chunk file
// name, so chunk listings and snapshots ignore it.
const immutableAdoptingFile = "adopting.pending"

// ErrImmutableAdoptionPending reports chunk files that a bulk import placed
// in the ImmutableDB directory and has not yet covered with the recorded tip.
// They are certified bytes the import still has to adopt, so nothing may
// reconcile, truncate or append to the directory until the import resumes.
var ErrImmutableAdoptionPending = errors.New(
	"a chunk adoption is pending in the immutable directory",
)

// BeginImmutableAdopt durably records that the import is about to move a
// group of chunks starting at chunk into the ImmutableDB directory, before
// the first file moves. From then until ClearImmutableAdopt, reconciliation
// leaves the directory alone: a stop after the files moved but before
// AdoptImmutableTip covers them would otherwise read them as bytes beyond the
// recorded tip and cut them, after the source copy is already gone.
// Repeating the call is safe. The caller holds LockImmutable.
func (d *Database) BeginImmutableAdopt(chunk uint64) error {
	rt := d.immutable
	if rt == nil {
		return ErrImmutableNotEnabled
	}
	if err := os.MkdirAll(rt.dir, 0o755); err != nil { // #nosec G301
		return fmt.Errorf("begin immutable adopt: %w", err)
	}
	path := filepath.Join(rt.dir, immutableAdoptingFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(
		tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644,
	) // #nosec G302
	if err != nil {
		return fmt.Errorf("begin immutable adopt: %w", err)
	}
	_, err = f.WriteString(strconv.FormatUint(chunk, 10) + "\n")
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("begin immutable adopt: %w", err)
	}
	if err := fsyncdir.Sync(rt.dir); err != nil {
		return fmt.Errorf("begin immutable adopt: %w", err)
	}
	return nil
}

// ClearImmutableAdopt removes the record BeginImmutableAdopt wrote, once a
// durable recorded tip covers every moved file of the group (the blob store
// has synced after AdoptImmutableTip) or the files are gone. It is a no-op
// when none exists. The caller holds LockImmutable.
func (d *Database) ClearImmutableAdopt() error {
	rt := d.immutable
	if rt == nil {
		return ErrImmutableNotEnabled
	}
	err := os.Remove(filepath.Join(rt.dir, immutableAdoptingFile))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("clear immutable adopt: %w", err)
	}
	if err := fsyncdir.Sync(rt.dir); err != nil &&
		!errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("clear immutable adopt: %w", err)
	}
	return nil
}

// ImmutableAdoptPending returns the first chunk of the group a bulk import
// recorded as being moved in, when the import has not cleared it.
func (d *Database) ImmutableAdoptPending() (uint64, bool, error) {
	rt := d.immutable
	if rt == nil {
		return 0, false, nil
	}
	data, err := os.ReadFile(filepath.Join(rt.dir, immutableAdoptingFile))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read immutable adopt record: %w", err)
	}
	chunk, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse immutable adopt record: %w", err)
	}
	return chunk, true, nil
}
