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

// Package compensate provides a disk-spooled compensation log for cloud blob
// transactions.
//
// A cloud bucket has no transaction primitive, so a commit that fails partway
// through must undo the object mutations it already applied. Undoing an
// overwrite needs the object's prior bytes, but holding every prior value in
// memory would let one multi-key commit (a block plus its metadata and indexes)
// retain gigabytes. This log spools prior values to a temporary file and keeps
// only their offsets in memory, and records nothing at all for keys that did
// not previously exist — the common append-only case, whose undo is a delete.
package compensate

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// entry is one recorded pre-commit object state.
type entry struct {
	key string
	// existed reports whether the object was present before the commit. When
	// false, offset/length are unset and the undo action is a delete.
	existed bool
	offset  int64
	length  int64
}

// Log records the pre-commit state of every object a commit is about to change.
// A Log is not safe for concurrent use; a blob transaction commits from a single
// goroutine.
type Log struct {
	file    *os.File
	size    int64
	entries []entry
}

// NewLog creates an empty compensation log. The caller must Close it to release
// the spool file, whether or not the commit succeeds.
func NewLog(namePattern string) (*Log, error) {
	file, err := os.CreateTemp("", namePattern)
	if err != nil {
		return nil, fmt.Errorf("compensation log: create spool file: %w", err)
	}
	return &Log{file: file}, nil
}

// RecordMissing notes that key had no object before the commit, so undoing a
// write to it means deleting it. No spool space is used.
func (l *Log) RecordMissing(key string) {
	l.entries = append(l.entries, entry{key: key})
}

// RecordValue spools key's prior bytes so an overwrite or delete can be undone.
func (l *Log) RecordValue(key string, value []byte) error {
	if l.file == nil {
		return errors.New("compensation log: closed")
	}
	written, err := l.file.WriteAt(value, l.size)
	if err != nil {
		return fmt.Errorf(
			"compensation log: spool prior value for %q: %w",
			key,
			err,
		)
	}
	l.record(key, int64(written))
	return nil
}

// RecordValueFrom streams key's prior bytes from r into the spool instead of
// buffering them first. Callers pass the raw object reader, deliberately not a
// size-capped one: the cap on ordinary reads exists to bound memory, and this
// path never holds the value in memory, so applying it here would make an
// object larger than the cap impossible to overwrite or delete inside a
// transaction. Spool space is bounded by the size of the objects a single commit
// replaces.
func (l *Log) RecordValueFrom(key string, r io.Reader) error {
	if l.file == nil {
		return errors.New("compensation log: closed")
	}
	written, err := io.Copy(
		io.NewOffsetWriter(l.file, l.size),
		r,
	)
	if err != nil {
		// Leave l.size past the partial write: the bytes are unusable, and
		// advancing avoids a later entry overlapping them.
		l.size += written
		return fmt.Errorf(
			"compensation log: spool prior value for %q: %w",
			key,
			err,
		)
	}
	l.record(key, written)
	return nil
}

func (l *Log) record(key string, length int64) {
	l.entries = append(l.entries, entry{
		key:     key,
		existed: true,
		offset:  l.size,
		length:  length,
	})
	l.size += length
}

// Len returns the number of recorded entries. Entry i corresponds to the i'th
// key the commit applies, so Undo(i) reverses exactly the changes already made
// when the i'th key failed.
func (l *Log) Len() int {
	return len(l.entries)
}

// Undo reverses the first n recorded changes, most recent first. Restoring an
// object that existed calls put with its prior bytes; one that did not exist
// calls del.
//
// Every entry is attempted even after a failure, so a single unreachable key
// does not abandon the rest of the compensation. The returned error joins all
// failures; a non-nil result means the bucket is left partially committed and
// callers must surface that rather than reporting a clean rollback.
func (l *Log) Undo(
	n int,
	put func(key string, value []byte) error,
	del func(key string) error,
) error {
	if n > len(l.entries) {
		n = len(l.entries)
	}
	var errs []error
	for i := n - 1; i >= 0; i-- {
		item := l.entries[i]
		if !item.existed {
			if err := del(item.key); err != nil {
				errs = append(errs, fmt.Errorf(
					"remove %q written by the failed commit: %w",
					item.key,
					err,
				))
			}
			continue
		}
		value, err := l.readValue(item)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := put(item.key, value); err != nil {
			errs = append(errs, fmt.Errorf(
				"restore prior value of %q: %w",
				item.key,
				err,
			))
		}
	}
	return errors.Join(errs...)
}

func (l *Log) readValue(item entry) ([]byte, error) {
	if l.file == nil {
		return nil, fmt.Errorf(
			"compensation log: closed before restoring %q",
			item.key,
		)
	}
	value := make([]byte, item.length)
	if _, err := io.ReadFull(
		io.NewSectionReader(l.file, item.offset, item.length),
		value,
	); err != nil {
		return nil, fmt.Errorf(
			"compensation log: read spooled value for %q: %w",
			item.key,
			err,
		)
	}
	return value, nil
}

// Close releases and removes the spool file. It is safe to call more than once.
func (l *Log) Close() error {
	if l.file == nil {
		return nil
	}
	name := l.file.Name()
	err := l.file.Close()
	l.file = nil
	if removeErr := os.Remove(name); removeErr != nil &&
		!errors.Is(removeErr, os.ErrNotExist) {
		err = errors.Join(err, removeErr)
	}
	return err
}
