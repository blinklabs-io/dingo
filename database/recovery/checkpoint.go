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

package recovery

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const (
	// checkpointFilePrefix and checkpointFileSuffix bracket the zero-padded
	// sequence number in a checkpoint filename, so a lexical sort of the
	// directory is also a numeric sort by sequence.
	checkpointFilePrefix = "checkpoint-"
	checkpointFileSuffix = ".bin"
	// checkpointSeqDigits pads sequence numbers wide enough for uint64.
	checkpointSeqDigits = 20
	// defaultCheckpointRetain is how many checkpoint generations to keep. One
	// is enough to recover; keeping a few means a checkpoint corrupted by bad
	// media still leaves an older anchor to fall back to.
	defaultCheckpointRetain = 3
)

// ErrNoCheckpoint reports that no readable checkpoint exists yet. It is the
// expected result on a fresh database, not a failure.
var ErrNoCheckpoint = errors.New("no valid checkpoint found")

// Checkpoint is a merkle-rooted summary of agreed cross-store state at a point
// in the journal.
//
// It is a summary, not a snapshot of the data: recovery uses it as a verified
// anchor for what the stores agreed on at that sequence, and as the floor below
// which journal records need not be replayed. The merkle root binds every other
// field, so a checkpoint whose bytes were damaged in a way the frame checksum
// happens to survive still fails verification.
type Checkpoint struct {
	TipHash     []byte
	BlobTipHash []byte
	MerkleRoot  []byte
	// Seq is the journal sequence this checkpoint covers. Records at or
	// below it are already reflected in both stores.
	Seq uint64
	// CreatedUnixMilli is when the checkpoint was taken, for operator logs.
	CreatedUnixMilli int64
	// CommitTimestamp is the cross-store fence both stores held.
	CommitTimestamp int64
	TipSlot         uint64
	TipBlockNumber  uint64
	BlobTipSlot     uint64
}

// TipPoint returns the metadata tip the checkpoint recorded.
func (c *Checkpoint) TipPoint() Point {
	return Point{Slot: c.TipSlot, Hash: c.TipHash}
}

// BlobTipPoint returns the blob tip the checkpoint recorded.
func (c *Checkpoint) BlobTipPoint() Point {
	return Point{Slot: c.BlobTipSlot, Hash: c.BlobTipHash}
}

// merkleLeaves renders the checkpoint as an ordered, self-describing leaf set.
//
// Each leaf carries a field-name tag so two different field groups can never
// produce the same leaf bytes, and the order is fixed by this function rather
// than by struct layout so a future field reordering cannot silently change
// historical roots.
func (c *Checkpoint) merkleLeaves() [][]byte {
	leaf := func(tag string, encode func(*encoder)) []byte {
		e := &encoder{buf: append([]byte(tag), 0)}
		encode(e)
		return e.buf
	}
	return [][]byte{
		leaf("seq", func(e *encoder) { e.uint64(c.Seq) }),
		leaf("created_unix_milli", func(e *encoder) {
			e.int64(c.CreatedUnixMilli)
		}),
		leaf("commit_timestamp", func(e *encoder) {
			e.int64(c.CommitTimestamp)
		}),
		leaf("tip", func(e *encoder) {
			e.uint64(c.TipSlot)
			e.uint64(c.TipBlockNumber)
			e.bytesField(c.TipHash)
		}),
		leaf("blob_tip", func(e *encoder) {
			e.uint64(c.BlobTipSlot)
			e.bytesField(c.BlobTipHash)
		}),
	}
}

// ComputeMerkleRoot returns the root the checkpoint's contents imply.
func (c *Checkpoint) ComputeMerkleRoot() []byte {
	return MerkleRoot(c.merkleLeaves())
}

// Verify reports whether the recorded merkle root matches the contents.
func (c *Checkpoint) Verify() error {
	if len(c.MerkleRoot) == 0 {
		return errors.New("checkpoint has no merkle root")
	}
	if !bytes.Equal(c.MerkleRoot, c.ComputeMerkleRoot()) {
		return errors.New("checkpoint merkle root does not match contents")
	}
	return nil
}

// Seal fills in the merkle root over the checkpoint's current contents. Call it
// after every field is set and before writing the checkpoint anywhere.
func (c *Checkpoint) Seal() {
	c.MerkleRoot = c.ComputeMerkleRoot()
}

// encodeCheckpoint renders a checkpoint payload. Hash fields too long for the
// wire format are rejected by the encoder.
func encodeCheckpoint(c Checkpoint) ([]byte, error) {
	e := &encoder{}
	e.uint64(c.Seq)
	e.int64(c.CreatedUnixMilli)
	e.int64(c.CommitTimestamp)
	e.uint64(c.TipSlot)
	e.uint64(c.TipBlockNumber)
	e.bytesField(c.TipHash)
	e.uint64(c.BlobTipSlot)
	e.bytesField(c.BlobTipHash)
	e.bytesField(c.MerkleRoot)
	if e.err != nil {
		return nil, fmt.Errorf("encode checkpoint: %w", e.err)
	}
	return e.buf, nil
}

// decodeCheckpoint parses a checkpoint payload. It does not verify the merkle
// root; callers decide whether an unverified checkpoint is useful to them.
func decodeCheckpoint(payload []byte) (Checkpoint, error) {
	d := &decoder{buf: payload}
	var c Checkpoint
	c.Seq = d.uint64()
	c.CreatedUnixMilli = d.int64()
	c.CommitTimestamp = d.int64()
	c.TipSlot = d.uint64()
	c.TipBlockNumber = d.uint64()
	c.TipHash = d.bytesField()
	c.BlobTipSlot = d.uint64()
	c.BlobTipHash = d.bytesField()
	c.MerkleRoot = d.bytesField()
	if err := d.done(); err != nil {
		return Checkpoint{}, err
	}
	return c, nil
}

// CheckpointStore persists checkpoint generations as individual files.
//
// Each generation is written to a temporary file, fsynced, then renamed into
// place and the directory fsynced, so a crash mid-write leaves either the
// previous generation or the new one, never a half-written file that reads as
// valid.
type CheckpointStore struct {
	logger *slog.Logger
	dir    string
	mu     sync.Mutex
	retain int
}

// NewCheckpointStore opens (creating if needed) a checkpoint directory.
func NewCheckpointStore(
	dir string,
	retain int,
	logger *slog.Logger,
) (*CheckpointStore, error) {
	if dir == "" {
		return nil, errors.New("checkpoint directory is required")
	}
	if retain <= 0 {
		retain = defaultCheckpointRetain
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create checkpoint dir %q: %w", dir, err)
	}
	return &CheckpointStore{dir: dir, retain: retain, logger: logger}, nil
}

// Dir returns the directory holding checkpoint files.
func (s *CheckpointStore) Dir() string {
	return s.dir
}

func (s *CheckpointStore) path(seq uint64) string {
	return filepath.Join(
		s.dir,
		fmt.Sprintf(
			"%s%0*d%s",
			checkpointFilePrefix,
			checkpointSeqDigits,
			seq,
			checkpointFileSuffix,
		),
	)
}

// Write seals and durably stores a checkpoint generation, then prunes older
// generations beyond the retain count.
func (s *CheckpointStore) Write(c Checkpoint) error {
	c.Seal()
	record := Record{Type: RecordTypeCheckpoint, Seq: c.Seq, Checkpoint: &c}
	frame, err := appendFrame(nil, record)
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	final := s.path(c.Seq)
	tmp, err := os.CreateTemp(s.dir, checkpointFilePrefix+"*.tmp")
	if err != nil {
		return fmt.Errorf("create temp checkpoint: %w", err)
	}
	tmpName := tmp.Name()
	// Any failure past this point must not leave the temp file behind, and
	// the successful path renames it away before this runs.
	defer func() {
		if _, statErr := os.Stat(tmpName); statErr == nil {
			if rmErr := os.Remove(tmpName); rmErr != nil {
				s.logger.Warn(
					"failed to remove temp checkpoint",
					"path", tmpName,
					"error", rmErr,
				)
			}
		}
	}()
	if _, err := tmp.Write(frame); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		return fmt.Errorf("install checkpoint: %w", err)
	}
	// Without a directory fsync the rename itself can be lost, leaving the
	// new generation invisible after a crash even though its contents were
	// synced. It is best effort: Windows refuses fsync on a directory
	// handle outright, and losing the anchor for one generation is not
	// worth failing a checkpoint that is otherwise written and durable.
	if err := syncDir(s.dir); err != nil {
		s.logger.Warn(
			"failed to sync checkpoint dir after install",
			"dir", s.dir,
			"error", err,
		)
	}
	s.pruneLocked()
	return nil
}

// Latest returns the newest checkpoint that both decodes and verifies.
//
// Corrupt or unverifiable generations are skipped with a warning rather than
// failing the call, which is the whole point of retaining several: a damaged
// newest generation should fall back to an older good one. ErrNoCheckpoint is
// returned when none are usable.
func (s *CheckpointStore) Latest() (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seqs, err := s.generationsLocked()
	if err != nil {
		return Checkpoint{}, err
	}
	for _, seq := range slices.Backward(seqs) {
		path := s.path(seq)
		c, err := readCheckpointFile(path)
		if err != nil {
			s.logger.Warn(
				"skipping unusable checkpoint",
				"path", path,
				"error", err,
			)
			continue
		}
		return c, nil
	}
	return Checkpoint{}, ErrNoCheckpoint
}

// readCheckpointFile decodes and verifies a single checkpoint file.
func readCheckpointFile(path string) (Checkpoint, error) {
	f, err := os.Open(path)
	if err != nil {
		return Checkpoint{}, err
	}
	defer f.Close() //nolint:errcheck
	record, err := readFrame(f)
	if err != nil {
		return Checkpoint{}, err
	}
	if record.Type != RecordTypeCheckpoint || record.Checkpoint == nil {
		return Checkpoint{}, fmt.Errorf(
			"%w: file holds a %s record",
			ErrCorruptRecord,
			record.Type,
		)
	}
	if err := record.Checkpoint.Verify(); err != nil {
		return Checkpoint{}, err
	}
	return *record.Checkpoint, nil
}

// generationsLocked returns the stored sequence numbers in ascending order.
func (s *CheckpointStore) generationsLocked() ([]uint64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read checkpoint dir: %w", err)
	}
	var seqs []uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		seq, ok := parseCheckpointName(entry.Name())
		if !ok {
			continue
		}
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	return seqs, nil
}

// parseCheckpointName extracts the sequence number from a checkpoint filename.
func parseCheckpointName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, checkpointFilePrefix) ||
		!strings.HasSuffix(name, checkpointFileSuffix) {
		return 0, false
	}
	digits := name[len(checkpointFilePrefix) : len(name)-len(checkpointFileSuffix)]
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// pruneLocked removes generations beyond the retain count, oldest first.
func (s *CheckpointStore) pruneLocked() {
	seqs, err := s.generationsLocked()
	if err != nil {
		s.logger.Warn("failed to list checkpoints for pruning", "error", err)
		return
	}
	if len(seqs) <= s.retain {
		return
	}
	for _, seq := range seqs[:len(seqs)-s.retain] {
		path := s.path(seq)
		if err := os.Remove(path); err != nil &&
			!errors.Is(err, fs.ErrNotExist) {
			s.logger.Warn(
				"failed to prune checkpoint",
				"path", path,
				"error", err,
			)
		}
	}
}

// syncDir fsyncs a directory so a rename into it is durable.
//
// Not every platform allows it — Windows rejects fsync on a directory handle,
// and some filesystems do too — so callers treat a failure as a warning rather
// than an error. The rename is still ordered by the filesystem's own journal
// there.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck
	return f.Sync()
}
