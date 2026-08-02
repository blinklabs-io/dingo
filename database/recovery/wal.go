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
	"bufio"
	"errors"
	"fmt"
	"io"
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
	// segmentFilePrefix and segmentFileSuffix bracket the zero-padded first
	// sequence number in a segment filename, so a lexical directory sort is
	// also a numeric sort by sequence.
	segmentFilePrefix = "wal-"
	segmentFileSuffix = ".log"
	// segmentSeqDigits pads sequence numbers wide enough for uint64.
	segmentSeqDigits = 20
	// defaultMaxSegmentBytes bounds a segment so truncation after a
	// checkpoint can actually reclaim space. Records are tens of bytes, so
	// 8MiB holds on the order of a hundred thousand commits.
	defaultMaxSegmentBytes int64 = 8 << 20
)

// ErrWALClosed reports use of a journal that has been closed.
var ErrWALClosed = errors.New("recovery journal is closed")

var ErrWALUnusable = errors.New("recovery journal is unusable after an append failure")

// WALConfig configures a journal.
type WALConfig struct {
	Logger *slog.Logger
	// Dir holds the segment files. It is created if absent.
	Dir string
	// MaxSegmentBytes bounds one segment file. Zero selects the default.
	MaxSegmentBytes int64
	// SyncOnBegin makes each begin record durable before it is returned.
	//
	// This is what makes the journal useful: the intent has to reach disk
	// before the stores are touched, or a crash inside the commit window
	// leaves no record of what was in flight. Turn it off only for tests or
	// throughput measurement, never for a node holding real state.
	SyncOnBegin bool
}

// WAL is an append-only journal of cross-store commit intents.
//
// Only begin records are synced. A commit or abort record that a crash loses is
// harmless: replay then reports the commit as in flight, recovery compares the
// stores, finds them consistent, and does nothing. Paying a second fsync to
// avoid a no-op repair would double the cost of every block.
type WAL struct {
	logger  *slog.Logger
	file    *os.File
	writer  *bufio.Writer
	dir     string
	scratch []byte

	maxSegmentBytes int64
	mu              sync.Mutex
	nextSeq         uint64
	segmentFirstSeq uint64
	segmentBytes    int64
	syncOnBegin     bool
	closed          bool
	unusable        bool
}

// OpenWAL opens (creating if needed) a journal directory and positions the
// writer at the end of the newest segment.
//
// Existing segments are scanned to recover the next sequence number, so
// sequences keep increasing across restarts and a replay can tell records
// written before a restart from those written after.
func OpenWAL(cfg WALConfig) (*WAL, error) {
	if cfg.Dir == "" {
		return nil, errors.New("journal directory is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	maxSegment := cfg.MaxSegmentBytes
	if maxSegment <= 0 {
		maxSegment = defaultMaxSegmentBytes
	}
	if err := os.MkdirAll(cfg.Dir, 0o750); err != nil {
		return nil, fmt.Errorf("create journal dir %q: %w", cfg.Dir, err)
	}
	w := &WAL{
		dir:             cfg.Dir,
		logger:          logger,
		maxSegmentBytes: maxSegment,
		syncOnBegin:     cfg.SyncOnBegin,
		nextSeq:         1,
	}
	highest, err := w.scanHighestSeq()
	if err != nil {
		return nil, err
	}
	if highest >= w.nextSeq {
		w.nextSeq = highest + 1
	}
	if err := w.openSegmentLocked(); err != nil {
		return nil, err
	}
	return w, nil
}

// Dir returns the directory holding segment files.
func (w *WAL) Dir() string {
	return w.dir
}

// scanHighestSeq returns the highest sequence number any existing segment
// accounts for.
//
// Segment filenames count as well as record contents. A segment whose records
// were all lost — a crash between creating the file and flushing the record
// that prompted it, with the older segments already truncated away — would
// otherwise leave nothing to recover the counter from, and sequences would
// restart at 1 and collide with numbers a durable checkpoint already covers.
// The filename survives that crash because the directory entry is synced when
// the segment is created.
//
// A segment whose tail was torn by a crash stops contributing record sequences
// at the tear, which is correct: records past a corrupt frame cannot be trusted
// to be complete, and reusing their sequence numbers is safe because nothing
// durable depends on them.
func (w *WAL) scanHighestSeq() (uint64, error) {
	segments, err := w.segmentSeqs()
	if err != nil {
		return 0, err
	}
	var highest uint64
	for _, first := range segments {
		if first > highest {
			highest = first
		}
		path := w.segmentPath(first)
		records, err := readSegment(path, w.logger)
		if err != nil {
			return 0, err
		}
		for _, r := range records {
			if r.Seq > highest {
				highest = r.Seq
			}
		}
	}
	return highest, nil
}

func (w *WAL) segmentPath(firstSeq uint64) string {
	return filepath.Join(
		w.dir,
		fmt.Sprintf(
			"%s%0*d%s",
			segmentFilePrefix,
			segmentSeqDigits,
			firstSeq,
			segmentFileSuffix,
		),
	)
}

// segmentSeqs returns the first-sequence of every segment, ascending.
func (w *WAL) segmentSeqs() ([]uint64, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read journal dir: %w", err)
	}
	var seqs []uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		seq, ok := parseSegmentName(entry.Name())
		if !ok {
			continue
		}
		seqs = append(seqs, seq)
	}
	slices.Sort(seqs)
	return seqs, nil
}

// parseSegmentName extracts the first sequence from a segment filename.
func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentFilePrefix) ||
		!strings.HasSuffix(name, segmentFileSuffix) {
		return 0, false
	}
	digits := name[len(segmentFilePrefix) : len(name)-len(segmentFileSuffix)]
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// openSegmentLocked opens the segment that the next sequence belongs to,
// appending to it if it already exists.
func (w *WAL) openSegmentLocked() error {
	path := w.segmentPath(w.nextSeq)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("open journal segment %q: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat journal segment %q: %w", path, err)
	}
	w.file = f
	w.writer = bufio.NewWriter(f)
	w.segmentFirstSeq = w.nextSeq
	w.segmentBytes = info.Size()
	// A new segment's existence must be durable before records land in it,
	// otherwise a crash can lose the whole segment and with it the intent
	// records callers believe are on disk.
	if info.Size() == 0 {
		if err := syncDir(w.dir); err != nil {
			w.logger.Warn(
				"failed to sync journal dir after segment create",
				"error", err,
			)
		}
	}
	return nil
}

// rotateLocked closes the current segment and starts one at the next sequence.
func (w *WAL) rotateLocked() error {
	if err := w.flushLocked(true); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close journal segment: %w", err)
	}
	w.file = nil
	w.writer = nil
	return w.openSegmentLocked()
}

// flushLocked drains the buffered writer and optionally fsyncs the file.
func (w *WAL) flushLocked(sync bool) error {
	if w.writer == nil || w.file == nil {
		return ErrWALClosed
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush journal: %w", err)
	}
	if !sync {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync journal: %w", err)
	}
	return nil
}

// appendLocked frames and writes a record, rotating first when the current
// segment is full.
func (w *WAL) appendLocked(r Record, sync bool) error {
	if w.closed {
		return ErrWALClosed
	}
	if w.segmentBytes >= w.maxSegmentBytes &&
		w.segmentFirstSeq != w.nextSeq {
		// Rotate only at a record boundary and never into an empty
		// segment, so a segment always starts with the sequence its
		// filename claims.
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	frame, err := appendFrame(w.scratch[:0], r)
	if err != nil {
		return err
	}
	w.scratch = frame
	if w.writer == nil {
		return ErrWALClosed
	}
	n, err := w.writer.Write(frame)
	w.segmentBytes += int64(n)
	if err != nil {
		return fmt.Errorf("write journal record: %w", err)
	}
	return w.flushLocked(sync)
}

// Begin records the intent of a cross-store commit and returns its sequence.
//
// The record is durable before this returns when SyncOnBegin is set. Pass the
// returned sequence to Commit or Abort once the commit resolves.
func (w *WAL) Begin(intent Intent, commitTimestamp int64) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrWALClosed
	}
	if w.unusable {
		return 0, ErrWALUnusable
	}
	seq := w.nextSeq
	record := Record{
		Type:            RecordTypeBegin,
		Seq:             seq,
		CommitTimestamp: commitTimestamp,
		Intent:          intent,
	}
	if err := w.appendLocked(record, w.syncOnBegin); err != nil {
		// A buffered writer or file may have accepted part of a frame. Do not
		// issue the same sequence again in this process; recovery must reopen
		// the journal and inspect the durable tail before assigning numbers.
		w.unusable = true
		return 0, err
	}
	w.nextSeq++
	return seq, nil
}

// Commit marks the commit with the given sequence as applied to both stores.
func (w *WAL) Commit(seq uint64) error {
	return w.resolve(seq, RecordTypeCommit)
}

// Abort marks the commit with the given sequence as rolled back.
func (w *WAL) Abort(seq uint64) error {
	return w.resolve(seq, RecordTypeAbort)
}

func (w *WAL) resolve(seq uint64, t RecordType) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWALClosed
	}
	if w.unusable {
		return ErrWALUnusable
	}
	err := w.appendLocked(Record{Type: t, Seq: seq}, false)
	if err != nil {
		w.unusable = true
	}
	return err
}

// AppendCheckpoint records a checkpoint inline in the journal and makes it
// durable. The checkpoint is sealed first, so its merkle root always matches
// the contents that were written.
func (w *WAL) AppendCheckpoint(c Checkpoint) error {
	c.Seal()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWALClosed
	}
	if w.unusable {
		return ErrWALUnusable
	}
	record := Record{
		Type:       RecordTypeCheckpoint,
		Seq:        c.Seq,
		Checkpoint: &c,
	}
	err := w.appendLocked(record, true)
	if err != nil {
		w.unusable = true
	}
	return err
}

// NextSeq returns the sequence the next Begin will use.
func (w *WAL) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextSeq
}

// Replay reads every readable record in sequence order and passes it to fn.
//
// A torn or corrupt tail ends the replay of the segment it appears in without
// failing the call: that shape is exactly what an unclean shutdown produces and
// the records before the tear are still good. Corruption is logged so an
// operator can see it happened. An error from fn stops the replay and is
// returned.
func (w *WAL) Replay(fn func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWALClosed
	}
	// Buffered records that have not reached the file yet would otherwise be
	// invisible to a replay taken while the node is running.
	if err := w.flushLocked(false); err != nil {
		return err
	}
	segments, err := w.segmentSeqs()
	if err != nil {
		return err
	}
	for _, first := range segments {
		records, err := readSegment(w.segmentPath(first), w.logger)
		if err != nil {
			return err
		}
		for _, r := range records {
			if err := fn(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// TruncateThrough removes segments whose records are all at or below seq.
//
// The active segment is never removed, and a segment is kept whenever any of
// its records is above seq, so truncation can only ever discard records a
// durable checkpoint already covers.
func (w *WAL) TruncateThrough(seq uint64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrWALClosed
	}
	if err := w.flushLocked(false); err != nil {
		return 0, err
	}
	segments, err := w.segmentSeqs()
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, first := range segments {
		if first == w.segmentFirstSeq {
			continue
		}
		path := w.segmentPath(first)
		records, complete, err := readSegmentStatus(path, w.logger)
		if err != nil {
			return removed, err
		}
		if !complete {
			// A corrupt or torn segment may contain an unresolved begin
			// before its unreadable tail. Keep it as a recovery artifact.
			break
		}
		highest := uint64(0)
		for _, r := range records {
			if r.Seq > highest {
				highest = r.Seq
			}
		}
		if highest > seq {
			// Segments are ordered by first sequence, so once one
			// reaches past the checkpoint every later one does too.
			break
		}
		if err := os.Remove(path); err != nil &&
			!errors.Is(err, fs.ErrNotExist) {
			return removed, fmt.Errorf(
				"remove journal segment %q: %w",
				path,
				err,
			)
		}
		removed++
	}
	if removed > 0 {
		if err := syncDir(w.dir); err != nil {
			w.logger.Warn(
				"failed to sync journal dir after truncation",
				"error", err,
			)
		}
	}
	return removed, nil
}

// Sync flushes and fsyncs any buffered records.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWALClosed
	}
	return w.flushLocked(true)
}

// Close flushes, syncs and closes the journal. It is safe to call more than
// once.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	var errs []error
	if w.writer != nil && w.file != nil {
		if err := w.writer.Flush(); err != nil {
			errs = append(errs, fmt.Errorf("flush journal: %w", err))
		}
		if err := w.file.Sync(); err != nil {
			errs = append(errs, fmt.Errorf("sync journal: %w", err))
		}
		if err := w.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close journal: %w", err))
		}
	}
	w.file = nil
	w.writer = nil
	return errors.Join(errs...)
}

// readSegment reads every intact record from a segment file.
//
// It returns an error only for problems that are not "the tail is damaged":
// a missing file yields no records, and a torn tail ends the read after the
// records that did survive.
func readSegment(path string, logger *slog.Logger) ([]Record, error) {
	records, _, err := readSegmentStatus(path, logger)
	return records, err
}

func readSegmentStatus(path string, logger *slog.Logger) ([]Record, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("open journal segment %q: %w", path, err)
	}
	defer f.Close() //nolint:errcheck
	reader := bufio.NewReader(f)
	var records []Record
	for {
		record, err := readFrame(reader)
		if errors.Is(err, io.EOF) {
			return records, true, nil
		}
		if err != nil {
			logger.Warn(
				"journal segment ends in an unreadable record",
				"path", path,
				"records_recovered", len(records),
				"error", err,
			)
			return records, false, nil
		}
		records = append(records, record)
	}
}
