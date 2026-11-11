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

package nodeparity

import (
	"bufio"
	"context"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// FromGenesisOption configures optional RunFromGenesis behaviour.
type FromGenesisOption func(*fromGenesisOptions)

type fromGenesisOptions struct {
	captureDir string
	// follower replaces runBoundaryFollower; only tests set it, since the
	// fake Dingo cannot serve a second chainsync session.
	follower func(context.Context, *boundaryCaptures)
	// afterCapturedUTxORead runs once a capture's UTxO set is read, or found
	// unreadable; only tests set it, to end the session before the boundary
	// is reported.
	afterCapturedUTxORead func(context.Context)
}

// WithBoundaryCaptureDir makes RunFromGenesis read Dingo's side of every
// epoch-boundary check as Dingo itself crosses the boundary, rather than when
// the UTxO reconstruction gets there, and keep each boundary's whole UTxO set
// in dir until the reconstruction compares against it.
//
// Dingo answers only for points within its rollback window of its tip, and
// the reconstruction moves at the speed Koios answers /tx_info, which on a
// busy chain is far slower than Dingo syncs. Without a capture every check at
// a boundary the reconstruction reaches late is refused as too old.
//
// Each capture is kept in dir as two files: its whole UTxO set, and a record
// of everything else read at the boundary. Both are removed once compared. A
// later RunFromGenesis on the same dir picks up every capture it left behind
// whose boundary is after the start point, so a restart loses no epoch whose
// boundary Dingo has since moved past, and removes the rest. A record that
// cannot be read reports every check of its epoch as not run, with the reason.
// Only one RunFromGenesis may use a dir at a time.
func WithBoundaryCaptureDir(dir string) FromGenesisOption {
	return func(o *fromGenesisOptions) { o.captureDir = dir }
}

// boundaryCapture is Dingo's side of one epoch boundary's checks, read at
// point while Dingo could still answer for it. The protocol-parameter and
// stake results are complete checks, since their Koios side does not depend
// on the reconstruction.
type boundaryCapture struct {
	epoch           uint64
	point           pcommon.Point
	ppMismatches    []koiosparity.CheckMismatch
	ppErr           error
	stakeMismatches []StakeMismatch
	stakeErr        error
	psElapsed       time.Duration
	// utxoPath is the file holding Dingo's whole UTxO set at point; empty
	// when utxoErr says why it could not be read.
	utxoPath string
	utxoErr  error
	// recordPath is the file holding everything else above, written by put.
	recordPath string
}

// removeFiles removes the capture's files, the record first: a UTxO file
// left without its record is swept on the next load, while a record left
// without its UTxO file would fail an epoch's UTxO check.
func (c *boundaryCapture) removeFiles() {
	if c.recordPath != "" {
		_ = os.Remove(c.recordPath)
	}
	if c.utxoPath != "" {
		_ = os.Remove(c.utxoPath)
	}
}

// boundaryCaptures hands captures from the follower to the main loop.
type boundaryCaptures struct {
	mu      sync.Mutex
	dir     string
	byEpoch map[uint64]*boundaryCapture
	// capturing holds each boundary the follower has started capturing and
	// not finished. A capture's checks can wait on Koios for an hour after
	// its UTxO set is read, and the main loop can reach the boundary
	// meanwhile; Dingo no longer answers for it then, so the main loop must
	// wait for the capture rather than query Dingo.
	capturing map[uint64]*capturingBoundary
}

type capturingBoundary struct {
	point pcommon.Point
	done  chan struct{}
}

func newBoundaryCaptures(dir string) *boundaryCaptures {
	return &boundaryCaptures{
		dir:       dir,
		byEpoch:   map[uint64]*boundaryCapture{},
		capturing: map[uint64]*capturingBoundary{},
	}
}

// begin records that the follower has started capturing epoch at point.
func (b *boundaryCaptures) begin(epoch uint64, point pcommon.Point) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if old := b.capturing[epoch]; old != nil {
		close(old.done)
	}
	b.capturing[epoch] = &capturingBoundary{
		point: point, done: make(chan struct{}),
	}
}

// finish ends the capture begin started for epoch at point, holding c as
// put does unless c is nil, for a capture abandoned unfinished. The error
// is put's.
func (b *boundaryCaptures) finish(
	epoch uint64, point pcommon.Point, c *boundaryCapture,
) error {
	var err error
	if c != nil {
		err = b.put(c)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur := b.capturing[epoch]; cur != nil && pointsEqual(cur.point, point) {
		close(cur.done)
		delete(b.capturing, epoch)
	}
	return err
}

// await is take, but first waits for a capture of epoch at point the
// follower is still making. It returns ctx's error if ctx ends first.
func (b *boundaryCaptures) await(
	ctx context.Context,
	epoch uint64,
	point pcommon.Point,
	logf func(format string, args ...any),
) (*boundaryCapture, error) {
	logged := false
	for {
		b.mu.Lock()
		cur := b.capturing[epoch]
		b.mu.Unlock()
		if cur == nil || !pointsEqual(cur.point, point) {
			return b.take(epoch, point), nil
		}
		if !logged {
			logged = true
			logf(
				"nodeparity: waiting for Dingo's epoch %d boundary capture "+
					"at slot %d to finish",
				epoch, point.Slot,
			)
		}
		select {
		case <-cur.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// put holds c as its epoch's capture, replacing any other, and writes c's
// record to the capture directory unless it already has one. An error means
// the record was not written: c is still held, but a restart loses it.
func (b *boundaryCaptures) put(c *boundaryCapture) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var err error
	if c.recordPath == "" {
		c.recordPath, err = writeCaptureRecord(b.dir, c)
	}
	// After the write: a capture of the same point shares its record name.
	if old := b.byEpoch[c.epoch]; old != nil && old != c {
		if old.recordPath != "" && old.recordPath != c.recordPath {
			_ = os.Remove(old.recordPath)
		}
		if old.utxoPath != "" && old.utxoPath != c.utxoPath {
			_ = os.Remove(old.utxoPath)
		}
	}
	b.byEpoch[c.epoch] = c
	return err
}

// handBack re-holds c, which take returned, for a session that ended before
// comparing it. If the follower has meanwhile captured c's epoch at another
// point, that capture is the one on Dingo's chain: it is kept, and c and its
// files are dropped instead.
func (b *boundaryCaptures) handBack(c *boundaryCapture) error {
	b.mu.Lock()
	cur := b.byEpoch[c.epoch]
	if cur != nil && cur != c && !pointsEqual(cur.point, c.point) {
		b.mu.Unlock()
		c.removeFiles()
		return nil
	}
	b.mu.Unlock()
	return b.put(c)
}

// holds reports whether the capture for epoch was read at exactly point.
func (b *boundaryCaptures) holds(epoch uint64, point pcommon.Point) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.byEpoch[epoch]
	return c != nil && pointsEqual(c.point, point)
}

// take returns the capture for epoch, only if it was read at exactly point:
// a capture from a block that a rollback later replaced answers for a
// different chain. It also discards every capture for an earlier epoch, which
// the main loop has passed and will never ask for.
func (b *boundaryCaptures) take(
	epoch uint64, point pcommon.Point,
) *boundaryCapture {
	b.mu.Lock()
	defer b.mu.Unlock()
	for e, c := range b.byEpoch {
		if e < epoch {
			c.removeFiles()
			delete(b.byEpoch, e)
		}
	}
	c := b.byEpoch[epoch]
	if c == nil || !pointsEqual(c.point, point) {
		return nil
	}
	delete(b.byEpoch, epoch)
	return c
}

// epochAt returns the epoch of the capture read, or being read, at exactly
// point, if there is one. A nil receiver holds none.
func (b *boundaryCaptures) epochAt(point pcommon.Point) (uint64, bool) {
	if b == nil {
		return 0, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.byEpoch {
		if pointsEqual(c.point, point) {
			return c.epoch, true
		}
	}
	for epoch, c := range b.capturing {
		if pointsEqual(c.point, point) {
			return epoch, true
		}
	}
	return 0, false
}

// close drops every capture held, leaving its files for the next run.
func (b *boundaryCaptures) close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.byEpoch)
}

// captureRecordVersion is boundaryCaptureRecord's format; load treats a
// record of any other version as unreadable.
const captureRecordVersion = 1

// boundaryCaptureRecord is a boundaryCapture as written to disk. Each error
// is kept as its message, nil when absent: an empty string would be
// indistinguishable from no error, and reading a failed check back as a
// passed one is a false match.
type boundaryCaptureRecord struct {
	Version         int                         `json:"version"`
	Epoch           uint64                      `json:"epoch"`
	Slot            uint64                      `json:"slot"`
	Hash            string                      `json:"hash"`
	CapturedAt      time.Time                   `json:"captured_at"`
	PPMismatches    []koiosparity.CheckMismatch `json:"pp_mismatches"`
	PPErr           *string                     `json:"pp_err"`
	StakeMismatches []stakeMismatchRecord       `json:"stake_mismatches"`
	StakeErr        *string                     `json:"stake_err"`
	PSElapsed       time.Duration               `json:"ps_elapsed"`
	// UTxOFile is the UTxO file's name within the capture directory.
	UTxOFile string  `json:"utxo_file"`
	UTxOErr  *string `json:"utxo_err"`
}

// stakeMismatchRecord is a StakeMismatch as written to disk.
type stakeMismatchRecord struct {
	PoolIDBech32 string `json:"pool_id_bech32"`
	DingoStake   uint64 `json:"dingo_stake"`
	KoiosStake   string `json:"koios_stake"`
	DiffLovelace int64  `json:"diff_lovelace"`
	Reason       string `json:"reason"`
	KoiosFault   bool   `json:"koios_fault"`
}

var (
	captureRecordName = regexp.MustCompile(
		`^capture-epoch-([0-9]+)-slot-([0-9]+)-([0-9a-f]*)\.json$`,
	)
	captureUTxOName = regexp.MustCompile(`^utxo-epoch-.*\.gob$`)
)

// partialSuffix marks a file still being written; load removes any left by
// a process that died mid-write.
const partialSuffix = ".partial"

// maxUTxOSetSizeHint caps the map size a UTxO set file's own count asks
// for, above mainnet's set, so a damaged count cannot allocate gigabytes up
// front; a larger set still loads, growing the map as it goes.
const maxUTxOSetSizeHint = 1 << 24

func captureRecordFileName(epoch uint64, point pcommon.Point) string {
	return fmt.Sprintf(
		"capture-epoch-%d-slot-%d-%x.json", epoch, point.Slot, point.Hash,
	)
}

func captureUTxOFileName(epoch uint64, point pcommon.Point) string {
	return fmt.Sprintf(
		"utxo-epoch-%d-slot-%d-%x.gob", epoch, point.Slot, point.Hash,
	)
}

func errMessage(err error) *string {
	if err == nil {
		return nil
	}
	msg := err.Error()
	return &msg
}

func messageErr(msg *string) error {
	if msg == nil {
		return nil
	}
	return errors.New(*msg)
}

func writeCaptureRecord(dir string, c *boundaryCapture) (string, error) {
	if c.utxoPath != "" && filepath.Dir(c.utxoPath) != filepath.Clean(dir) {
		return "", fmt.Errorf(
			"capture UTxO file %s is outside the capture directory", c.utxoPath,
		)
	}
	rec := boundaryCaptureRecord{
		Version:      captureRecordVersion,
		Epoch:        c.epoch,
		Slot:         c.point.Slot,
		Hash:         hex.EncodeToString(c.point.Hash),
		CapturedAt:   time.Now().UTC(),
		PPMismatches: c.ppMismatches,
		PPErr:        errMessage(c.ppErr),
		StakeErr:     errMessage(c.stakeErr),
		PSElapsed:    c.psElapsed,
		UTxOErr:      errMessage(c.utxoErr),
	}
	for _, m := range c.stakeMismatches {
		rec.StakeMismatches = append(
			rec.StakeMismatches, stakeMismatchRecord(m),
		)
	}
	if c.utxoPath != "" {
		rec.UTxOFile = filepath.Base(c.utxoPath)
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, captureRecordFileName(c.epoch, c.point))
	if err := writeFileAtomic(path, func(f *os.File) error {
		_, err := f.Write(data)
		return err
	}); err != nil {
		return "", fmt.Errorf("store epoch %d boundary capture record: %w", c.epoch, err)
	}
	return path, nil
}

// readCaptureRecord reads the record at path, named for epoch and point, and
// checks it describes a complete capture of that boundary.
func readCaptureRecord(
	dir, path string, epoch uint64, point pcommon.Point,
) (*boundaryCapture, time.Time, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, err
	}
	var rec boundaryCaptureRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, time.Time{}, err
	}
	switch {
	case rec.Version != captureRecordVersion:
		return nil, time.Time{}, fmt.Errorf("record version %d, want %d",
			rec.Version, captureRecordVersion)
	case rec.Epoch != epoch || rec.Slot != point.Slot ||
		rec.Hash != hex.EncodeToString(point.Hash):
		return nil, time.Time{}, errors.New("record does not match its file name")
	case (rec.UTxOFile == "") == (rec.UTxOErr == nil):
		return nil, time.Time{}, errors.New(
			"record names neither a UTxO file nor why it has none",
		)
	case rec.UTxOFile != "" && rec.UTxOFile != captureUTxOFileName(epoch, point):
		// Any other name, even inside the directory, would compare this
		// boundary against another boundary's set.
		return nil, time.Time{}, fmt.Errorf(
			"record names UTxO file %q, not this boundary's", rec.UTxOFile,
		)
	}
	c := &boundaryCapture{
		epoch:        epoch,
		point:        point,
		ppMismatches: rec.PPMismatches,
		ppErr:        messageErr(rec.PPErr),
		stakeErr:     messageErr(rec.StakeErr),
		psElapsed:    rec.PSElapsed,
		utxoErr:      messageErr(rec.UTxOErr),
		recordPath:   path,
	}
	for _, m := range rec.StakeMismatches {
		c.stakeMismatches = append(c.stakeMismatches, StakeMismatch(m))
	}
	if rec.UTxOFile != "" {
		c.utxoPath = filepath.Join(dir, rec.UTxOFile)
	}
	return c, rec.CapturedAt, nil
}

// load holds every capture an earlier run left in the capture directory whose
// boundary is after start, the point this run's chainsync resumes after, and
// removes the rest: chainsync never delivers a boundary at or before start.
// It also removes files a run died writing and UTxO files no record names.
//
// An unreadable record still holds its epoch, as a capture whose every check
// failed with the reason: dropping it would let the epoch be checked live,
// which Dingo refuses as too old, or, for a record cut short, risk reading a
// failed check back as a passed one. Of two records for one epoch, from a
// boundary block a rollback replaced, the later capture is kept; it is the
// one read from the chain Dingo had moved to, which the reconstruction
// follows.
func (b *boundaryCaptures) load(
	start pcommon.Point,
) (loaded, removed int, err error) {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return 0, 0, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	afterStart := func(slot uint64) bool {
		return (start.Slot == 0 && len(start.Hash) == 0) || slot > start.Slot
	}
	capturedAt := map[uint64]time.Time{}
	var utxoFiles []string
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(b.dir, name)
		if strings.HasSuffix(name, partialSuffix) {
			_ = os.Remove(path)
			removed++
			continue
		}
		if captureUTxOName.MatchString(name) {
			utxoFiles = append(utxoFiles, path)
			continue
		}
		m := captureRecordName.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		epoch, err1 := strconv.ParseUint(m[1], 10, 64)
		slot, err2 := strconv.ParseUint(m[2], 10, 64)
		hash, err3 := hex.DecodeString(m[3])
		if err := errors.Join(err1, err2, err3); err != nil {
			// No boundary has this name, so the record can never be
			// compared; its UTxO file, if any, goes as unreferenced.
			_ = os.Remove(path)
			removed++
			continue
		}
		point := pcommon.NewPoint(slot, hash)
		c, at, err := readCaptureRecord(b.dir, path, epoch, point)
		if err != nil {
			reason := fmt.Errorf(
				"boundary capture record %s is unreadable, so this check "+
					"did not run: %w", name, err,
			)
			c = &boundaryCapture{
				epoch: epoch, point: point,
				ppErr: reason, stakeErr: reason, utxoErr: reason,
				recordPath: path,
			}
			if utxo := filepath.Join(
				b.dir, captureUTxOFileName(epoch, point),
			); fileExists(utxo) {
				c.utxoPath = utxo
			}
		}
		if !afterStart(slot) {
			c.removeFiles()
			removed++
			continue
		}
		if old := b.byEpoch[epoch]; old != nil {
			removed++
			if !at.After(capturedAt[epoch]) {
				c.removeFiles()
				continue
			}
			old.removeFiles()
			loaded--
		}
		b.byEpoch[epoch] = c
		capturedAt[epoch] = at
		loaded++
	}
	named := map[string]bool{}
	for _, c := range b.byEpoch {
		named[c.utxoPath] = true
	}
	for _, path := range utxoFiles {
		if !named[path] && os.Remove(path) == nil {
			removed++
		}
	}
	return loaded, removed, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// writeFileAtomic writes path through write, so a reader sees either no file
// or the whole of it, durably on disk, even when the process dies mid-write.
func writeFileAtomic(path string, write func(*os.File) error) (err error) {
	tmp := path + partialSuffix
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			_ = f.Close()
		}
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if err := write(f); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	err = f.Close()
	f = nil
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// The rename is durable only once the directory entry is. Windows cannot
	// flush a directory handle; NTFS journals the rename itself.
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

func writeUTxOSetFile(path string, set UTxOSet) error {
	return writeFileAtomic(path, func(f *os.File) error {
		return encodeUTxOSet(f, set)
	})
}

func encodeUTxOSet(f *os.File, set UTxOSet) error {
	// One gob message per entry: encoding the map as a single value would
	// buffer the whole encoding, a second copy of a mainnet-sized set.
	w := bufio.NewWriterSize(f, 1<<20)
	enc := gob.NewEncoder(w)
	if err := enc.Encode(len(set)); err != nil {
		return err
	}
	for ref, entry := range set {
		if err := enc.Encode(utxoSetFileEntry{Ref: ref, Entry: entry}); err != nil {
			return err
		}
	}
	return w.Flush()
}

type utxoSetFileEntry struct {
	Ref   string
	Entry string
}

func readUTxOSetFile(path string) (UTxOSet, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	dec := gob.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	var n int
	if err := dec.Decode(&n); err != nil {
		return nil, err
	}
	// Every entry takes at least a byte, so a count beyond the file size
	// means a damaged file, not a set to allocate for.
	if n < 0 || int64(n) > info.Size() {
		return nil, fmt.Errorf(
			"UTxO set file %s claims %d entries in %d bytes",
			filepath.Base(path), n, info.Size(),
		)
	}
	set := make(UTxOSet, min(n, maxUTxOSetSizeHint))
	for range n {
		var e utxoSetFileEntry
		if err := dec.Decode(&e); err != nil {
			return nil, err
		}
		set[e.Ref] = e.Entry
	}
	return set, nil
}

// captureBoundary reads Dingo's side of epoch's checks at point. The UTxO set
// is read first: the protocol-parameter and stake checks wait on Koios, and
// on mainnet that wait has outlasted Dingo's rollback window, so a UTxO read
// after them was refused as too old.
func captureBoundary(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
	epoch uint64,
	koios *koiosparity.KoiosClient,
	cache *koiosparity.Cache,
	network string,
	dir string,
) *boundaryCapture {
	return captureBoundaryInOrder(
		epoch, point,
		func() (string, error) {
			return captureUTxOSetFile(ctx, dingoAddr, magic, point, epoch, dir)
		},
		func(c *boundaryCapture) {
			c.ppMismatches, c.ppErr, c.stakeMismatches, c.stakeErr = runProtocolParamsAndStake(
				ctx, dingoAddr, magic, point, koios, cache, network, epoch,
			)
		},
	)
}

// captureBoundaryInOrder runs readUTxO to completion before checks, which
// fills in the protocol-parameter and stake results.
func captureBoundaryInOrder(
	epoch uint64,
	point pcommon.Point,
	readUTxO func() (string, error),
	checks func(*boundaryCapture),
) *boundaryCapture {
	c := &boundaryCapture{epoch: epoch, point: point}
	c.utxoPath, c.utxoErr = readUTxO()
	psStart := time.Now()
	checks(c)
	c.psElapsed = time.Since(psStart)
	return c
}

func captureUTxOSetFile(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	point pcommon.Point,
	epoch uint64,
	dir string,
) (string, error) {
	set, err := captureGenesisBaseline(ctx, dingoAddr, magic, point)
	if err != nil {
		return "", fmt.Errorf("read Dingo's UTxO set at slot %d: %w", point.Slot, err)
	}
	path := filepath.Join(dir, captureUTxOFileName(epoch, point))
	if err := writeUTxOSetFile(path, set); err != nil {
		return "", fmt.Errorf("store Dingo's UTxO set at slot %d: %w", point.Slot, err)
	}
	return path, nil
}

// boundaryFollowerRetryDelay is the wait before the boundary follower
// reconnects after a session ends. It is a variable only so tests can
// shorten it; no production code changes it.
var boundaryFollowerRetryDelay = 5 * time.Second

// runBoundaryFollower follows Dingo's chain from start on its own chainsync
// connection, doing no per-block work, and at the first block of each epoch
// calls capture and hands the result to captures. It runs until ctx ends,
// reconnecting from the last block it saw.
func runBoundaryFollower(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	start pcommon.Point,
	epochOf func(context.Context, uint64) (uint64, error),
	capture func(context.Context, pcommon.Point, uint64) *boundaryCapture,
	captures *boundaryCaptures,
	logf func(format string, args ...any),
) {
	var (
		mu        sync.Mutex
		lastPoint = start
		prevEpoch uint64
		haveEpoch bool
	)
	if start.Slot != 0 || len(start.Hash) != 0 {
		if e, err := epochOf(ctx, start.Slot); err == nil {
			prevEpoch, haveEpoch = e, true
		}
	}
	for ctx.Err() == nil {
		sessionCtx, endSession := context.WithCancel(ctx)
		err := followBoundariesOnce(sessionCtx, dingoAddr, magic, &mu, &lastPoint,
			func(cbCtx context.Context, block lcommon.Block) error {
				if block.Era().Id == byron.EraIdByron {
					return nil
				}
				// An error ends the session before the block counts as
				// seen, so the next session delivers it again: skipping it
				// would capture the epoch's second block instead, which the
				// main loop never asks for.
				epoch, err := epochOf(cbCtx, block.SlotNumber())
				if err != nil {
					return fmt.Errorf("epoch of slot %d: %w", block.SlotNumber(), err)
				}
				if haveEpoch && epoch <= prevEpoch {
					return nil
				}
				point := pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
				// A capture an earlier run left for this very block answers
				// for it already, and Dingo may no longer.
				if captures.holds(epoch, point) {
					prevEpoch, haveEpoch = epoch, true
					return nil
				}
				captures.begin(epoch, point)
				c := capture(cbCtx, point, epoch)
				if err := cbCtx.Err(); err != nil {
					if c.utxoPath != "" {
						_ = os.Remove(c.utxoPath)
					}
					_ = captures.finish(epoch, point, nil)
					return err
				}
				if err := captures.finish(epoch, point, c); err != nil {
					logf(
						"nodeparity: epoch %d boundary capture is not stored, "+
							"so a restart before it is compared loses it: %v",
						epoch, err,
					)
				}
				prevEpoch, haveEpoch = epoch, true
				if c.utxoErr != nil {
					logf(
						"nodeparity: captured Dingo's epoch %d boundary checks at "+
							"slot %d, but not its UTxO set: %v",
						epoch, point.Slot, c.utxoErr,
					)
				} else {
					logf(
						"nodeparity: captured Dingo's epoch %d boundary at slot %d "+
							"for the UTxO comparison",
						epoch, point.Slot,
					)
				}
				return nil
			},
			func(cbCtx context.Context, point pcommon.Point) {
				if point.Slot == 0 && len(point.Hash) == 0 {
					haveEpoch = false
					return
				}
				if e, err := epochOf(cbCtx, point.Slot); err == nil &&
					(!haveEpoch || e < prevEpoch) {
					prevEpoch, haveEpoch = e, true
				}
			},
		)
		endSession()
		if ctx.Err() != nil {
			return
		}
		logf("nodeparity: boundary follower session ended, reconnecting: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(boundaryFollowerRetryDelay):
		}
	}
}

// followBoundariesOnce runs one chainsync session from *lastPoint until it
// ends, calling onBlock and onRollback under mu. A block becomes *lastPoint
// only once onBlock succeeds; an onBlock error ends the session.
func followBoundariesOnce(
	ctx context.Context,
	dingoAddr string,
	magic uint32,
	mu *sync.Mutex,
	lastPoint *pcommon.Point,
	onBlock func(context.Context, lcommon.Block) error,
	onRollback func(context.Context, pcommon.Point),
) error {
	rawConn, err := dialRaw(ctx, protoFromAddr(dingoAddr), dingoAddr)
	if err != nil {
		return err
	}
	conn, err := ouroboros.New(
		ouroboros.WithConnection(rawConn),
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
		ouroboros.WithMuxerSegmentReadTimeout(0),
		ouroboros.WithChainSyncConfig(chainsync.NewConfig(
			chainsync.WithPipelineLimit(1),
			chainsync.WithRollForwardFunc(func(
				_ chainsync.CallbackContext, _ uint, blockData any, _ chainsync.Tip,
			) error {
				block, ok := blockData.(lcommon.Block)
				if !ok {
					return fmt.Errorf("unexpected roll-forward payload type %T", blockData)
				}
				mu.Lock()
				defer mu.Unlock()
				if err := onBlock(ctx, block); err != nil {
					return err
				}
				*lastPoint = pcommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
				return nil
			}),
			chainsync.WithRollBackwardFunc(func(
				_ chainsync.CallbackContext, point pcommon.Point, _ chainsync.Tip,
			) error {
				mu.Lock()
				defer mu.Unlock()
				*lastPoint = point
				onRollback(ctx, point)
				return nil
			}),
		)),
	)
	if err != nil {
		rawConn.Close() //nolint:errcheck
		return err
	}
	defer conn.Close()                                      //nolint:errcheck
	stop := context.AfterFunc(ctx, func() { conn.Close() }) //nolint:errcheck
	defer stop()
	cs := conn.ChainSync()
	if cs == nil || cs.Client == nil {
		return errors.New("ChainSync client unavailable")
	}
	mu.Lock()
	from := *lastPoint
	mu.Unlock()
	if err := cs.Client.Sync([]pcommon.Point{from}); err != nil {
		return err
	}
	go keepNtCConnAlive(ctx, conn, ntcKeepaliveInterval)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-conn.ErrorChan():
		if !ok {
			return errDingoClosedConnection
		}
		return err
	}
}
