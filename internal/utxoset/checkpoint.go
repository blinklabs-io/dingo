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

package utxoset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	checkpointPrefix  = "ckpt-"
	checkpointBackup  = ".bak"
	checkpointSidecar = ".json"
	checkpointTmp     = "tmp-"

	// DefaultCheckpointKeep is how many checkpoints are kept when a Config
	// does not say.
	DefaultCheckpointKeep = 2
)

// ErrNoCheckpointDir reports a checkpoint request on a set that has no
// checkpoint directory.
var ErrNoCheckpointDir = errors.New("utxoset: no checkpoint directory is configured")

// ErrCheckpointNoSpace reports a checkpoint not written because the disk holding
// the checkpoint directory cannot take the copy.
var ErrCheckpointNoSpace = errors.New("utxoset: not enough free disk for a checkpoint")

// Checkpoint is a rolling on-disk copy of the whole set, with every ledger-state
// family it holds, taken at one commit. The ledger state of a family the
// metadata store still holds is not in it: restoring the checkpoint rolls the
// ledger back to its point, and that rollback rewinds the metadata store.
type Checkpoint struct {
	// Slot and Hash name the set's tip in the state the copy was read from;
	// the copy holds exactly that state.
	Slot uint64 `json:"slot"`
	Hash string `json:"hash"`
	// Bytes and SHA256 describe the backup file.
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
	// Path is the backup file.
	Path string `json:"-"`
}

// Point returns the checkpoint's tip as a chain point.
func (c Checkpoint) Point() ocommon.Point {
	hash, _ := hex.DecodeString(c.Hash)
	return ocommon.Point{Slot: c.Slot, Hash: hash}
}

func checkpointName(slot uint64, hash string) string {
	return fmt.Sprintf("%s%020d-%s", checkpointPrefix, slot, hash)
}

// SetCheckpointDir sets the directory WriteCheckpoint writes to and how many
// checkpoints it keeps; keep below one selects DefaultCheckpointKeep.
func (s *Set) SetCheckpointDir(dir string, keep int) {
	if keep < 1 {
		keep = DefaultCheckpointKeep
	}
	s.ckptMu.Lock()
	defer s.ckptMu.Unlock()
	s.ckptDir = dir
	s.ckptKeep = keep
}

// CheckpointBarrier holds off the commits of the metadata store the set is
// kept beside while a checkpoint fixes the version it copies, so the copy is
// taken at a boundary between two ledger commits. It returns the function that
// lifts the hold.
type CheckpointBarrier func(ctx context.Context) (release func(), err error)

// SetCheckpointBarrier installs the barrier WriteCheckpoint holds while it
// fixes the copy's version. Nil removes it.
func (s *Set) SetCheckpointBarrier(b CheckpointBarrier) {
	if b == nil {
		s.ckptBarrier.Store(nil)
		return
	}
	s.ckptBarrier.Store(&b)
}

func (s *Set) barrier() func(context.Context) (func(), error) {
	if b := s.ckptBarrier.Load(); b != nil {
		return *b
	}
	return nil
}

func (s *Set) checkpointDir() (string, int) {
	s.ckptMu.Lock()
	defer s.ckptMu.Unlock()
	return s.ckptDir, s.ckptKeep
}

// WriteCheckpoint writes a checkpoint of the set at its current tip, keeps the
// newest SetCheckpointDir keep of them and returns the one it wrote. A set
// with a failure mark, a pending merge or rollback, or no tip has no state
// worth restoring and is refused.
// Only one checkpoint is written at a time.
func (s *Set) WriteCheckpoint(ctx context.Context) (Checkpoint, error) {
	return s.writeCheckpoint(ctx, false)
}

// writeCheckpoint is WriteCheckpoint; a pinned checkpoint is written without
// pruning, for a caller that is about to restore it and removes the others
// itself.
func (s *Set) writeCheckpoint(ctx context.Context, pinned bool) (Checkpoint, error) {
	dir, keep := s.checkpointDir()
	if dir == "" {
		return Checkpoint{}, ErrNoCheckpointDir
	}
	s.ckptWrite.Lock()
	defer s.ckptWrite.Unlock()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return Checkpoint{}, fmt.Errorf("utxoset: create checkpoint directory: %w", err)
	}
	if err := s.checkpointSpace(dir); err != nil {
		return Checkpoint{}, err
	}
	tmp, err := os.CreateTemp(dir, checkpointTmp+"*"+checkpointBackup)
	if err != nil {
		return Checkpoint{}, fmt.Errorf("utxoset: create checkpoint file: %w", err)
	}
	tmpName := tmp.Name()
	keepTmp := false
	defer func() {
		if !keepTmp {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	h := sha256.New()
	cw := &countingWriter{w: io.MultiWriter(tmp, h)}
	tip, hasTip, err := s.store.BackupCheckpoint(ctx, cw, s.barrier())
	if err != nil {
		return Checkpoint{}, fmt.Errorf("utxoset: write checkpoint: %w", err)
	}
	if !hasTip {
		return Checkpoint{}, errors.New("utxoset: the set has no tip, so no checkpoint is written from it")
	}
	if err := tmp.Sync(); err != nil {
		return Checkpoint{}, fmt.Errorf("utxoset: sync checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return Checkpoint{}, fmt.Errorf("utxoset: close checkpoint: %w", err)
	}
	cp := Checkpoint{
		Slot:   tip.Slot,
		Hash:   hex.EncodeToString(tip.Hash),
		Bytes:  cw.n,
		SHA256: hex.EncodeToString(h.Sum(nil)),
	}
	base := filepath.Join(dir, checkpointName(cp.Slot, cp.Hash))
	cp.Path = base + checkpointBackup
	// The sidecar is written first and the backup renamed last, so a backup
	// under its final name always has its description beside it.
	meta, err := json.Marshal(cp)
	if err != nil {
		return Checkpoint{}, err
	}
	if err := writeFileSync(base+checkpointSidecar, meta); err != nil {
		return Checkpoint{}, fmt.Errorf("utxoset: write checkpoint description: %w", err)
	}
	if err := os.Rename(tmpName, cp.Path); err != nil {
		return Checkpoint{}, fmt.Errorf("utxoset: publish checkpoint: %w", err)
	}
	keepTmp = true
	if err := syncDir(dir); err != nil {
		return Checkpoint{}, fmt.Errorf("utxoset: sync checkpoint directory: %w", err)
	}
	if !pinned {
		if err := s.pruneCheckpoints(dir, keep); err != nil {
			return cp, err
		}
	}
	s.logger.Info(
		"wrote a live UTxO set checkpoint",
		"slot", cp.Slot, "bytes", cp.Bytes, "kept", keep,
	)
	return cp, nil
}

// checkpointSpace refuses a copy the disk cannot hold. The copy is about the
// size of the newest checkpoint, or of the set's data when none exists, and is
// written beside the kept ones before the oldest is removed, so the room is
// needed in addition to them.
func (s *Set) checkpointSpace(dir string) error {
	need := s.store.DataBytes()
	if cps, err := listCheckpoints(dir); err == nil && len(cps) > 0 {
		need = max(need, cps[0].Bytes)
	}
	need += need / 10
	avail := s.availableDisk
	if avail == nil {
		avail = AvailableDiskBytes
	}
	free, err := avail(dir)
	if err != nil {
		// An unreadable free-space figure is not a reason to write no
		// checkpoint.
		return nil //nolint:nilerr
	}
	if need < 0 || free > uint64(need) {
		return nil
	}
	s.m.checkpointsSkipped.Inc()
	s.logger.Warn(
		"skipping a live UTxO set checkpoint: the disk lacks the space for it",
		"free_bytes", free, "needed_bytes", need, "dir", dir,
	)
	return fmt.Errorf("%w: %d bytes free, about %d needed", ErrCheckpointNoSpace, free, need)
}

// Checkpoints lists the checkpoints on disk, newest first.
func (s *Set) Checkpoints() ([]Checkpoint, error) {
	dir, _ := s.checkpointDir()
	if dir == "" {
		return nil, nil
	}
	return listCheckpoints(dir)
}

// CheckpointBytes returns the disk the checkpoints and any unfinished
// checkpoint file or restore check occupy.
func (s *Set) CheckpointBytes() (int64, error) {
	dir, _ := s.checkpointDir()
	if dir == "" {
		return 0, nil
	}
	var total int64
	err := filepath.WalkDir(dir, func(_ string, e os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if e.IsDir() {
			return nil
		}
		info, err := e.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		total += info.Size()
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	return total, err
}

func listCheckpoints(dir string) ([]Checkpoint, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("utxoset: read checkpoint directory: %w", err)
	}
	var out []Checkpoint
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, checkpointPrefix) ||
			!strings.HasSuffix(name, checkpointBackup) {
			continue
		}
		base := strings.TrimSuffix(name, checkpointBackup)
		raw, err := os.ReadFile(filepath.Join(dir, base+checkpointSidecar)) // #nosec G304 -- inside the checkpoint directory
		if err != nil {
			continue
		}
		var cp Checkpoint
		if err := json.Unmarshal(raw, &cp); err != nil {
			continue
		}
		if checkpointName(cp.Slot, cp.Hash) != base {
			continue
		}
		cp.Path = filepath.Join(dir, name)
		out = append(out, cp)
	}
	slices.SortFunc(out, func(a, b Checkpoint) int {
		switch {
		case a.Slot > b.Slot:
			return -1
		case a.Slot < b.Slot:
			return 1
		}
		return strings.Compare(b.Hash, a.Hash)
	})
	return out, nil
}

func (s *Set) pruneCheckpoints(dir string, keep int) error {
	cps, err := listCheckpoints(dir)
	if err != nil {
		return err
	}
	for i := keep; i < len(cps); i++ {
		base := strings.TrimSuffix(cps[i].Path, checkpointBackup)
		for _, p := range []string{cps[i].Path, base + checkpointSidecar} {
			if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("utxoset: remove an old checkpoint: %w", err)
			}
		}
	}
	return removeTmpEntries(dir)
}

// removeTmpEntries removes what a backup or a restore check killed mid-way
// leaves: a temporary file, or a scratch store directory.
func removeTmpEntries(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("utxoset: read checkpoint directory: %w", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), checkpointTmp) {
			if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
				return fmt.Errorf("utxoset: remove an unfinished checkpoint file: %w", err)
			}
		}
	}
	return nil
}

// dropCheckpointsExcept removes every checkpoint but keep: they describe a
// state the set no longer holds.
func (s *Set) dropCheckpointsExcept(keep Checkpoint) error {
	dir, _ := s.checkpointDir()
	cps, err := listCheckpoints(dir)
	if err != nil {
		return err
	}
	for _, cp := range cps {
		if cp.Path == keep.Path {
			continue
		}
		base := strings.TrimSuffix(cp.Path, checkpointBackup)
		for _, p := range []string{cp.Path, base + checkpointSidecar} {
			if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("utxoset: remove a replaced checkpoint: %w", err)
			}
		}
	}
	return nil
}

const (
	restoreMarkerName = "restore-pending.json"
	restoreScratch    = checkpointTmp + "restore-"
)

// restoreMarker names the checkpoint a restore is replacing the set with. It is
// written, outside the store, before anything of the set is destroyed and
// removed once the restored set is checked, so a start that finds it knows the
// set may hold part of a restore.
type restoreMarker struct {
	Slot uint64 `json:"slot"`
	Hash string `json:"hash"`
}

func (s *Set) restoreMarkerPath() string {
	dir, _ := s.checkpointDir()
	return filepath.Join(dir, restoreMarkerName)
}

// verifyCheckpointFile returns cp's open backup after checking its digest.
func verifyCheckpointFile(cp Checkpoint) (*os.File, error) {
	f, err := os.Open(cp.Path) // #nosec G304 -- listed from the checkpoint directory
	if err != nil {
		return nil, fmt.Errorf("open checkpoint: %w", err)
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	if n != cp.Bytes || hex.EncodeToString(h.Sum(nil)) != cp.SHA256 {
		_ = f.Close()
		return nil, fmt.Errorf(
			"checkpoint at slot %d does not match its recorded digest", cp.Slot,
		)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// validateCheckpoint loads cp into a scratch store beside the checkpoints and
// returns the tip it holds, leaving the set untouched: it checks the digest,
// that the backup loads, that the layout is one this release reads and that
// the copy has a tip.
func (s *Set) validateCheckpoint(
	ctx context.Context,
	cp Checkpoint,
) (ocommon.Point, error) {
	f, err := verifyCheckpointFile(cp)
	if err != nil {
		return ocommon.Point{}, err
	}
	defer f.Close()
	dir, _ := s.checkpointDir()
	scratch := filepath.Join(dir, restoreScratch+checkpointName(cp.Slot, cp.Hash))
	_ = os.RemoveAll(scratch)
	defer func() { _ = os.RemoveAll(scratch) }()
	st, err := badgerutxo.Open(scratch, badgerutxo.WithMemTableSize(DefaultMemTableSize))
	if err != nil {
		return ocommon.Point{}, fmt.Errorf("open a scratch store: %w", err)
	}
	defer st.Close()
	if err := st.Restore(ctx, f); err != nil {
		return ocommon.Point{}, fmt.Errorf("load checkpoint: %w", err)
	}
	stale, err := st.Stale()
	if err != nil {
		return ocommon.Point{}, err
	}
	tip, ok, err := st.Tip()
	if err != nil {
		return ocommon.Point{}, err
	}
	if stale || !ok {
		return ocommon.Point{}, fmt.Errorf(
			"checkpoint at slot %d is in a layout this release does not read", cp.Slot,
		)
	}
	if err := st.Checkpointable(); err != nil {
		return ocommon.Point{}, fmt.Errorf("checkpoint at slot %d: %w", cp.Slot, err)
	}
	return tip, nil
}

// restoreCheckpoint replaces the set with cp, which validateCheckpoint has
// accepted. The marker is written
// before the set is cleared and removed after the restored set is checked, so
// a crash between leaves a marker that finishPendingRestore redoes the restore
// from. An error after the set is cleared leaves the marker in place.
func (s *Set) restoreCheckpoint(
	ctx context.Context,
	cp Checkpoint,
) error {
	f, err := verifyCheckpointFile(cp)
	if err != nil {
		return err
	}
	defer f.Close()
	meta, err := json.Marshal(restoreMarker{Slot: cp.Slot, Hash: cp.Hash})
	if err != nil {
		return err
	}
	markerPath := s.restoreMarkerPath()
	if err := writeFileSync(markerPath, meta); err != nil {
		return fmt.Errorf("write the restore marker: %w", err)
	}
	dir, _ := s.checkpointDir()
	if err := syncDir(dir); err != nil {
		return err
	}
	if err := s.store.Reset(); err != nil {
		return fmt.Errorf("clear the set: %w", err)
	}
	if err := s.store.Restore(ctx, f); err != nil {
		return fmt.Errorf("load checkpoint: %w", err)
	}
	stale, err := s.store.Stale()
	if err != nil {
		return err
	}
	tip, ok, err := s.store.Tip()
	if err != nil {
		return err
	}
	if stale || !ok || !samePoint(tip, cp.Point()) {
		return fmt.Errorf(
			"the restored set does not hold checkpoint %d's state", cp.Slot,
		)
	}
	if err := os.Remove(markerPath); err != nil {
		return fmt.Errorf("clear the restore marker: %w", err)
	}
	return syncDir(dir)
}

// finishPendingRestore redoes a restore a previous run began and did not
// finish, so the set is never used while it may hold part of one.
func (s *Set) finishPendingRestore(ctx context.Context) error {
	dir, _ := s.checkpointDir()
	if dir == "" {
		return nil
	}
	raw, err := os.ReadFile(s.restoreMarkerPath()) // #nosec G304 -- inside the checkpoint directory
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("utxoset: read the restore marker: %w", err)
	}
	var m restoreMarker
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("utxoset: the restore marker is unreadable: %w", err)
	}
	cps, err := listCheckpoints(dir)
	if err != nil {
		return err
	}
	for _, cp := range cps {
		if cp.Slot != m.Slot || cp.Hash != m.Hash {
			continue
		}
		s.logger.Warn(
			"finishing a live UTxO set restore a previous run began",
			"checkpoint_slot", cp.Slot,
		)
		if err := s.restoreCheckpoint(ctx, cp); err != nil {
			return fmt.Errorf(
				"utxoset: finish the interrupted restore of the checkpoint at slot %d: %w",
				cp.Slot, err,
			)
		}
		return nil
	}
	return fmt.Errorf(
		"utxoset: a restore of the checkpoint at slot %d was interrupted, the live set may hold part of it, and that checkpoint is no longer in %s; put its .bak and .json files back there",
		m.Slot, dir,
	)
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) // #nosec G304 -- inside the checkpoint directory
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- the checkpoint directory
	if err != nil {
		return err
	}
	defer d.Close()
	// Directories cannot be synced on every platform; a failure here only
	// narrows the crash window and is not an error.
	_ = d.Sync()
	return nil
}
