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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ReimportTarget is where a re-import loads ledger state. Set is a copy of the
// live set that nothing else reads, so a re-import that fails at any point
// leaves the live set as it was. Commit replaces the live set with the copy
// once the import is complete.
type ReimportTarget struct {
	Set *Set
	// Commit checks that the copy holds the state at point, then swaps it in
	// behind the restore marker, which a start that finds it finishes.
	Commit func(ctx context.Context, point ocommon.Point) error
}

// ScratchReimportFunc loads ledger state from a Mithril snapshot at or below
// localTip into target, and returns the point it loaded.
type ScratchReimportFunc func(
	ctx context.Context,
	localTip ocommon.Point,
	target ReimportTarget,
) (ocommon.Point, error)

// SetScratchReimporter installs the re-import recoverUnreachable falls back to
// in place of the one SetReimporter installs, for one that loads into a copy
// of the set. Nil removes it.
func (s *Set) SetScratchReimporter(fn ScratchReimportFunc) {
	if fn == nil {
		s.scratchReimport.Store(nil)
		return
	}
	s.scratchReimport.Store(&fn)
}

// scratchDir is where a re-import's copy of the set lives, beside the set.
func (s *Set) scratchDir() string {
	return s.dir + "-reimport"
}

// newScratch opens a copy of the live set in a directory of its own. Its
// checkpoint directory is the live set's, so the swap goes through the same
// restore the checkpoints use.
func (s *Set) newScratch(ctx context.Context) (*Set, error) {
	ckptDir, keep := s.checkpointDir()
	if ckptDir == "" {
		return nil, errors.New(
			"utxoset: a re-import needs a checkpoint directory to swap the imported set in through",
		)
	}
	dir := s.scratchDir()
	if err := os.RemoveAll(dir); err != nil {
		return nil, fmt.Errorf("utxoset: clear the scratch set: %w", err)
	}
	sc, err := Open(Config{
		Dir: dir, Logger: s.logger, HistorySlots: s.history,
		Only: s.only, Authoritative: s.rollBackAhead,
		CheckpointDir: ckptDir, CheckpointKeep: keep,
	})
	if err != nil {
		return nil, fmt.Errorf("utxoset: open the scratch set: %w", err)
	}
	// The import keeps the families the set holds, as a catch-up does, so
	// the copy starts as the live set. A copy that fails is not replaced by
	// an empty set: the import would then drop the history the live set
	// holds and swap it in.
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(s.store.Backup(ctx, pw)) }()
	if err := sc.store.Restore(ctx, pr); err != nil {
		_ = pr.CloseWithError(err)
		_ = sc.Close()
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("utxoset: copy the live set for the re-import: %w", err)
	}
	return sc, nil
}

func (s *Set) closeScratch(sc *Set) {
	_ = sc.Close()
	_ = os.RemoveAll(s.scratchDir())
}

// swapIn replaces the live set with the scratch copy, which holds the state at
// point: the copy is written as a checkpoint, which is then restored over the
// live set the way a checkpoint recovery restores one.
func (s *Set) swapIn(
	ctx context.Context,
	sc *Set,
	point ocommon.Point,
) error {
	tip, ok, err := sc.store.Tip()
	if err != nil {
		return fmt.Errorf("utxoset: read the imported set's tip: %w", err)
	}
	if !ok || !samePoint(tip, point) {
		return fmt.Errorf(
			"utxoset: the imported set is at slot %d, not the ledger state's slot %d",
			tip.Slot, point.Slot,
		)
	}
	// The live set's own checkpoint writes prune by slot, and the copy's
	// state is older than the live set's whenever the import loads an
	// earlier snapshot, so a live write between the copy's checkpoint and its
	// restore would delete it. Holding the live writers' mutex across both
	// keeps them out; the copy writes under its own.
	s.ckptWrite.Lock()
	defer s.ckptWrite.Unlock()
	// The checkpoint is written pinned against this set's rotation too, and
	// the checkpoints of the state it replaces go once the restore has
	// finished.
	cp, err := sc.writeCheckpoint(ctx, true)
	if err != nil {
		return fmt.Errorf("utxoset: write the imported set: %w", err)
	}
	if s.afterSwapCheckpoint != nil {
		s.afterSwapCheckpoint()
	}
	if err := s.restoreCheckpoint(ctx, cp); err != nil {
		return fmt.Errorf("utxoset: replace the live set with the imported one: %w", err)
	}
	if err := s.dropCheckpointsExcept(cp); err != nil {
		return err
	}
	return nil
}

// runReimport runs the installed re-import and reports whether one is
// installed.
func (s *Set) runReimport(
	ctx context.Context,
	localTip ocommon.Point,
) (ocommon.Point, bool, error) {
	if fn := s.scratchReimport.Load(); fn != nil {
		if err := s.reimportSpace(); err != nil {
			return ocommon.Point{}, true, err
		}
		sc, err := s.newScratch(ctx)
		if err != nil {
			return ocommon.Point{}, true, err
		}
		defer s.closeScratch(sc)
		point, err := (*fn)(ctx, localTip, ReimportTarget{
			Set: sc,
			Commit: func(ctx context.Context, point ocommon.Point) error {
				return s.swapIn(ctx, sc, point)
			},
		})
		return point, true, err
	}
	if fn := s.reimport.Load(); fn != nil {
		point, err := (*fn)(ctx, localTip)
		return point, true, err
	}
	return ocommon.Point{}, false, nil
}

// reimportSpace refuses a re-import the disk cannot hold, before the import
// writes anything: the copy of the set beside it and the checkpoint the swap
// writes are both about the size of the set's data, and the checkpoints
// already kept stay until the swap has finished.
func (s *Set) reimportSpace() error {
	ckptDir, _ := s.checkpointDir()
	if ckptDir == "" {
		return nil
	}
	size := s.store.DataBytes()
	need := size + size/10
	avail := s.availableDisk
	if avail == nil {
		avail = AvailableDiskBytes
	}
	if err := os.MkdirAll(ckptDir, 0o750); err != nil {
		return fmt.Errorf("utxoset: create checkpoint directory: %w", err)
	}
	scratchFree, err := avail(filepath.Dir(s.scratchDir()))
	if err != nil {
		return nil //nolint:nilerr // an unreadable figure is not a reason to refuse
	}
	ckptFree, err := avail(ckptDir)
	if err != nil {
		return nil //nolint:nilerr
	}
	// Two directories reporting the same free figure are taken to share a
	// disk, which then has to hold both files.
	total := need
	if scratchFree == ckptFree {
		total = 2 * need
	}
	if need < 0 || (scratchFree > uint64(total) && ckptFree > uint64(total)) {
		return nil
	}
	s.logger.Warn(
		"refusing a live UTxO set re-import: the disk lacks the space for it",
		"free_bytes", min(scratchFree, ckptFree), "needed_bytes", total,
	)
	return fmt.Errorf(
		"%w: %d bytes free, about %d needed for a re-import",
		ErrCheckpointNoSpace, min(scratchFree, ckptFree), total,
	)
}
