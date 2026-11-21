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

	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ReimportFunc loads ledger state from a Mithril snapshot whose point is at or
// below localTip into the set and the metadata store, and returns the point it
// loaded. The chain's blocks are never fetched again: the ledger replays the
// blocks it already stores from that point.
type ReimportFunc func(
	ctx context.Context,
	localTip ocommon.Point,
) (ocommon.Point, error)

// SetReimporter installs the function recoverUnreachable falls back to when no
// checkpoint can bring the set to the ledger. Nil removes it.
func (s *Set) SetReimporter(fn ReimportFunc) {
	if fn == nil {
		s.reimport.Store(nil)
		return
	}
	s.reimport.Store(&fn)
}

// CanReimport reports whether a re-importer is installed.
func (s *Set) CanReimport() bool {
	return s.reimport.Load() != nil || s.scratchReimport.Load() != nil
}

// FinishRestore finishes a checkpoint restore an earlier run began and did not
// finish, so the set is not read while it may hold part of one.
func (s *Set) FinishRestore(ctx context.Context) error {
	return s.finishPendingRestore(ctx)
}

// Reimport runs the installed re-importer, for a start that finds a re-import
// the node began unfinished. It fails when none is installed.
func (s *Set) Reimport(
	ctx context.Context,
	localTip ocommon.Point,
) (ocommon.Point, error) {
	point, ok, err := s.runReimport(ctx, localTip)
	if !ok {
		return ocommon.Point{}, errors.New("utxoset: no Mithril re-import is available")
	}
	return point, err
}

// recoverUnreachable runs when the set's journal cannot bring it to the ledger,
// which cause describes: the set is behind a Mithril trust boundary or the
// consumed-output prune floor, a failed commit is older than the journal's
// floor, or the ledger's tip is below the journal's floor. It tries, in order:
//
//  1. the newest checkpoint at or below the ledger's tip: the set is restored,
//     and the ledger is rolled back to the checkpoint's point so it replays
//     the blocks it stores;
//  2. a re-import of ledger state from a Mithril snapshot at or below the
//     ledger's tip, through the reimporter the node installed.
//
// It returns nil once the set and the ledger agree at some point, and cause
// extended with what is missing when neither source is available.
func (s *Set) recoverUnreachable(
	ctx context.Context,
	resume ResumeFunc,
	ledgerTip ocommon.Point,
	cause error,
) error {
	recovered, tried, err := s.recoverFromCheckpoints(ctx, resume, ledgerTip)
	if err != nil {
		return errors.Join(cause, err)
	}
	if recovered {
		return nil
	}
	if s.CanReimport() {
		s.logger.Warn(
			"no checkpoint can bring the live UTxO set to the ledger; "+
				"re-importing ledger state from a Mithril snapshot",
			"ledger_slot", ledgerTip.Slot,
			"checkpoints_tried", tried,
		)
		point, _, err := s.runReimport(ctx, ledgerTip)
		if err != nil {
			return fmt.Errorf(
				"%w; the re-import of ledger state from a Mithril snapshot at or below slot %d failed: %w",
				cause, ledgerTip.Slot, err,
			)
		}
		s.logger.Info(
			"re-imported ledger state from a Mithril snapshot; the ledger replays its stored blocks from there",
			"slot", point.Slot, "ledger_slot", ledgerTip.Slot,
		)
		return nil
	}
	return fmt.Errorf("%w; %s", cause, s.missingRecovery(ledgerTip, tried))
}

// missingRecovery names what recovery lacked.
func (s *Set) missingRecovery(ledgerTip ocommon.Point, tried int) string {
	dir, _ := s.checkpointDir()
	var ckpt string
	switch {
	case dir == "":
		ckpt = "no checkpoint directory is configured (ledgerUtxoCheckpointEpochs)"
	case tried == 0:
		ckpt = fmt.Sprintf(
			"the checkpoint directory %s holds no checkpoint at or below slot %d",
			dir, ledgerTip.Slot,
		)
	default:
		ckpt = fmt.Sprintf(
			"none of the %d checkpoints at or below slot %d in %s could be restored and resumed from",
			tried, ledgerTip.Slot, dir,
		)
	}
	return ckpt + ", and no Mithril aggregator is available for this network to re-import ledger state from"
}

// recoverFromCheckpoints restores the newest checkpoint at or below the
// ledger's tip that loads, and asks the ledger to roll back to its point. A
// checkpoint that fails its digest, loads in a layout this release does not
// read, or holds a tip above the ledger's is skipped; a point the ledger
// declines, such as one below a Mithril trust boundary, is skipped for an
// older one.
func (s *Set) recoverFromCheckpoints(
	ctx context.Context,
	resume ResumeFunc,
	ledgerTip ocommon.Point,
) (recovered bool, tried int, err error) {
	cps, err := s.Checkpoints()
	if err != nil {
		return false, 0, err
	}
	for _, cp := range cps {
		if cp.Slot > ledgerTip.Slot {
			continue
		}
		tried++
		s.logger.Warn(
			"restoring a live UTxO set checkpoint",
			"checkpoint_slot", cp.Slot,
			"bytes", cp.Bytes,
			"ledger_slot", ledgerTip.Slot,
		)
		tip, err := s.validateCheckpoint(ctx, cp)
		if err != nil {
			s.logger.Warn(
				"a live UTxO set checkpoint cannot be used",
				"checkpoint_slot", cp.Slot, "error", err,
			)
			continue
		}
		if tip.Slot > ledgerTip.Slot ||
			(tip.Slot == ledgerTip.Slot && !samePoint(tip, ledgerTip)) {
			s.logger.Warn(
				"a live UTxO set checkpoint is ahead of the ledger",
				"checkpoint_slot", tip.Slot, "ledger_slot", ledgerTip.Slot,
			)
			continue
		}
		if !samePoint(tip, ledgerTip) {
			if resume == nil {
				continue
			}
			// Ask before anything is replaced: a checkpoint the ledger
			// declines leaves the set as it was.
			ok, err := resume(ledger.WithResumeCheck(ctx), tip)
			if err != nil {
				return false, tried, fmt.Errorf(
					"check that the ledger can roll back to the checkpoint at slot %d: %w",
					tip.Slot, err,
				)
			}
			if !ok {
				s.logger.Warn(
					"the ledger cannot roll back to a live UTxO set checkpoint",
					"checkpoint_slot", tip.Slot, "ledger_slot", ledgerTip.Slot,
				)
				continue
			}
		}
		if err := s.restoreCheckpoint(ctx, cp); err != nil {
			return false, tried, fmt.Errorf(
				"replace the live UTxO set with the checkpoint at slot %d: %w",
				cp.Slot, err,
			)
		}
		if samePoint(tip, ledgerTip) {
			s.logger.Info(
				"restored a live UTxO set checkpoint at the ledger's tip",
				"slot", tip.Slot,
			)
			return true, tried, nil
		}
		if resume == nil {
			continue
		}
		s.resuming.Store(true)
		resumed, err := resume(ctx, tip)
		s.resuming.Store(false)
		if err != nil && !resumed {
			return false, tried, fmt.Errorf(
				"roll the ledger back to the checkpoint at slot %d: %w",
				tip.Slot, err,
			)
		}
		if resumed {
			if err != nil {
				s.logger.Warn(
					"the ledger rolled back to a live UTxO set checkpoint but reported an error",
					"checkpoint_slot", tip.Slot, "error", err,
				)
			}
			s.m.resumes.Inc()
			s.logger.Info(
				"restored a live UTxO set checkpoint; the ledger replays its stored blocks from it",
				"checkpoint_slot", tip.Slot, "ledger_slot", ledgerTip.Slot,
			)
			return true, tried, nil
		}
	}
	return false, tried, nil
}
