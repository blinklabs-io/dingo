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

package mithril

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// errCatchUpLocalAhead reports that the local chain is a strict descendant of
// the target artifact's chain: its tip is above the artifact's sealed range and
// the artifact's tip block is present on the local chain. There is nothing to
// catch up; importing the (older) artifact would rewind the database.
var errCatchUpLocalAhead = errors.New(
	"local chain is ahead of the target Mithril artifact",
)

// verifyCatchupBeforeImport wraps verifyCatchupIntersection for Sync: a
// strictly-ahead local chain is mapped to (upToDate=true) after advancing the
// import marker to targetImmutable, so later runs no-op without re-downloading.
func verifyCatchupBeforeImport(
	db *database.Database,
	immutableDir string,
	targetImmutable uint64,
	logger *slog.Logger,
) (upToDate bool, err error) {
	verifyErr := verifyCatchupIntersection(db, immutableDir, logger)
	if verifyErr == nil {
		return false, nil
	}
	if !errors.Is(verifyErr, errCatchUpLocalAhead) {
		return false, verifyErr
	}
	// The local chain already contains every block the artifact seals, so the
	// database supersedes the artifact. Advance the import marker so later
	// runs no-op before downloading anything.
	if targetImmutable > 0 {
		if markErr := setImmutableImportMarker(
			db, targetImmutable,
		); markErr != nil {
			return false, markErr
		}
	}
	return true, nil
}

// verifyCatchupIntersection confirms the local chain tip is present, with a
// matching hash, in the freshly-downloaded target immutable data — i.e. the
// local chain is an ancestor of the target artifact's chain. It MUST run before
// any state mutation so a divergent database is left untouched and the operator
// is told to perform a full resync.
//
// When the local tip is above the target's sealed immutable range (its block is
// not yet in an immutable file of the artifact), the check is reversed: the
// artifact's tip block must be present on the local chain, proving the local
// chain a strict descendant with nothing to catch up (errCatchUpLocalAhead).
// Anything else is a divergence. Importing would otherwise apply an OLDER
// snapshot over a newer database: the reconcile pass would tombstone every
// live row created after the artifact tip and the volatile cleanup would
// rewind the chain, permanently corrupting the database.
func verifyCatchupIntersection(
	db *database.Database,
	immutableDir string,
	logger *slog.Logger,
) error {
	recent, err := database.BlocksRecent(db, 1)
	if err != nil {
		return fmt.Errorf("reading local chain tip: %w", err)
	}
	if len(recent) == 0 {
		return errors.New("catch-up: local database has no chain tip")
	}
	tip := recent[0]

	imm, err := immutable.New(immutableDir)
	if err != nil {
		return fmt.Errorf("opening target immutable DB: %w", err)
	}
	iter, err := imm.BlocksFromPoint(
		ocommon.Point{Slot: tip.Slot, Hash: tip.Hash},
	)
	if err != nil {
		if errors.Is(err, immutable.ErrPointBeyondLastChunk) {
			return verifyLocalAheadOfArtifactTip(db, imm, tip, logger)
		}
		return fmt.Errorf("locating local tip in target immutable DB: %w", err)
	}
	defer func() { _ = iter.Close() }()

	first, err := iter.Next()
	if err != nil {
		return fmt.Errorf("reading target immutable at local tip: %w", err)
	}
	if first == nil {
		return verifyLocalAheadOfArtifactTip(db, imm, tip, logger)
	}
	if first.Slot != tip.Slot || !bytes.Equal(first.Hash, tip.Hash) {
		return fmt.Errorf(
			"local chain diverges from the target Mithril v2 artifact at "+
				"slot %d (local block %x; artifact has slot %d block %x); "+
				"perform a full Mithril resync (remove the database and run "+
				"`dingo mithril sync` again)",
			tip.Slot, tip.Hash, first.Slot, first.Hash,
		)
	}
	logger.Info(
		"catch-up: local chain tip confirmed on the target artifact chain",
		"component", "mithril",
		"tip_slot", tip.Slot,
	)
	return nil
}

func verifyLocalAheadOfArtifactTip(
	db *database.Database,
	imm *immutable.ImmutableDb,
	localTip models.Block,
	logger *slog.Logger,
) error {
	artifactTip, err := imm.GetTip()
	if err != nil {
		return fmt.Errorf("reading target immutable tip: %w", err)
	}
	if artifactTip == nil {
		return errors.New("catch-up: target immutable DB has no chain tip")
	}
	_, err = database.BlockByPoint(db, *artifactTip)
	if err == nil {
		logger.Info(
			"catch-up: local chain is ahead of the target artifact",
			"component", "mithril",
			"local_tip_slot", localTip.Slot,
			"artifact_tip_slot", artifactTip.Slot,
		)
		return errCatchUpLocalAhead
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		return fmt.Errorf("looking up target immutable tip locally: %w", err)
	}
	return fmt.Errorf(
		"local chain diverges from the target Mithril v2 artifact above "+
			"slot %d (local tip slot %d block %x; artifact tip block %x "+
			"is not present locally); perform a full Mithril resync "+
			"(remove the database and run `dingo mithril sync` again)",
		artifactTip.Slot, localTip.Slot, localTip.Hash, artifactTip.Hash,
	)
}
