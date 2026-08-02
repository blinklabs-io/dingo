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

package dingo

import (
	"fmt"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/recovery"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// crashRecoveryConfig translates the node's crash-recovery settings into the
// database's. It returns nil when the subsystem is disabled, which is what
// switches it off end to end.
func (n *Node) crashRecoveryConfig() (*recovery.Config, error) {
	if !n.config.crashRecovery.Enabled {
		return nil, nil
	}
	checkMode, err := recovery.ParseCheckMode(
		n.config.crashRecovery.ConsistencyCheckMode,
	)
	if err != nil {
		return nil, err
	}
	return &recovery.Config{
		Logger:             n.config.logger,
		CheckMode:          checkMode,
		CheckpointInterval: n.config.crashRecovery.CheckpointInterval,
		SyncJournal:        n.config.crashRecovery.SyncJournal,
	}, nil
}

// nodeRecoverySource adds the chain manager's tip to the database's view of
// stored state.
//
// The database cannot report it — the chain manager sits above the database and
// the import direction only goes one way — but recovery needs it: blocks
// between the applied tip and the chain tip are legitimately on-chain and
// merely not applied yet, and must not be mistaken for the residue of an
// interrupted commit.
type nodeRecoverySource struct {
	recovery.StateSource
	chain *chain.Chain
}

// ChainTip implements recovery.ChainTipSource.
//
// No primary chain is reported as a zero tip rather than an error. An error
// here would propagate out of recovery and abort startup, which is exactly what
// recovery is not supposed to do; a zero tip instead leaves the trim boundary
// at the metadata tip, and recovery's own guard against trimming with no known
// tip still protects the blob store.
func (s nodeRecoverySource) ChainTip() (recovery.Point, uint64, error) {
	if s.chain == nil {
		return recovery.Point{}, 0, nil
	}
	tip := s.chain.Tip()
	return recovery.Point{
		Slot: tip.Point.Slot,
		Hash: tip.Point.Hash,
	}, tip.BlockNumber, nil
}

// nodeRecoveryRepairer carries out the repairs recovery decides on.
type nodeRecoveryRepairer struct {
	db     *database.Database
	ledger *ledger.LedgerState
	chain  *chain.ChainManager
}

// TrimBlobAbove removes blocks the blob store holds above slot.
func (r nodeRecoveryRepairer) TrimBlobAbove(slot uint64) (int, error) {
	return r.db.TrimBlobAbove(slot)
}

// RollbackTo rewinds applied ledger state to point.
func (r nodeRecoveryRepairer) RollbackTo(point recovery.Point) error {
	return r.ledger.RollbackToPoint(ocommon.Point{
		Slot: point.Slot,
		Hash: point.Hash,
	})
}

func (r nodeRecoveryRepairer) RewindPrimaryChainTo(point recovery.Point) error {
	if r.chain == nil {
		return fmt.Errorf("primary chain manager is unavailable")
	}
	return r.chain.RewindPrimaryChainToPoint(ocommon.Point{
		Slot: point.Slot,
		Hash: point.Hash,
	})
}

// ResetCommitFence brings both stores back onto a common commit timestamp.
func (r nodeRecoveryRepairer) ResetCommitFence() error {
	return r.db.ResetCommitFence()
}

// runCrashRecovery runs the startup consistency checks and repairs whatever
// divergence they and the intent journal identify, then starts periodic
// checkpointing.
//
// It runs after the chain manager and ledger are up, because both the diagnosis
// and the repairs need them: the chain tip bounds what may be trimmed, and a
// rollback is ledger work.
func (n *Node) runCrashRecovery() error {
	mgr := n.db.Recovery()
	if mgr == nil {
		return nil
	}
	source := nodeRecoverySource{
		StateSource: n.db.RecoveryStateSource(),
		chain:       n.chainManager.PrimaryChain(),
	}
	result, err := mgr.Recover(source, nodeRecoveryRepairer{
		db:     n.db,
		ledger: n.ledgerState,
		chain:  n.chainManager,
	})
	if err != nil {
		return fmt.Errorf("crash recovery failed: %w", err)
	}
	switch result.Outcome {
	case recovery.OutcomeClean:
		n.config.logger.Info(
			"crash recovery found no work to do",
			"component", "database",
			"consistency_checks", result.Report.Worst().String(),
		)
	case recovery.OutcomeRepaired:
		n.config.logger.Warn(
			"crash recovery repaired an interrupted commit",
			"component", "database",
			"actions", result.Actions,
			"unresolved_intents", len(result.Pending),
		)
	case recovery.OutcomeUnrepaired:
		// Startup continues: the existing tip reconciliation runs after
		// this and fixes several of the shapes that land here, and a
		// node that refuses to start is worse for an operator than one
		// that starts and says loudly what it found.
		n.config.logger.Error(
			"crash recovery found problems it could not repair",
			"component", "database",
			"actions", result.Actions,
			"unresolved_intents", len(result.Pending),
		)
	}
	mgr.Start(source)
	return nil
}
