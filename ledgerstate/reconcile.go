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

package ledgerstate

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// reconcileKeys is the set of live ledger keys present in an imported snapshot.
// During a Mithril v2 catch-up import the import phases populate it; the
// reconcile then marks any live database row whose key is absent here inactive.
// The snapshot is the complete, trusted ledger state at its tip, so every live
// row missing from it has been spent/deregistered/retired by that tip.
type reconcileKeys struct {
	accounts                    map[string]struct{}
	pools                       map[string]struct{}
	dreps                       map[string]struct{}
	governanceProposals         map[string]struct{}
	accountsComplete            bool
	poolsComplete               bool
	drepsComplete               bool
	governanceProposalsComplete bool
	// drained is the set of families whose SQL rows were deleted before the
	// import: none of their rows can be stale.
	drained nodesettings.FamilyMask
}

func newReconcileKeys() *reconcileKeys {
	return &reconcileKeys{
		accounts:            make(map[string]struct{}),
		pools:               make(map[string]struct{}),
		dreps:               make(map[string]struct{}),
		governanceProposals: make(map[string]struct{}),
	}
}

func (k *reconcileKeys) addAccount(tag uint8, stakingKey []byte) {
	k.accounts[models.NewStakeCredentialRef(tag, stakingKey).MapKey()] = struct{}{}
}

func (k *reconcileKeys) addPool(keyHash []byte) {
	k.pools[string(keyHash)] = struct{}{}
}

func (k *reconcileKeys) addDrep(tag uint8, credential []byte) {
	k.dreps[models.NewStakeCredentialRef(tag, credential).MapKey()] = struct{}{}
}

func (k *reconcileKeys) addGovernanceProposal(txHash []byte, actionIndex uint32) {
	k.governanceProposals[govActionIdKey(txHash, actionIndex)] = struct{}{}
}

func (k *reconcileKeys) markCertStateComplete() {
	k.accountsComplete = true
	k.poolsComplete = true
	k.drepsComplete = true
}

func (k *reconcileKeys) markGovernanceProposalsComplete() {
	k.governanceProposalsComplete = true
}

// reconcileStaleLedgerState marks every live row absent from the imported
// snapshot's live key set inactive: accounts and DReps go Active=false, pools get a retirement at tipEpoch, and stale
// governance proposals get DeletedSlot=tipSlot. It never deletes a row. On an
// empty database (a fresh bootstrap) it is a no-op.
//
// Each entity is reconciled only after its import path marks key recording
// complete. A completed empty key set is authoritative and deactivates any
// previously-live rows; an incomplete empty key set is skipped so an import path
// that did not process an entity cannot mass-deactivate unrelated live state.
//
// Stale keys are collected under a read transaction; all mutations are applied
// together under one write transaction so the reconcile is atomic.
func reconcileStaleLedgerState(
	ctx context.Context,
	db *database.Database,
	keys *reconcileKeys,
	tipSlot uint64,
	tipEpoch uint64,
	logger *slog.Logger,
) error {
	if logger == nil {
		logger = slog.Default()
	}
	store := db.Metadata()

	// Phase 1: collect the live rows that are absent from the snapshot.
	var (
		staleAccts     []models.StakeCredentialRef
		staleDreps     []models.StakeCredentialRef
		stalePools     [][]byte
		staleProposals []*models.GovernanceProposal
	)

	skipDrained := func(entity string) {
		logger.Info(
			"skipping "+entity+" reconcile: its SQL rows were drained, so "+
				"none can be stale",
			"component", "ledgerstate",
		)
	}
	skipIncomplete := func(entity string) {
		logger.Warn(
			"skipping "+entity+" reconcile: the snapshot import did not "+
				"complete live-key recording for this entity",
			"component", "ledgerstate",
		)
	}

	rtxn := db.MetadataTxn(ctx, false)
	if keys.drained.Has(nodesettings.FamilyAccounts) {
		skipDrained("account")
	} else if !keys.accountsComplete {
		skipIncomplete("account")
	} else {
		accts, err := store.GetActiveAccountCredentials(rtxn.Metadata())
		if err != nil {
			rtxn.Release()
			return fmt.Errorf("listing active accounts: %w", err)
		}
		for _, a := range accts {
			if err := ctx.Err(); err != nil {
				rtxn.Release()
				return fmt.Errorf("scanning active accounts: %w", err)
			}
			if _, ok := keys.accounts[a.MapKey()]; !ok {
				staleAccts = append(staleAccts, models.StakeCredentialRef{
					Tag: a.Tag,
					Key: bytes.Clone(a.Key),
				})
			}
		}
	}

	if keys.drained.Has(nodesettings.FamilyDReps) {
		skipDrained("DRep")
	} else if !keys.drepsComplete {
		skipIncomplete("DRep")
	} else {
		dreps, err := store.GetActiveDreps(rtxn.Metadata())
		if err != nil {
			rtxn.Release()
			return fmt.Errorf("listing active DReps: %w", err)
		}
		for _, d := range dreps {
			if err := ctx.Err(); err != nil {
				rtxn.Release()
				return fmt.Errorf("scanning active DReps: %w", err)
			}
			ref := models.NewStakeCredentialRef(d.CredentialTag, d.Credential)
			if _, ok := keys.dreps[ref.MapKey()]; !ok {
				staleDreps = append(staleDreps, models.StakeCredentialRef{
					Tag: d.CredentialTag,
					Key: bytes.Clone(d.Credential),
				})
			}
		}
	}

	if keys.drained.Has(nodesettings.FamilyPoolsHistory) {
		skipDrained("pool")
	} else if !keys.poolsComplete {
		skipIncomplete("pool")
	} else {
		pools, err := store.GetActivePoolKeyHashes(rtxn.Metadata())
		if err != nil {
			rtxn.Release()
			return fmt.Errorf("listing active pools: %w", err)
		}
		for _, p := range pools {
			if err := ctx.Err(); err != nil {
				rtxn.Release()
				return fmt.Errorf("scanning active pools: %w", err)
			}
			if _, ok := keys.pools[string(p)]; !ok {
				stalePools = append(stalePools, bytes.Clone(p))
			}
		}
	}

	if keys.drained.Has(nodesettings.FamilyGovernance) {
		skipDrained("governance proposal")
	} else if !keys.governanceProposalsComplete {
		skipIncomplete("governance proposal")
	} else {
		proposals, err := db.GetGovernanceProposalSet(ctx, rtxn)
		if err != nil {
			rtxn.Release()
			return fmt.Errorf("listing governance proposal set: %w", err)
		}
		for _, proposal := range proposals {
			if err := ctx.Err(); err != nil {
				rtxn.Release()
				return fmt.Errorf("scanning governance proposal set: %w", err)
			}
			if _, ok := keys.governanceProposals[govActionIdKey(proposal.TxHash, proposal.ActionIndex)]; !ok {
				staleProposals = append(staleProposals, proposal)
			}
		}
	}
	rtxn.Release()

	// Phase 2: apply all the inactive-markings atomically.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"reconcile cancelled before applying stale markers: %w",
			err,
		)
	}
	wtxn := db.MetadataTxn(ctx, true)
	defer wtxn.Release()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"reconcile cancelled before applying stale markers: %w",
			err,
		)
	}
	if err := store.DeactivateAccounts(
		wtxn.Metadata(), staleAccts, tipSlot,
	); err != nil {
		return fmt.Errorf(
			"deactivating %d stale accounts: %w",
			len(staleAccts),
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"reconcile cancelled before applying stale markers: %w",
			err,
		)
	}
	if err := store.DeactivateDreps(
		wtxn.Metadata(), staleDreps, tipSlot,
	); err != nil {
		return fmt.Errorf(
			"deactivating %d stale DReps: %w",
			len(staleDreps),
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"reconcile cancelled before applying stale markers: %w",
			err,
		)
	}
	if err := store.RetirePools(
		wtxn.Metadata(), stalePools, tipEpoch, tipSlot,
	); err != nil {
		return fmt.Errorf("retiring %d stale pools: %w", len(stalePools), err)
	}
	for _, proposal := range staleProposals {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf(
				"reconcile cancelled before applying stale markers: %w",
				err,
			)
		}
		deletedSlot := tipSlot
		proposal.DeletedSlot = &deletedSlot
		if err := store.SetGovernanceProposal(
			proposal,
			wtxn.Metadata(),
		); err != nil {
			return fmt.Errorf(
				"retiring stale governance proposal %x#%d: %w",
				proposal.TxHash,
				proposal.ActionIndex,
				err,
			)
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"reconcile cancelled before applying stale markers: %w",
			err,
		)
	}
	if err := wtxn.Commit(); err != nil {
		return fmt.Errorf("committing reconcile: %w", err)
	}

	logger.Info(
		"reconciled stale ledger state against newer snapshot",
		"component", "ledgerstate",
		"tip_slot", tipSlot,
		"tip_epoch", tipEpoch,
		"accounts_deactivated", len(staleAccts),
		"dreps_deactivated", len(staleDreps),
		"pools_retired", len(stalePools),
		"governance_proposals_retired", len(staleProposals),
	)
	return nil
}
