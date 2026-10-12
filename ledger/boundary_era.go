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

package ledger

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// errBoundaryEraNotAuthorized marks an epoch-boundary block encoded in an era
// other than the one the ledger moves into.
var errBoundaryEraNotAuthorized = errors.New(
	"boundary block era not authorized by the ledger",
)

// boundaryEraInputs is the ledger state the era decision at an epoch boundary
// reads. The forger and the boundary check build it from the same fields, so
// they cannot disagree about the era.
type boundaryEraInputs struct {
	// sourceEra is the era of the epoch that ends, epoch that epoch and tip
	// the last block applied in it.
	sourceEra eras.EraDesc
	epoch     models.Epoch
	tip       ochainsync.Tip
	// pparams are the source-era parameters after the boundary's classic
	// update enactment and before any translation.
	pparams lcommon.ProtocolParameters
	// newEpochID is the epoch the first block after the boundary opens.
	newEpochID uint64
	// txn is the boundary transaction, nil outside a rollover.
	txn *database.Txn
}

// ledgerBoundaryEra returns the era the ledger is in after the boundary into
// in.newEpochID, decided from ledger state alone as the hard-fork combinator's
// singleEraTransition does (Shelley/ShelleyHFC.hs): a TriggerAtEpoch era ends
// at its epoch, and a TriggerAtVersion era ends once the parameters in force
// next epoch name the next era's major and at least k blocks of the ending
// epoch lie at or past the voting deadline (shelleyAfterVoting >= k).
//
// From Conway the major moves through HardForkInitiation, which ENACT applies
// at the boundary itself, so the major is forecast from the proposals ENACT
// will enact rather than read from pparams. A source without a protocol
// version is undecided and decided is false. So is Byron, which carries none:
// validateByronShelleyTransition gates that boundary.
//
// A returned error is a failure to read ledger state, not a verdict.
func (ls *LedgerState) ledgerBoundaryEra(
	ctx context.Context,
	in boundaryEraInputs,
) (eraID uint, decided bool, err error) {
	sourceID := in.sourceEra.Id
	if sourceID == byron.EraIdByron {
		return sourceID, false, nil
	}
	version, versionErr := GetProtocolVersion(in.pparams)
	if versionErr != nil {
		return sourceID, false, nil //nolint:nilerr // no version, nothing to judge
	}
	shape, err := ls.eraShapeWithError()
	if err != nil {
		return 0, false, fmt.Errorf("era shape: %w", err)
	}
	eraList := ls.eraList()
	indexOf := func(id uint) int {
		return slices.IndexFunc(eraList, func(e eras.EraDesc) bool {
			return e.Id == id
		})
	}
	reached := indexOf(sourceID)
	if reached < 0 {
		return sourceID, false, nil
	}

	// The two version-trigger inputs are read lazily and once: the forecast
	// reads governance state and the count reads the chain.
	var (
		major         uint
		majorResolved bool
	)
	resolveMajor := func() (uint, error) {
		if majorResolved {
			return major, nil
		}
		major = version.Major
		if sourceID >= eras.ConwayEraDesc.Id {
			forecast, err := ls.forecastHardForkInitiations(ctx, in)
			if err != nil {
				return 0, err
			}
			if forecast != nil {
				forecastVersion, err := GetProtocolVersion(forecast)
				if err != nil {
					return 0, err
				}
				major = forecastVersion.Major
			}
		}
		majorResolved = true
		return major, nil
	}
	var (
		closed         bool
		closedResolved bool
	)
	votingClosed := func() (bool, error) {
		if !closedResolved {
			var err error
			closed, err = ls.votingWindowClosed(
				ctx, sourceID, in.epoch, in.tip,
			)
			if err != nil {
				return false, err
			}
			closedResolved = true
		}
		return closed, nil
	}

	for {
		entry, ok := shape.EraForID(eraList[reached].Id)
		if !ok {
			break
		}
		advance := false
		switch entry.NextEraTrigger.Kind {
		case hardfork.TriggerAtEpoch:
			advance = entry.NextEraTrigger.Epoch <= in.newEpochID
		case hardfork.TriggerAtVersion:
			currentMajor, err := resolveMajor()
			if err != nil {
				return 0, false, err
			}
			versionEra, ok := ls.eraForVersion(currentMajor)
			if !ok || indexOf(versionEra) <= reached {
				break
			}
			advance, err = votingClosed()
			if err != nil {
				return 0, false, err
			}
		case hardfork.TriggerNotDuringThisExecution:
		}
		if !advance {
			break
		}
		// A schedule that names an era this binary does not run cannot be
		// followed, and must not read as a ledger that stays put.
		if reached+1 >= len(eraList) {
			return 0, false, fmt.Errorf(
				"era %d ends at epoch %d but has no successor",
				eraList[reached].Id, in.newEpochID,
			)
		}
		reached++
	}
	return eraList[reached].Id, true, nil
}

// votingWindowClosed reports whether at least k blocks of epoch lie at or past
// its voting deadline, the point from which a pparams change for the next
// epoch can no longer be rolled back.
func (ls *LedgerState) votingWindowClosed(
	ctx context.Context,
	eraID uint,
	epoch models.Epoch,
	tip ochainsync.Tip,
) (bool, error) {
	k, ok := ls.securityParamForEra(eraID)
	if !ok {
		return false, errors.New("security parameter is unavailable")
	}
	countFrom, ok := ls.votingCountStartSlot(eraID, epoch)
	if !ok {
		return false, fmt.Errorf(
			"voting deadline of epoch %d is unavailable", epoch.EpochId,
		)
	}
	count, err := ls.countBlocksFromSlot(ctx, tip, countFrom)
	if err != nil {
		return false, fmt.Errorf(
			"count blocks of epoch %d from slot %d: %w",
			epoch.EpochId, countFrom, err,
		)
	}
	return count >= k, nil
}

// forecastHardForkInitiations returns in.pparams with the protocol version the
// boundary's ENACT will leave, or nil when governance state is unavailable.
func (ls *LedgerState) forecastHardForkInitiations(
	ctx context.Context,
	in boundaryEraInputs,
) (lcommon.ProtocolParameters, error) {
	if ls.db == nil || in.sourceEra.PParamsUpdateFunc == nil {
		return nil, nil
	}
	boundarySlot, err := checkedSlotAdd(
		in.epoch.StartSlot, uint64(in.epoch.LengthInSlots),
	)
	if err != nil {
		return nil, err
	}
	// Read before the ratified rows, so that a decision written in between
	// is seen by one of the two.
	pending, err := ls.pendingRatifiedProposals(ctx, in.txn)
	if err != nil {
		return nil, err
	}
	return governance.ForecastHardForkInitiations(
		ctx,
		ls.db,
		in.txn,
		in.epoch.EpochId+1,
		boundarySlot,
		in.pparams,
		in.sourceEra.PParamsUpdateFunc,
		pending,
	)
}

// pendingRatifiedProposals returns the proposals the previous boundary's
// deferred RATIFY decided and has not yet written, waiting for that decision.
func (ls *LedgerState) pendingRatifiedProposals(
	ctx context.Context,
	txn *database.Txn,
) ([]*models.GovernanceProposal, error) {
	rec, err := loadPendingRatification(ls.db, txn)
	if err != nil || rec == nil {
		return nil, err
	}
	ls.ratificationMu.Lock()
	job := ls.ratificationJob
	ls.ratificationMu.Unlock()
	if job == nil || job.record != *rec {
		return nil, fmt.Errorf(
			"pending ratification for epoch %d at slot %d has no job",
			rec.Epoch, rec.BoundarySlot,
		)
	}
	select {
	case <-job.decided:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if job.err != nil {
		return nil, fmt.Errorf(
			"ratification for epoch %d failed: %w", rec.Epoch, job.err,
		)
	}
	if job.decision == nil {
		return nil, nil
	}
	return job.decision.Ratified, nil
}

// authorizeBoundaryEra decides the era a validated boundary moves into and
// refuses a block that is not encoded in it, as the hard-fork combinator does
// with HardForkLedgerErrorWrongEra, in either direction: a block in an era the
// ledger has not entered and one in an era the ledger has left are both
// wrong.
//
// bodyEraID is the era the block body is encoded in and targetEraID the era
// boundaryEraForBlock chose, which may sit one era past the body when the
// header protocol major elevates it. A header major is not an era signal:
// cardano-node stamps its own protocol version, and outside mainnet the header
// major is not bounded below major 12. So an elevation past the ledger's era
// is capped at that era, while a body that only the elevation brings up to the
// ledger's era is the validated predecessor-era pair a boundary may carry.
//
// It returns the era to transition into. An undecided ledger leaves
// targetEraID unchanged.
func (ls *LedgerState) authorizeBoundaryEra(
	ctx context.Context,
	in boundaryEraInputs,
	bodyEraID, targetEraID uint,
) (uint, error) {
	ledgerEraID, decided, err := ls.ledgerBoundaryEra(ctx, in)
	if err != nil {
		return 0, fmt.Errorf("decide boundary era: %w", err)
	}
	if !decided {
		return targetEraID, nil
	}
	eraList := ls.eraList()
	indexOf := func(id uint) int {
		return slices.IndexFunc(eraList, func(e eras.EraDesc) bool {
			return e.Id == id
		})
	}
	ledgerIndex := indexOf(ledgerEraID)
	bodyIndex := indexOf(bodyEraID)
	targetIndex := indexOf(targetEraID)
	if ledgerIndex < 0 || bodyIndex < 0 || targetIndex < 0 {
		return targetEraID, nil
	}
	if bodyIndex > ledgerIndex || targetIndex < ledgerIndex {
		return 0, fmt.Errorf(
			"%w: epoch %d block is in era %d but the ledger enters era %d",
			errBoundaryEraNotAuthorized,
			in.newEpochID,
			bodyEraID,
			ledgerEraID,
		)
	}
	return ledgerEraID, nil
}
