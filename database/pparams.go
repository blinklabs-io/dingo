// Copyright 2025 Blink Labs Software
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

package database

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// GetPParams resolves the protocol-parameters row at epoch <= the
// supplied epoch whose era_id matches eraId, then decodes it with
// decodeFunc. The era filter is required because at era boundaries
// the rollover path writes both an old-era row (post-pparams-update)
// and a new-era row (transitionToEra) at the same epoch — without
// the filter, the latest insert wins regardless of shape and the
// caller's era-specific decoder rejects the CBOR on element count.
func (d *Database) GetPParams(
	epoch uint64,
	eraId uint,
	decodeFunc func([]byte) (lcommon.ProtocolParameters, error),
	txn *Txn,
) (lcommon.ProtocolParameters, error) {
	var (
		pparams []models.PParams
		ppErr   error
	)
	if txn == nil {
		pparams, ppErr = d.metadata.GetPParams(epoch, eraId, nil)
	} else {
		pparams, ppErr = d.metadata.GetPParams(
			epoch, eraId, txn.Metadata(),
		)
	}
	if ppErr != nil {
		return nil, ppErr
	}
	if len(pparams) == 0 {
		return nil, nil
	}
	return decodeFunc(pparams[0].Cbor)
}

func (d *Database) SetPParams(
	params []byte,
	slot, epoch uint64,
	era uint,
	txn *Txn,
) error {
	if txn == nil {
		err := d.metadata.SetPParams(params, slot, epoch, era, nil)
		if err != nil {
			return err
		}
	} else {
		err := d.metadata.SetPParams(params, slot, epoch, era, txn.Metadata())
		if err != nil {
			return err
		}
	}
	return nil
}

// selectPParamUpdateForEnactment picks the proposed protocol-parameter update
// to enact as the parameters for the boundary INTO enactEpoch, and reports
// whether quorum was met.
//
// Per the Shelley update system (and cardano-ledger), a proposal carries its
// SUBMISSION epoch e in the stored `epoch` field and is enacted as epoch e+1's
// parameters. So the update enacted for enactEpoch is the one submitted in
// enactEpoch-1; callers fetch and filter by that submission epoch. Epoch 0 has
// no prior epoch, so nothing can be enacted for it.
//
// Quorum is the number of DISTINCT genesis-key delegates among the matching
// proposals; the update applied is the most recent one (rows arrive ordered
// id DESC, so the first match is newest). Returns (nil, count, false) when
// enactEpoch is 0 or quorum is not met.
func selectPParamUpdateForEnactment(
	rows []models.PParamUpdate,
	enactEpoch uint64,
	quorum int,
) (*models.PParamUpdate, int, bool) {
	if enactEpoch == 0 {
		return nil, 0, false
	}
	submissionEpoch := enactEpoch - 1
	uniqueGenesis := make(map[string]struct{})
	var latest *models.PParamUpdate
	for i := range rows {
		if rows[i].Epoch != submissionEpoch {
			continue
		}
		uniqueGenesis[string(rows[i].GenesisHash)] = struct{}{}
		if latest == nil {
			latest = &rows[i]
		}
	}
	if latest == nil || len(uniqueGenesis) < quorum {
		return nil, len(uniqueGenesis), false
	}
	return latest, len(uniqueGenesis), true
}

// ApplyPParamUpdates enacts, for the boundary INTO epoch, the pending pparam
// update submitted in epoch-1 (see selectPParamUpdateForEnactment for the
// submission-epoch semantics), mutating *currentPParams and persisting the
// result for epoch.
func (d *Database) ApplyPParamUpdates(
	slot, epoch uint64,
	era uint,
	quorum int,
	currentPParams *lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
	txn *Txn,
) error {
	if txn == nil {
		tmpTxn := d.MetadataTxn(true)
		defer tmpTxn.Release()
		if err := d.ApplyPParamUpdates(
			slot, epoch, era, quorum, currentPParams,
			decodeFunc, updateFunc, tmpTxn,
		); err != nil {
			return err
		}
		if err := tmpTxn.Commit(); err != nil {
			return fmt.Errorf("commit pparams update: %w", err)
		}
		return nil
	}
	if epoch == 0 {
		// No prior (submission) epoch, so nothing to enact.
		return nil
	}
	// Fetch proposals submitted in the prior epoch; they are what gets enacted
	// as epoch's parameters.
	submissionEpoch := epoch - 1
	pparamUpdates, err := d.metadata.GetPParamUpdates(
		submissionEpoch, txn.Metadata(),
	)
	if err != nil {
		return fmt.Errorf(
			"get pparam updates for epoch %d: %w", submissionEpoch, err,
		)
	}
	latestUpdate, uniqueCount, ok := selectPParamUpdateForEnactment(
		pparamUpdates, epoch, quorum,
	)
	if !ok {
		d.logger.Debug(
			"pparam update quorum not met or none pending, skipping",
			"enact_epoch", epoch,
			"submission_epoch", submissionEpoch,
			"uniqueProposals", uniqueCount,
			"quorum", quorum,
		)
		return nil
	}
	tmpPParamUpdate, err := decodeFunc(latestUpdate.Cbor)
	if err != nil {
		return fmt.Errorf("decode pparam update: %w", err)
	}
	// Update current pparams
	if *currentPParams == nil {
		return fmt.Errorf(
			"current PParams is nil - cannot apply protocol parameter updates for epoch %d",
			epoch,
		)
	}
	newPParams, err := updateFunc(
		*currentPParams,
		tmpPParamUpdate,
	)
	if err != nil {
		return fmt.Errorf("apply pparam update: %w", err)
	}
	*currentPParams = newPParams
	d.logger.Debug(
		"updated protocol params",
		"enact_epoch", epoch,
		"submission_epoch", submissionEpoch,
		"uniqueProposals", uniqueCount,
		"quorum", quorum,
		"pparams", fmt.Sprintf("%#v", currentPParams),
	)
	// Write pparams update to DB
	pparamsCbor, err := cbor.Encode(*currentPParams)
	if err != nil {
		return fmt.Errorf("encode updated pparams: %w", err)
	}
	// Store params for the target epoch (epoch) where they take effect
	return d.metadata.SetPParams(
		pparamsCbor,
		slot,
		epoch,
		era,
		txn.Metadata(),
	)
}

// ComputeAndApplyPParamUpdates computes the new protocol parameters by applying
// the pending update to enact for the given epoch, and persists the result for
// that epoch. The epoch parameter is the epoch where the updates take effect
// (currentEpoch + 1 during epoch rollover); per the Shelley update system the
// enacted proposal is the one submitted in epoch-1 (see
// selectPParamUpdateForEnactment). The quorum parameter is the minimum number
// of unique genesis-key delegates that must have submitted proposals (from
// shelley-genesis.json updateQuorum).
// Although the interface is passed by value, era-specific update functions may
// mutate its underlying concrete protocol-parameter pointer in place. Callers
// that need the original value preserved must pass an independently owned copy;
// the returned value is the authoritative updated parameter set.
//
// hasPlutusV2CostModelFunc reports whether the enacted update itself (not the
// merged result) explicitly specifies a PlutusV2 cost model (map key 1). This
// is the pre-Conway equivalent of governance.EnactmentResult's
// PlutusV2CostModelWritten: on a network that forks into Babbage before
// receiving a real PlutusV2 cost model, that model can arrive through this
// classic Shelley-style update system (as it did on real mainnet, well before
// CIP-1694 governance existed), and the caller needs the same real-write
// provenance signal here that governance.EnactProposal provides for the
// Conway/Dijkstra path -- comparing the merged result's value before and
// after is unsound for the same reason it is there: HardForkBabbage's
// synthetic default is the real, canonical mainnet value, so a real update
// writing that exact value would otherwise look unchanged. See
// blinklabs-io/dingo#3825's PR review. May be nil (no signal available for
// this era, e.g. Byron), in which case the returned bool is always false.
func (d *Database) ComputeAndApplyPParamUpdates(
	slot, epoch uint64,
	era uint,
	quorum int,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(
		lcommon.ProtocolParameters,
		any,
	) (lcommon.ProtocolParameters, error),
	hasPlutusV2CostModelFunc func(any) bool,
	txn *Txn,
) (lcommon.ProtocolParameters, bool, error) {
	if txn == nil {
		tmpTxn := d.MetadataTxn(true)
		defer tmpTxn.Release()
		result, plutusV2CostModelWritten, err := d.ComputeAndApplyPParamUpdates(
			slot, epoch, era, quorum, currentPParams,
			decodeFunc, updateFunc, hasPlutusV2CostModelFunc, tmpTxn,
		)
		if err != nil {
			return nil, false, err
		}
		if err := tmpTxn.Commit(); err != nil {
			return nil, false, fmt.Errorf("commit pparams update: %w", err)
		}
		return result, plutusV2CostModelWritten, nil
	}
	if epoch == 0 {
		// No prior (submission) epoch, so nothing to enact.
		return currentPParams, false, nil
	}
	// Fetch proposals submitted in the prior epoch; they are what gets enacted
	// as epoch's parameters.
	submissionEpoch := epoch - 1
	pparamUpdates, err := d.metadata.GetPParamUpdates(
		submissionEpoch, txn.Metadata(),
	)
	if err != nil {
		return nil, false, fmt.Errorf(
			"get pparam updates for epoch %d: %w",
			submissionEpoch,
			err,
		)
	}
	latestUpdate, uniqueCount, ok := selectPParamUpdateForEnactment(
		pparamUpdates, epoch, quorum,
	)
	if !ok {
		d.logger.Debug(
			"pparam update quorum not met or none pending, skipping",
			"enact_epoch", epoch,
			"submission_epoch", submissionEpoch,
			"uniqueProposals", uniqueCount,
			"quorum", quorum,
		)
		return currentPParams, false, nil
	}
	tmpPParamUpdate, err := decodeFunc(latestUpdate.Cbor)
	if err != nil {
		return nil, false, fmt.Errorf("decode pparam update: %w", err)
	}
	// Compute updated pparams
	if currentPParams == nil {
		return nil, false, fmt.Errorf(
			"current PParams is nil - cannot apply protocol parameter updates for epoch %d",
			epoch,
		)
	}
	newPParams, err := updateFunc(
		currentPParams,
		tmpPParamUpdate,
	)
	if err != nil {
		return nil, false, fmt.Errorf("apply pparam update: %w", err)
	}
	d.logger.Debug(
		"computed updated protocol params",
		"enact_epoch", epoch,
		"submission_epoch", submissionEpoch,
		"uniqueProposals", uniqueCount,
		"quorum", quorum,
		"pparams", fmt.Sprintf("%#v", newPParams),
	)
	// Write pparams update to DB
	pparamsCbor, err := cbor.Encode(newPParams)
	if err != nil {
		return nil, false, fmt.Errorf("encode updated pparams: %w", err)
	}
	// Store params for the target epoch (epoch) where they take effect
	err = d.metadata.SetPParams(
		pparamsCbor,
		slot,
		epoch,
		era,
		txn.Metadata(),
	)
	if err != nil {
		return nil, false, fmt.Errorf("set pparams: %w", err)
	}
	plutusV2CostModelWritten := hasPlutusV2CostModelFunc != nil &&
		hasPlutusV2CostModelFunc(tmpPParamUpdate)
	return newPParams, plutusV2CostModelWritten, nil
}

// ForecastPParamUpdates computes the protocol parameters that the epoch
// rollover will enact for the given epoch by applying the pending proposed
// protocol-parameter update already collected in ledger state, WITHOUT
// persisting anything. It mirrors ComputeAndApplyPParamUpdates' quorum,
// decode, and apply semantics exactly — same submission-epoch lookup
// (updates submitted in epoch-1), same unique-genesis quorum count, same
// latest-update selection via selectPParamUpdateForEnactment — but performs
// no writes, so it is safe to call from header verification and concurrently.
//
// It does not mutate currentPParams: era update functions mutate their
// concrete pointer in place (see PParamsUpdateShelley), so before applying
// an update it clones currentPParams via cloneFunc and mutates the clone.
// The clone happens only when an update will actually be enacted, so the
// common no-op forecast pays no clone cost and returns the original
// currentPParams pointer. When no pending update meets quorum for the
// epoch — no proposals, quorum not met, or epoch is 0 — it returns
// currentPParams unchanged, matching the "nothing enacted" forecast.
func (d *Database) ForecastPParamUpdates(
	epoch uint64,
	quorum int,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(
		lcommon.ProtocolParameters,
		any,
	) (lcommon.ProtocolParameters, error),
	cloneFunc func(lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error),
	txn *Txn,
) (lcommon.ProtocolParameters, error) {
	if currentPParams == nil ||
		decodeFunc == nil ||
		updateFunc == nil ||
		cloneFunc == nil ||
		epoch == 0 {
		return currentPParams, nil
	}
	// Fetch proposals submitted in the prior epoch; they are what will be
	// enacted as epoch's parameters.
	submissionEpoch := epoch - 1
	var (
		pparamUpdates []models.PParamUpdate
		err           error
	)
	if txn == nil {
		pparamUpdates, err = d.metadata.GetPParamUpdates(submissionEpoch, nil)
	} else {
		pparamUpdates, err = d.metadata.GetPParamUpdates(
			submissionEpoch, txn.Metadata(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf(
			"get pparam updates for epoch %d: %w",
			submissionEpoch,
			err,
		)
	}
	latestUpdate, _, ok := selectPParamUpdateForEnactment(
		pparamUpdates, epoch, quorum,
	)
	if !ok {
		return currentPParams, nil
	}
	tmpPParamUpdate, err := decodeFunc(latestUpdate.Cbor)
	if err != nil {
		return nil, fmt.Errorf("decode pparam update: %w", err)
	}
	// Clone before applying: updateFunc mutates its concrete pointer in
	// place, and this forecast must not touch the caller's currentPParams.
	owned, err := cloneFunc(currentPParams)
	if err != nil {
		return nil, fmt.Errorf("clone pparams for forecast: %w", err)
	}
	if owned == nil {
		return currentPParams, nil
	}
	newPParams, err := updateFunc(owned, tmpPParamUpdate)
	if err != nil {
		return nil, fmt.Errorf("apply pparam update: %w", err)
	}
	return newPParams, nil
}

// DeletePParamsAfterSlot removes protocol parameter records added after
// the given slot.
func (d *Database) DeletePParamsAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.DeletePParamsAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete pparams after slot %d: %w",
			slot,
			err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return nil
}

// DeletePParamUpdatesAfterSlot removes protocol parameter update records
// added after the given slot.
func (d *Database) DeletePParamUpdatesAfterSlot(
	slot uint64,
	txn *Txn,
) error {
	owned := false
	if txn == nil {
		txn = d.MetadataTxn(true)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	if err := d.metadata.DeletePParamUpdatesAfterSlot(
		slot,
		txn.Metadata(),
	); err != nil {
		return fmt.Errorf(
			"failed to delete pparam updates after slot %d: %w",
			slot,
			err,
		)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}
		owned = false
	}
	return nil
}

func (d *Database) SetPParamUpdate(
	genesis, params []byte,
	slot, epoch uint64,
	txn *Txn,
) error {
	if txn == nil {
		err := d.metadata.SetPParamUpdate(genesis, params, slot, epoch, nil)
		if err != nil {
			return err
		}
	} else {
		err := d.metadata.SetPParamUpdate(genesis, params, slot, epoch, txn.Metadata())
		if err != nil {
			return err
		}
	}
	return nil
}
