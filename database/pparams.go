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
	// Check for pparam updates that apply at the end of the epoch
	pparamUpdates, err := d.metadata.GetPParamUpdates(epoch, txn.Metadata())
	if err != nil {
		return fmt.Errorf("get pparam updates for epoch %d: %w", epoch, err)
	}
	if len(pparamUpdates) == 0 {
		// nothing to do
		return nil
	}
	// Filter to only updates targeting this specific epoch and count
	// unique genesis key delegates
	uniqueGenesis := make(map[string]struct{})
	var latestUpdate *models.PParamUpdate
	for i := range pparamUpdates {
		if pparamUpdates[i].Epoch != epoch {
			continue
		}
		genesisKey := string(pparamUpdates[i].GenesisHash)
		uniqueGenesis[genesisKey] = struct{}{}
		if latestUpdate == nil {
			latestUpdate = &pparamUpdates[i]
		}
	}
	// Check quorum: need at least 'quorum' unique genesis key delegates
	if len(uniqueGenesis) < quorum {
		d.logger.Debug(
			"pparam update quorum not met, skipping",
			"epoch", epoch,
			"uniqueProposals", len(uniqueGenesis),
			"quorum", quorum,
		)
		return nil
	}
	if latestUpdate == nil {
		// No updates for this specific epoch
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
		"epoch", epoch,
		"uniqueProposals", len(uniqueGenesis),
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
// pending updates for the given target epoch. The epoch parameter should be the
// epoch where updates take effect (currentEpoch + 1 during epoch rollover).
// The quorum parameter specifies the minimum number of unique genesis key
// delegates that must have submitted update proposals for the update to be
// applied (from shelley-genesis.json updateQuorum).
// Although the interface is passed by value, era-specific update functions may
// mutate its underlying concrete protocol-parameter pointer in place. Callers
// that need the original value preserved must pass an independently owned copy;
// the returned value is the authoritative updated parameter set.
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
	txn *Txn,
) (lcommon.ProtocolParameters, error) {
	if txn == nil {
		tmpTxn := d.MetadataTxn(true)
		defer tmpTxn.Release()
		result, err := d.ComputeAndApplyPParamUpdates(
			slot, epoch, era, quorum, currentPParams,
			decodeFunc, updateFunc, tmpTxn,
		)
		if err != nil {
			return nil, err
		}
		if err := tmpTxn.Commit(); err != nil {
			return nil, fmt.Errorf("commit pparams update: %w", err)
		}
		return result, nil
	}
	// Check for pparam updates that apply at the end of the epoch
	pparamUpdates, err := d.metadata.GetPParamUpdates(epoch, txn.Metadata())
	if err != nil {
		return nil, fmt.Errorf(
			"get pparam updates for epoch %d: %w",
			epoch,
			err,
		)
	}
	if len(pparamUpdates) == 0 {
		// nothing to do, return current params unchanged
		return currentPParams, nil
	}
	// Filter to only updates targeting this specific epoch and count
	// unique genesis key delegates
	uniqueGenesis := make(map[string]struct{})
	var latestUpdate *models.PParamUpdate
	for i := range pparamUpdates {
		if pparamUpdates[i].Epoch != epoch {
			continue
		}
		genesisKey := string(pparamUpdates[i].GenesisHash)
		uniqueGenesis[genesisKey] = struct{}{}
		if latestUpdate == nil {
			latestUpdate = &pparamUpdates[i]
		}
	}
	// Check quorum: need at least 'quorum' unique genesis key delegates
	if len(uniqueGenesis) < quorum {
		d.logger.Debug(
			"pparam update quorum not met, skipping",
			"epoch", epoch,
			"uniqueProposals", len(uniqueGenesis),
			"quorum", quorum,
		)
		return currentPParams, nil
	}
	if latestUpdate == nil {
		// No updates for this specific epoch
		return currentPParams, nil
	}
	tmpPParamUpdate, err := decodeFunc(latestUpdate.Cbor)
	if err != nil {
		return nil, fmt.Errorf("decode pparam update: %w", err)
	}
	// Compute updated pparams
	if currentPParams == nil {
		return nil, fmt.Errorf(
			"current PParams is nil - cannot apply protocol parameter updates for epoch %d",
			epoch,
		)
	}
	newPParams, err := updateFunc(
		currentPParams,
		tmpPParamUpdate,
	)
	if err != nil {
		return nil, fmt.Errorf("apply pparam update: %w", err)
	}
	d.logger.Debug(
		"computed updated protocol params",
		"epoch", epoch,
		"uniqueProposals", len(uniqueGenesis),
		"quorum", quorum,
		"pparams", fmt.Sprintf("%#v", newPParams),
	)
	// Write pparams update to DB
	pparamsCbor, err := cbor.Encode(newPParams)
	if err != nil {
		return nil, fmt.Errorf("encode updated pparams: %w", err)
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
		return nil, fmt.Errorf("set pparams: %w", err)
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
