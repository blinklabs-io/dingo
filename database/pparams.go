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

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func (d *Database) GetPParams(
	epoch uint64,
	decodeFunc func([]byte) (lcommon.ProtocolParameters, error),
	txn *Txn,
) (lcommon.ProtocolParameters, error) {
	var ret lcommon.ProtocolParameters
	var err error
	if txn == nil {
		pparams, ppErr := d.metadata.GetPParams(epoch, nil)
		if ppErr != nil {
			return ret, ppErr
		}
		if len(pparams) == 0 {
			return ret, nil
		}
		// pparams is ordered, so grab the first
		tmpPParams := pparams[0]
		ret, err = decodeFunc(tmpPParams.Cbor)
	} else {
		pparams, ppErr := d.metadata.GetPParams(epoch, txn.Metadata())
		if ppErr != nil {
			return ret, ppErr
		}
		if len(pparams) == 0 {
			return ret, nil
		}
		// pparams is ordered, so grab the first
		tmpPParams := pparams[0]
		ret, err = decodeFunc(tmpPParams.Cbor)
	}
	return ret, err
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
	currentPParams *lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
	txn *Txn,
) error {
	// Check for pparam updates that apply at the end of the epoch
	pparamUpdates, err := d.metadata.GetPParamUpdates(epoch, txn.Metadata())
	if err != nil {
		return err
	}
	if len(pparamUpdates) == 0 {
		// nothing to do
		return nil
	}
	// We only want the latest for the epoch
	pparamUpdate := pparamUpdates[0]
	tmpPParamUpdate, err := decodeFunc(pparamUpdate.Cbor)
	if err != nil {
		return err
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
		return err
	}
	*currentPParams = newPParams
	d.logger.Debug(
		"updated protocol params",
		"pparams",
		fmt.Sprintf("%#v", currentPParams),
	)
	// Write pparams update to DB
	pparamsCbor, err := cbor.Encode(&currentPParams)
	if err != nil {
		return err
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
// This function takes currentPParams as a value and returns the updated parameters
// without mutating the input. This allows callers to capture the result in a
// transaction and apply it to in-memory state after the transaction commits.
func (d *Database) ComputeAndApplyPParamUpdates(
	slot, epoch uint64,
	era uint,
	currentPParams lcommon.ProtocolParameters,
	decodeFunc func([]byte) (any, error),
	updateFunc func(
		lcommon.ProtocolParameters,
		any,
	) (lcommon.ProtocolParameters, error),
	txn *Txn,
) (lcommon.ProtocolParameters, error) {
	// Check for pparam updates that apply at the end of the epoch
	pparamUpdates, err := d.metadata.GetPParamUpdates(epoch, txn.Metadata())
	if err != nil {
		return nil, err
	}
	if len(pparamUpdates) == 0 {
		// nothing to do, return current params unchanged
		return currentPParams, nil
	}
	// We only want the latest for the epoch
	pparamUpdate := pparamUpdates[0]
	tmpPParamUpdate, err := decodeFunc(pparamUpdate.Cbor)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	d.logger.Debug(
		"computed updated protocol params",
		"pparams",
		fmt.Sprintf("%#v", newPParams),
	)
	// Write pparams update to DB
	pparamsCbor, err := cbor.Encode(&newPParams)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	return newPParams, nil
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
