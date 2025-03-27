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
		if err != nil {
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
		if err != nil {
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
