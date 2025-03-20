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

package state

import (
	"github.com/blinklabs-io/dingo/database"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type LedgerView struct {
	ls  *LedgerState
	txn *database.Txn
}

func (lv *LedgerView) NetworkId() uint {
	genesis := lv.ls.config.CardanoNodeConfig.ShelleyGenesis()
	if genesis == nil {
		// no config, definitely not mainnet
		return 0
	}
	networkName := genesis.NetworkId
	if networkName == "Mainnet" {
		return 1
	}
	return 0
}

func (lv *LedgerView) UtxoById(
	utxoId lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	utxo, err := lv.ls.db.(*database.BaseDatabase).UtxoByRef(
		utxoId.Id().Bytes(),
		utxoId.Index(),
		lv.txn,
	)
	if err != nil {
		return lcommon.Utxo{}, err
	}
	tmpOutput, err := utxo.Decode()
	if err != nil {
		return lcommon.Utxo{}, err
	}
	return lcommon.Utxo{
		Id:     utxoId,
		Output: tmpOutput,
	}, nil
}

func (lv *LedgerView) StakeRegistration(
	stakingKey []byte,
) ([]lcommon.StakeRegistrationCertificate, error) {
	ret := []lcommon.StakeRegistrationCertificate{}
	certs, err := lv.txn.DB().
		Metadata().
		GetStakeRegistrations(stakingKey, lv.txn.Metadata())
	if err != nil {
		return ret, err
	}
	for _, cert := range certs {
		ret = append(
			ret,
			lcommon.StakeRegistrationCertificate{
				StakeRegistration: lcommon.StakeCredential{
					Credential: cert.StakingKey,
				},
			},
		)
	}
	return ret, nil
}

func (lv *LedgerView) PoolRegistration(
	poolKeyHash []byte,
) ([]lcommon.PoolRegistrationCertificate, error) {
	ret := []lcommon.PoolRegistrationCertificate{}
	certs, err := lv.txn.DB().
		Metadata().
		GetPoolRegistrations(poolKeyHash, lv.txn.Metadata())
	if err != nil {
		return ret, err
	}
	for _, cert := range certs {
		ret = append(
			ret,
			lcommon.PoolRegistrationCertificate{
				Operator: lcommon.PoolKeyHash(
					lcommon.NewBlake2b224(cert.PoolKeyHash),
				),
			},
		)
	}
	return ret, nil
}
