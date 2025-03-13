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
	"github.com/blinklabs-io/dingo/state/models"
	"github.com/blinklabs-io/gouroboros/ledger/common"
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
	utxo, err := models.UtxoByRefTxn(
		lv.txn,
		utxoId.Id().Bytes(),
		utxoId.Index(),
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
	certs := []models.StakeRegistration{}
	result := lv.txn.Metadata().
		Where("staking_key = ?", stakingKey).
		Find(&certs)
	if result.Error != nil {
		return ret, result.Error
	}
	for _, cert := range certs {
		ret = append(
			ret,
			common.StakeRegistrationCertificate{
				StakeRegistration: common.StakeCredential{
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
	certs := []models.PoolRegistration{}
	result := lv.txn.Metadata().
		Where("pool_key_hash = ?", poolKeyHash).
		Find(&certs)
	if result.Error != nil {
		return ret, result.Error
	}
	for _, cert := range certs {
		ret = append(
			ret,
			common.PoolRegistrationCertificate{
				Operator: common.PoolKeyHash(
					common.NewBlake2b224(cert.PoolKeyHash),
				),
			},
		)
	}
	return ret, nil
}
