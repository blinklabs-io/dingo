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

package metadata

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type MetadataStore interface {
	// Database
	Close() error
	GetCommitTimestamp() (int64, error)
	SetCommitTimestamp(types.Txn, int64) error
	Transaction() types.Txn

	// Ledger state
	AddUtxos(
		[]models.UtxoSlot,
		types.Txn,
	) error
	GetPoolRegistrations(
		lcommon.PoolKeyHash,
		types.Txn,
	) ([]lcommon.PoolRegistrationCertificate, error)
	GetPool(
		lcommon.PoolKeyHash,
		bool, // includeInactive
		types.Txn,
	) (*models.Pool, error)
	GetStakeRegistrations(
		[]byte, // stakeKey
		types.Txn,
	) ([]lcommon.StakeRegistrationCertificate, error)
	GetTip(types.Txn) (ochainsync.Tip, error)

	GetAccount(
		[]byte, // stakeKey
		bool, // includeInactive
		types.Txn,
	) (*models.Account, error)
	GetBlockNonce(
		ocommon.Point,
		types.Txn,
	) ([]byte, error)
	GetDatum(
		lcommon.Blake2b256,
		types.Txn,
	) (*models.Datum, error)
	GetDrep(
		[]byte, // credential
		bool, // includeInactive
		types.Txn,
	) (*models.Drep, error)
	GetPParams(
		uint64, // epoch
		types.Txn,
	) ([]models.PParams, error)
	GetPParamUpdates(
		uint64, // epoch
		types.Txn,
	) ([]models.PParamUpdate, error)
	GetUtxo(
		[]byte, // txId
		uint32, // idx
		types.Txn,
	) (*models.Utxo, error)
	GetTransactionByHash(
		[]byte, // hash
		types.Txn,
	) (*models.Transaction, error)
	GetScript(
		lcommon.ScriptHash,
		types.Txn,
	) (*models.Script, error)

	SetBlockNonce(
		[]byte, // blockHash
		uint64, // slotNumber
		[]byte, // nonce
		bool, // isCheckpoint
		types.Txn,
	) error
	SetDatum(
		lcommon.Blake2b256,
		[]byte,
		uint64, // slot
		types.Txn,
	) error
	SetEpoch(
		uint64, // slot
		uint64, // epoch
		[]byte, // nonce
		uint, // era
		uint, // slotLength
		uint, // lengthInSlots
		types.Txn,
	) error
	SetPParams(
		[]byte, // params
		uint64, // slot
		uint64, // epoch
		uint, // eraId
		types.Txn,
	) error
	SetPParamUpdate(
		[]byte, // genesis
		[]byte, // update
		uint64, // slot
		uint64, // epoch
		types.Txn,
	) error
	SetTip(
		ochainsync.Tip,
		types.Txn,
	) error
	SetTransaction(
		lcommon.Transaction,
		ocommon.Point,
		uint32, // idx
		map[int]uint64, // certDeposits: indexed by certificate position in tx.Certificates(); absent keys are treated as zero/no deposit
		types.Txn,
	) error

	// Helpers
	DeleteBlockNoncesBeforeSlot(uint64, types.Txn) error
	DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		uint64,
		types.Txn,
	) error
	DeleteUtxo(any, types.Txn) error
	DeleteUtxos([]any, types.Txn) error
	DeleteUtxosAfterSlot(uint64, types.Txn) error
	GetEpochsByEra(uint, types.Txn) ([]models.Epoch, error)
	GetEpochs(types.Txn) ([]models.Epoch, error)
	GetUtxosAddedAfterSlot(uint64, types.Txn) ([]models.Utxo, error)
	GetUtxosByAddress(ledger.Address, types.Txn) ([]models.Utxo, error)
	GetUtxosDeletedBeforeSlot(
		uint64,
		int,
		types.Txn,
	) ([]models.Utxo, error)
	SetUtxoDeletedAtSlot(
		ledger.TransactionInput,
		uint64,
		types.Txn,
	) error
	SetUtxosNotDeletedAfterSlot(uint64, types.Txn) error
}

// New creates a new metadata store instance using the specified plugin
func New(pluginName string) (MetadataStore, error) {
	// Get and start the plugin
	p, err := plugin.StartPlugin(plugin.PluginTypeMetadata, pluginName)
	if err != nil {
		return nil, err
	}

	// Type assert to MetadataStore interface
	metadataStore, ok := p.(MetadataStore)
	if !ok {
		return nil, fmt.Errorf(
			"plugin '%s' does not implement MetadataStore interface",
			pluginName,
		)
	}

	return metadataStore, nil
}
