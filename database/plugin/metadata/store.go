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
	// Database management methods

	// Close closes the metadata store and releases all resources.
	Close() error

	// GetCommitTimestamp retrieves the last commit timestamp from the database.
	GetCommitTimestamp() (int64, error)

	// SetCommitTimestamp sets the last commit timestamp in the database.
	// Parameter order is (timestamp, txn) to match other store methods where
	// the transaction is the final parameter.
	SetCommitTimestamp(int64, types.Txn) error

	// Transaction creates a new metadata transaction.
	Transaction() types.Txn

	// Ledger state methods

	// AddUtxos adds one or more unspent transaction outputs to the database.
	AddUtxos(
		[]models.UtxoSlot,
		types.Txn,
	) error

	// GetPoolRegistrations retrieves all registration certificates for a pool.
	GetPoolRegistrations(
		lcommon.PoolKeyHash,
		types.Txn,
	) ([]lcommon.PoolRegistrationCertificate, error)

	// GetPool retrieves a pool by its key hash, optionally including inactive pools.
	GetPool(
		lcommon.PoolKeyHash,
		bool, // includeInactive
		types.Txn,
	) (*models.Pool, error)

	// GetActivePoolRelays retrieves all relays from currently active pools.
	// This is used for ledger peer discovery.
	GetActivePoolRelays(types.Txn) ([]models.PoolRegistrationRelay, error)

	// GetStakeRegistrations retrieves all stake registration certificates for an account.
	GetStakeRegistrations(
		[]byte, // stakeKey
		types.Txn,
	) ([]lcommon.StakeRegistrationCertificate, error)

	// GetTip retrieves the current chain tip.
	GetTip(types.Txn) (ochainsync.Tip, error)

	// GetAccount retrieves an account by stake key, optionally including inactive accounts.
	GetAccount(
		[]byte, // stakeKey
		bool, // includeInactive
		types.Txn,
	) (*models.Account, error)

	// GetBlockNonce retrieves a block nonce for a given point.
	GetBlockNonce(
		ocommon.Point,
		types.Txn,
	) ([]byte, error)

	// GetDatum retrieves a datum by its hash, returning nil if not found.
	GetDatum(
		lcommon.Blake2b256,
		types.Txn,
	) (*models.Datum, error)

	// GetDrep retrieves a DRep by its credential, optionally including inactive DReps.
	GetDrep(
		[]byte, // credential
		bool, // includeInactive
		types.Txn,
	) (*models.Drep, error)

	// GetPParams retrieves protocol parameters for a given epoch.
	GetPParams(
		uint64, // epoch
		types.Txn,
	) ([]models.PParams, error)

	// GetPParamUpdates retrieves protocol parameter updates for a given epoch.
	GetPParamUpdates(
		uint64, // epoch
		types.Txn,
	) ([]models.PParamUpdate, error)

	// GetUtxo retrieves an unspent transaction output by transaction ID and index.
	GetUtxo(
		[]byte, // txId
		uint32, // idx
		types.Txn,
	) (*models.Utxo, error)

	// GetTransactionByHash retrieves a transaction by its hash.
	GetTransactionByHash(
		[]byte, // hash
		types.Txn,
	) (*models.Transaction, error)

	// GetScript retrieves a script by its hash.
	GetScript(
		lcommon.ScriptHash,
		types.Txn,
	) (*models.Script, error)

	// SetBlockNonce stores a block nonce for a given block hash and slot.
	SetBlockNonce(
		[]byte, // blockHash
		uint64, // slotNumber
		[]byte, // nonce
		bool, // isCheckpoint
		types.Txn,
	) error

	// SetDatum stores a datum with its hash and slot.
	SetDatum(
		lcommon.Blake2b256,
		[]byte,
		uint64, // slot
		types.Txn,
	) error

	// SetEpoch sets epoch information.
	SetEpoch(
		uint64, // slot
		uint64, // epoch
		[]byte, // nonce
		uint, // era
		uint, // slotLength
		uint, // lengthInSlots
		types.Txn,
	) error

	// SetPParams stores protocol parameters.
	SetPParams(
		[]byte, // params
		uint64, // slot
		uint64, // epoch
		uint, // eraId
		types.Txn,
	) error

	// SetPParamUpdate stores a protocol parameter update.
	SetPParamUpdate(
		[]byte, // genesis
		[]byte, // update
		uint64, // slot
		uint64, // epoch
		types.Txn,
	) error

	// SetTip sets the current chain tip.
	SetTip(
		ochainsync.Tip,
		types.Txn,
	) error

	// SetTransaction stores a transaction with its metadata.
	SetTransaction(
		lcommon.Transaction,
		ocommon.Point,
		uint32, // idx
		map[int]uint64, // certDeposits: indexed by certificate position in tx.Certificates(); absent keys are treated as zero/no deposit
		types.Txn,
	) error

	// Helper methods

	// DeleteBlockNoncesBeforeSlot removes block nonces older than the given slot.
	DeleteBlockNoncesBeforeSlot(uint64, types.Txn) error

	// DeleteBlockNoncesBeforeSlotWithoutCheckpoints removes block nonces older than the given slot,
	// excluding checkpoint nonces.
	DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		uint64,
		types.Txn,
	) error

	// DeleteUtxo removes a single unspent transaction output.
	DeleteUtxo(models.UtxoId, types.Txn) error

	// DeleteUtxos removes multiple unspent transaction outputs.
	DeleteUtxos([]models.UtxoId, types.Txn) error

	// DeleteUtxosAfterSlot removes all UTxOs created after the given slot.
	DeleteUtxosAfterSlot(uint64, types.Txn) error

	// GetEpochsByEra retrieves all epochs for a given era.
	GetEpochsByEra(uint, types.Txn) ([]models.Epoch, error)

	// GetEpochs retrieves all epochs.
	GetEpochs(types.Txn) ([]models.Epoch, error)

	// GetUtxosAddedAfterSlot retrieves all UTxOs added after the given slot.
	GetUtxosAddedAfterSlot(uint64, types.Txn) ([]models.Utxo, error)

	// GetUtxosByAddress retrieves all UTxOs for a given address.
	GetUtxosByAddress(ledger.Address, types.Txn) ([]models.Utxo, error)

	// GetUtxosDeletedBeforeSlot retrieves UTxOs deleted before the given slot, up to the specified limit.
	GetUtxosDeletedBeforeSlot(
		uint64,
		int,
		types.Txn,
	) ([]models.Utxo, error)

	// SetUtxoDeletedAtSlot marks a UTxO as deleted at the given slot.
	SetUtxoDeletedAtSlot(
		ledger.TransactionInput,
		uint64,
		types.Txn,
	) error

	// SetUtxosNotDeletedAfterSlot marks all UTxOs created after the given slot as not deleted.
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
