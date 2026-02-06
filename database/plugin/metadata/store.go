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

	// GetActivePoolKeyHashes retrieves the key hashes of all currently active pools.
	// A pool is active if it has a registration and either no retirement or
	// the retirement epoch is in the future.
	GetActivePoolKeyHashes(types.Txn) ([][]byte, error)

	// GetActivePoolKeyHashesAtSlot retrieves the key hashes of pools that were
	// active at the given slot. A pool was active at a slot if:
	// 1. It had a registration with added_slot <= slot
	// 2. Either:
	//    a. No retirement with added_slot <= slot, OR
	//    b. The most recent retirement was for an epoch that hadn't started yet, OR
	//    c. A registration occurred AFTER the most recent retirement (re-registration
	//       cancels a pending retirement)
	//
	// When determining order of events in the same slot, block_index (transaction
	// index within block) and cert_index (certificate index within transaction)
	// are used as tie-breakers since cert_index resets per transaction. The full
	// ordering is: added_slot DESC, block_index DESC, cert_index DESC.
	// This handles cases where registration and retirement occur in different
	// transactions within the same block.
	//
	// This is used for stake snapshot calculations at historical points.
	//
	// Returns types.ErrNoEpochData (wrapped) if epoch data has not been synced
	// for the requested slot. Callers should use errors.Is() to check.
	GetActivePoolKeyHashesAtSlot(uint64, types.Txn) ([][]byte, error)

	// GetStakeByPool returns the total delegated stake and delegator count for a pool.
	// This aggregates all accounts delegated to the pool and sums their UTxO values.
	GetStakeByPool(
		[]byte, // poolKeyHash
		types.Txn,
	) (uint64, uint64, error) // (totalStake, delegatorCount, error)

	// GetStakeByPools returns delegated stake for multiple pools in a single query.
	// Returns maps of pool key hash -> total stake and pool key hash -> delegator count.
	GetStakeByPools(
		[][]byte, // poolKeyHashes
		types.Txn,
	) (map[string]uint64, map[string]uint64, error)

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

	// GetActiveDreps retrieves all active DReps.
	GetActiveDreps(types.Txn) ([]*models.Drep, error)

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

	// SetGenesisTransaction stores a genesis transaction record.
	// Genesis transactions have no inputs, witnesses, or fees - just outputs.
	SetGenesisTransaction(
		hash []byte,
		blockHash []byte,
		outputs []models.Utxo,
		txn types.Txn,
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

	// GetEpoch retrieves a single epoch by its ID.
	// Returns nil if the epoch is not found.
	GetEpoch(uint64, types.Txn) (*models.Epoch, error)

	// GetEpochs retrieves all epochs.
	GetEpochs(types.Txn) ([]models.Epoch, error)

	// GetUtxosAddedAfterSlot retrieves all UTxOs added after the given slot.
	GetUtxosAddedAfterSlot(uint64, types.Txn) ([]models.Utxo, error)

	// GetUtxosByAddress retrieves all UTxOs for a given address.
	GetUtxosByAddress(ledger.Address, types.Txn) ([]models.Utxo, error)

	// GetUtxosByAssets retrieves all UTxOs that contain the specified assets.
	// Pass nil for assetName to match all assets under the policy, or empty []byte{} to match assets with empty names.
	GetUtxosByAssets(
		policyId []byte,
		assetName []byte,
		txn types.Txn,
	) ([]models.Utxo, error)

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

	// Stake snapshot methods

	// SavePoolStakeSnapshot saves a single pool stake snapshot.
	SavePoolStakeSnapshot(
		*models.PoolStakeSnapshot,
		types.Txn,
	) error

	// SavePoolStakeSnapshots saves multiple pool stake snapshots in batch.
	SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot,
		types.Txn,
	) error

	// GetPoolStakeSnapshot retrieves a specific pool's stake snapshot for an epoch.
	GetPoolStakeSnapshot(
		uint64, // epoch
		string, // snapshotType ("mark", "set", or "go")
		[]byte, // poolKeyHash
		types.Txn,
	) (*models.PoolStakeSnapshot, error)

	// GetPoolStakeSnapshotsByEpoch retrieves all pool stake snapshots for an epoch.
	GetPoolStakeSnapshotsByEpoch(
		uint64, // epoch
		string, // snapshotType
		types.Txn,
	) ([]*models.PoolStakeSnapshot, error)

	// GetTotalActiveStake returns the sum of all pool stakes for an epoch.
	GetTotalActiveStake(
		uint64, // epoch
		string, // snapshotType
		types.Txn,
	) (uint64, error)

	// SaveEpochSummary saves an epoch summary.
	SaveEpochSummary(
		*models.EpochSummary,
		types.Txn,
	) error

	// GetEpochSummary retrieves the summary for a specific epoch.
	GetEpochSummary(
		uint64, // epoch
		types.Txn,
	) (*models.EpochSummary, error)

	// GetLatestEpochSummary retrieves the most recent epoch summary.
	GetLatestEpochSummary(types.Txn) (*models.EpochSummary, error)

	// DeletePoolStakeSnapshotsForEpoch deletes snapshots for a specific epoch and type.
	DeletePoolStakeSnapshotsForEpoch(
		uint64, // epoch
		string, // snapshotType
		types.Txn,
	) error

	// DeletePoolStakeSnapshotsAfterEpoch deletes all snapshots after a given epoch.
	DeletePoolStakeSnapshotsAfterEpoch(uint64, types.Txn) error

	// DeletePoolStakeSnapshotsBeforeEpoch deletes all snapshots before a given epoch.
	DeletePoolStakeSnapshotsBeforeEpoch(uint64, types.Txn) error

	// DeleteEpochSummariesAfterEpoch deletes all epoch summaries after a given epoch.
	DeleteEpochSummariesAfterEpoch(uint64, types.Txn) error

	// DeleteEpochSummariesBeforeEpoch deletes all epoch summaries before a given epoch.
	DeleteEpochSummariesBeforeEpoch(uint64, types.Txn) error

	// GetTransactionHashesAfterSlot returns transaction hashes for transactions added after the given slot.
	// This is used for blob cleanup during rollback/truncation.
	GetTransactionHashesAfterSlot(uint64, types.Txn) ([][]byte, error)

	// DeleteTransactionsAfterSlot removes transaction records added after the given slot.
	// Child records are automatically removed via CASCADE constraints.
	DeleteTransactionsAfterSlot(uint64, types.Txn) error

	// Governance methods

	// GetGovernanceProposal retrieves a governance proposal by transaction hash and action index.
	GetGovernanceProposal(
		[]byte, // txHash
		uint32, // actionIndex
		types.Txn,
	) (*models.GovernanceProposal, error)

	// GetActiveGovernanceProposals retrieves all governance proposals that haven't expired.
	GetActiveGovernanceProposals(
		uint64, // epoch
		types.Txn,
	) ([]*models.GovernanceProposal, error)

	// SetGovernanceProposal creates or updates a governance proposal.
	SetGovernanceProposal(
		*models.GovernanceProposal,
		types.Txn,
	) error

	// GetGovernanceVotes retrieves all votes for a governance proposal.
	GetGovernanceVotes(
		uint, // proposalID
		types.Txn,
	) ([]*models.GovernanceVote, error)

	// SetGovernanceVote records a vote on a governance proposal.
	SetGovernanceVote(
		*models.GovernanceVote,
		types.Txn,
	) error

	// Committee methods

	// GetCommitteeMember retrieves a committee member by cold key.
	GetCommitteeMember(
		[]byte, // coldKey
		types.Txn,
	) (*models.AuthCommitteeHot, error)

	// GetActiveCommitteeMembers retrieves all active committee members.
	GetActiveCommitteeMembers(types.Txn) ([]*models.AuthCommitteeHot, error)

	// IsCommitteeMemberResigned checks if a committee member has resigned.
	IsCommitteeMemberResigned(
		[]byte, // coldKey
		types.Txn,
	) (bool, error)

	// Constitution methods

	// GetConstitution retrieves the current constitution.
	GetConstitution(types.Txn) (*models.Constitution, error)

	// SetConstitution sets the constitution.
	SetConstitution(
		*models.Constitution,
		types.Txn,
	) error

	// DeleteConstitutionsAfterSlot removes constitutions added after the given slot
	// and clears deleted_slot for any that were soft-deleted after that slot.
	// This is used during chain rollbacks.
	DeleteConstitutionsAfterSlot(uint64, types.Txn) error

	// Governance rollback methods

	// DeleteGovernanceProposalsAfterSlot removes proposals added after the given slot
	// and clears deleted_slot for any that were soft-deleted after that slot.
	DeleteGovernanceProposalsAfterSlot(uint64, types.Txn) error

	// DeleteGovernanceVotesAfterSlot removes votes added after the given slot
	// and clears deleted_slot for any that were soft-deleted after that slot.
	DeleteGovernanceVotesAfterSlot(uint64, types.Txn) error
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
