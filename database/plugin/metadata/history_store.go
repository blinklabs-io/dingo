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
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// HistoryStore is the read surface behind the Blockfrost historical
// endpoints: slot-range aggregates and per-epoch distributions that a
// paginated API needs without materializing the full result set.
//
// A method that takes limit/offset reports the unpaginated total so the
// caller can set pagination headers from a single consistent read. Where a
// method takes order, it is "asc" or "desc"; any other value is treated as
// "asc". GetEpochStakes has a fixed order, as the endpoint it serves takes
// none.
type HistoryStore interface {
	// SumTransactionOutputsInSlotRange returns the total lovelace the
	// transactions in the inclusive slot range put on chain: the outputs of a
	// valid transaction, or the collateral return of an invalid one. Spent
	// outputs count only while the store retains them, which holds in API
	// storage mode.
	SumTransactionOutputsInSlotRange(
		startSlot uint64,
		endSlot uint64,
		txn types.Txn,
	) (uint64, error)

	// GetBlockHashesInSlotRange returns the hashes of canonical blocks in the
	// inclusive slot range ordered by slot, along with the unpaginated count.
	GetBlockHashesInSlotRange(
		startSlot uint64,
		endSlot uint64,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([][]byte, int, error)

	// GetPoolBlockHashesInSlotRange is GetBlockHashesInSlotRange restricted
	// to the blocks issued by one pool.
	GetPoolBlockHashesInSlotRange(
		poolKeyHash []byte,
		startSlot uint64,
		endSlot uint64,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([][]byte, int, error)

	// GetEarliestNonCheckpointBlockSlot returns the lowest slot at or after
	// fromSlot holding a non-checkpoint block_nonce row. Nonce pruning deletes
	// only those rows, so the slot bounds the history the block lists cover.
	// The bool is false when no such row exists.
	GetEarliestNonCheckpointBlockSlot(
		fromSlot uint64,
		txn types.Txn,
	) (uint64, bool, error)

	// GetEpochStakes returns the registered, delegated credentials captured
	// by the reward snapshot labelled epoch, ordered by pool then credential.
	// A nil poolKeyHash selects every pool.
	GetEpochStakes(
		epoch uint64,
		poolKeyHash []byte,
		limit int,
		offset int,
		txn types.Txn,
	) ([]models.EpochStakeRow, int, error)

	// GetRetiredPoolsPage pages the pools whose latest certificate is an
	// uncancelled retirement that took effect at or before currentEpoch,
	// ordered by retirement epoch then announcement position, along with
	// the unpaginated count.
	GetRetiredPoolsPage(
		currentEpoch uint64,
		limit, offset int,
		descending bool,
		txn types.Txn,
	) ([]models.PoolRetiringRow, int, error)

	// GetPoolHistory returns a pool's stake snapshots labelled below
	// throughLabel (exclusive), joined with the epoch totals and calculated
	// rewards, along with the unpaginated count.
	GetPoolHistory(
		poolKeyHash []byte,
		throughLabel uint64,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([]models.PoolHistoryRow, int, error)

	// GetPoolDelegators returns the registered credentials delegated to a
	// pool, in delegation order, along with the unpaginated count.
	GetPoolDelegators(
		poolKeyHash []byte,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([]models.PoolDelegatorRow, int, error)

	// GetPoolCertificateEvents returns a pool's registration and retirement
	// certificates in chain order, along with the unpaginated count.
	// Certificates synthesized by a ledger-state import have no transaction
	// and are excluded.
	GetPoolCertificateEvents(
		poolKeyHash []byte,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([]models.PoolCertificateEvent, int, error)

	// GetEarliestRewardStakeInputEpoch returns the oldest snapshot label
	// that still has per-credential stake rows; ok is false when none
	// remain. Rows below it were pruned.
	GetEarliestRewardStakeInputEpoch(
		txn types.Txn,
	) (epoch uint64, ok bool, err error)

	// GetAccountStakeHistory returns the stake a credential held in each
	// retained epoch-boundary distribution below throughLabel (exclusive)
	// it was registered and delegated in, along with the unpaginated count.
	GetAccountStakeHistory(
		credentialTag uint8,
		stakingKey []byte,
		throughLabel uint64,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([]models.AccountStakeHistoryRow, int, error)

	// GetAccountMIRHistory returns the MIR payments to a credential in chain
	// order, along with the unpaginated count.
	GetAccountMIRHistory(
		credentialTag uint8,
		stakingKey []byte,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([]models.AccountMIRRow, int, error)

	// GetAccountAssetBalances returns the native assets held by the live
	// UTxOs of a credential, one entry per asset ordered by policy then
	// name, along with the number of distinct assets. It returns
	// models.ErrAccountAggregateLimit when the credential holds more than
	// limits allows.
	GetAccountAssetBalances(
		credentialTag uint8,
		stakingKey []byte,
		limit int,
		offset int,
		order string,
		limits models.AccountAggregateLimits,
		txn types.Txn,
	) ([]models.AssetQuantity, int, error)

	// GetAccountAddressTotals returns the lifetime received and sent totals
	// of every address carrying a stake credential. Sent totals need spent
	// UTxOs, which the store retains in API storage mode. It returns
	// models.ErrAccountAggregateLimit when the credential holds more than
	// limits allows.
	GetAccountAddressTotals(
		credentialTag uint8,
		stakingKey []byte,
		limits models.AccountAggregateLimits,
		txn types.Txn,
	) (models.AccountAddressTotals, error)

	// GetGovernanceProposalsPage pages every stored proposal in submission
	// order, along with the unpaginated count.
	GetGovernanceProposalsPage(
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([]*models.GovernanceProposal, int, error)

	// HasUnattributedCommitteeVotes reports whether any stored committee vote
	// was cast by a hot credential no authorization row maps to a cold
	// credential. Votes outlive the authorizations a pruned table dropped,
	// and a cold-credential vote list cannot be complete while one exists.
	HasUnattributedCommitteeVotes(txn types.Txn) (bool, error)

	// GetGovernanceVotesPage pages every recorded cast of the votes
	// matching filter, ordered by the slot that cast it, along with the
	// unpaginated count. A revote keeps its earlier casts, and votes
	// invalidated by a DRep deregistration keep theirs with a deleted slot.
	GetGovernanceVotesPage(
		filter models.GovernanceVoteFilter,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([]models.GovernanceVoteRow, int, error)

	// GetDRepUpdates returns a DRep's registration, update and
	// deregistration certificates in chain order, along with the
	// unpaginated count.
	GetDRepUpdates(
		credentialTag uint8,
		credential []byte,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([]models.DRepUpdateRow, int, error)

	// GetDRepDelegatorStakes returns the registered credentials delegating
	// to a DRep with their live stake, in delegation order, along with the
	// unpaginated count. predefined selects an always-abstain or
	// always-no-confidence DRep, which has no credential of its own.
	GetDRepDelegatorStakes(
		credentialTag uint8,
		credential []byte,
		predefined *uint64,
		limit int,
		offset int,
		order string,
		txn types.Txn,
	) ([]models.PoolDelegatorRow, int, error)
}
