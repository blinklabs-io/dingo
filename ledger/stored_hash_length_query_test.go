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

package ledger

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// newStoredQueryRowsLedger is a ledger over a LedgerDB set holding changes,
// for a row the set stores as given and a LocalStateQuery result must reject.
func newStoredQueryRowsLedger(
	t *testing.T,
	changes LedgerChanges,
) *LedgerState {
	t.Helper()
	db := fxOpenDB(t)
	ls := newPoolDistr2Ledger(t, db)
	fxSeed(t, db, changes)
	return ls
}

// newShortTxIDLedger is a ledger whose live UTxO set holds one output under
// a 31-byte transaction id, paying the returned address.
func newShortTxIDLedger(t *testing.T) (*LedgerState, lcommon.Address) {
	t.Helper()
	db := fxOpenDB(t)
	addr, row := seedShortTxIDUtxo(t, db)
	ls := newPoolDistr2Ledger(t, db)
	set := newQuerySetHistory()
	set.rawRows = []models.Utxo{row}
	_, set.readErr = lcommon.NewBlake2b256Checked(row.TxId)
	ls.config.UtxoSet = set
	return ls, addr
}

func TestAllDRepDelegatorsRejectsMalformedStoredCredential(t *testing.T) {
	t.Parallel()
	short := shortStoredHash(lcommon.Blake2b224Size, 0x71)
	ls := newStoredQueryRowsLedger(t, LedgerChanges{
		Accounts: []AccountChange{{Slot: 1, Account: models.Account{
			StakingKey: short,
			Drep:       bytes.Repeat([]byte{0x72}, lcommon.Blake2b224Size),
			Active:     true,
			AddedSlot:  1,
		}}},
	})
	delegators, err := ls.allDRepDelegators(context.Background(), nil)
	require.ErrorContains(t, err, "drep delegator")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, delegators)
}

func TestDRepDelegatorsRejectsMalformedStoredCredential(t *testing.T) {
	t.Parallel()
	short := shortStoredHash(lcommon.Blake2b224Size, 0x73)
	drep := bytes.Repeat([]byte{0x74}, lcommon.Blake2b224Size)
	ls := newStoredQueryRowsLedger(t, LedgerChanges{
		Accounts: []AccountChange{{Slot: 1, Account: models.Account{
			StakingKey: short,
			Drep:       drep,
			Active:     true,
			AddedSlot:  1,
		}}},
		DRepLinks: []DRepLinkChange{{
			Slot:  1,
			Drep:  models.NewStakeCredentialRef(0, drep),
			Stake: models.NewStakeCredentialRef(0, short),
			Live:  true,
		}},
	})
	delegators, err := ls.drepDelegators(t.Context(), &models.Drep{
		Credential: drep,
	}, nil)
	require.ErrorContains(t, err, "drep delegator")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, delegators)
}

func TestFilteredDelegationsRejectsMalformedStoredPool(t *testing.T) {
	t.Parallel()
	cred := bytes.Repeat([]byte{0x75}, lcommon.Blake2b224Size)
	ls := newStoredQueryRowsLedger(t, LedgerChanges{
		Accounts: []AccountChange{{Slot: 1, Account: models.Account{
			StakingKey: cred,
			Pool:       shortStoredHash(lcommon.Blake2b224Size, 0x76),
			Active:     true,
			AddedSlot:  1,
		}}},
	})
	result, err := ls.queryShelleyFilteredDelegationAndRewardAccounts(
		context.Background(),
		[]olocalstatequery.StakeCredential{{
			Tag:   0,
			Bytes: lcommon.NewBlake2b224(cred),
		}},
		QueryPoint{},
		nil,
	)
	require.ErrorContains(t, err, "delegation pool id")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, result)
}

func TestGovernanceProposalStateRejectsMalformedStoredSPOVoter(t *testing.T) {
	t.Parallel()
	proposal := models.GovernanceProposal{
		ID:            1,
		TxHash:        bytes.Repeat([]byte{0x7E}, lcommon.Blake2b256Size),
		AnchorURL:     "https://example.invalid/proposal.json",
		AnchorHash:    bytes.Repeat([]byte{0x78}, lcommon.Blake2b256Size),
		ReturnAddress: append([]byte{0xe0}, make([]byte, 28)...),
		GovActionCbor: []byte{0x80},
		AddedSlot:     1,
	}
	ls := newStoredQueryRowsLedger(t, LedgerChanges{
		Proposals: []ProposalChange{{Slot: 1, Proposal: proposal}},
		Votes: []VoteChange{{
			Slot:   1,
			TxHash: proposal.TxHash,
			Vote: models.GovernanceVote{
				ID:              1,
				ProposalID:      1,
				VoterType:       models.VoterTypeSPO,
				VoterCredential: shortStoredHash(lcommon.Blake2b224Size, 0x77),
				AddedSlot:       1,
			},
		}},
	})
	state, err := ls.governanceProposalState(t.Context(), &proposal,
		lcommon.GovActionId{},
		QueryPoint{},
		nil,
	)
	require.ErrorContains(t, err, "governance vote")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Equal(t, olocalstatequery.GovActionState{}, state)
}

// seedShortTxIDUtxo stores the output CBOR under a 31-byte transaction id, so
// the row would resolve and decode with only its result key malformed. The
// CBOR resolver and the query each refuse the id; either alone fails the
// query closed.
func seedShortTxIDUtxo(
	t *testing.T,
	db *database.Database,
) (lcommon.Address, models.Utxo) {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x79}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	out := babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: 1_000_000},
	}
	cborBytes, err := cbor.Encode(&out)
	require.NoError(t, err)
	shortTxID := shortStoredHash(lcommon.Blake2b256Size, 0x7A)
	txn := db.Transaction(context.Background(), true)
	require.NoError(t, db.Blob().SetUtxo(txn.Blob(), shortTxID, 0, cborBytes))
	require.NoError(t, txn.Commit())
	paymentKey := addr.PaymentKeyHash()
	return addr, models.Utxo{
		TxId:       shortTxID,
		OutputIdx:  0,
		PaymentKey: paymentKey[:],
		Cbor:       cborBytes,
	}
}

func TestQueryUtxoByTxInPropagatesMalformedLiveSetKey(t *testing.T) {
	t.Parallel()
	ls, _ := newShortTxIDLedger(t)
	result, err := ls.queryShelleyUtxoByTxIn([]ledger.ShelleyTransactionInput{
		{TxId: lcommon.NewBlake2b256(bytes.Repeat([]byte{0x7B}, 32))},
	},
		QueryPoint{},
		nil,
	)
	require.ErrorContains(t, err, "invalid blake2b-256 hash")
	require.Nil(t, result)
}

func TestQueryUtxoByAddressRejectsMalformedStoredTransactionID(t *testing.T) {
	t.Parallel()
	ls, addr := newShortTxIDLedger(t)
	result, err := ls.queryShelleyUtxoByAddress(
		[]ledger.Address{addr},
		QueryPoint{},
		nil,
	)
	require.ErrorContains(t, err, "invalid blake2b-256 hash")
	require.Nil(t, result)
}

// TestQueryPoolDistr2RejectsMalformedSnapshotPoolKey drives GetPoolDistr2
// over a mark snapshot row whose pool key is one byte short. Padded, the key
// resolves to no registered pool and the pool is silently omitted from the
// distribution cardano-cli computes a leadership schedule from.
func TestQueryPoolDistr2RejectsMalformedSnapshotPoolKey(t *testing.T) {
	t.Parallel()
	db := fxOpenDB(t)
	require.NoError(t, fxSeedSnapshot(t, db, &models.PoolStakeSnapshot{
		Epoch:        0,
		SnapshotType: snapshotTypeMark,
		PoolKeyHash:  shortStoredHash(lcommon.Blake2b224Size, 0x7C),
		TotalStake:   types.Uint64(1_000_000),
		CapturedSlot: 1,
	}))
	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(t.Context(), poolDistr2Query(), QueryPoint{})
	require.ErrorContains(t, err, "pool stake distribution snapshot pool key")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, result)
}

// TestChainDepStateRejectsMalformedStoredOpCertIssuer covers the op-cert
// counter map. Dropping the malformed issuer would report "no certificate
// accepted yet" for a cold key the chain enforces a counter against.
func TestChainDepStateRejectsMalformedStoredOpCertIssuer(t *testing.T) {
	t.Parallel()
	ls := newStoredQueryRowsLedger(t, LedgerChanges{
		OpCerts: []OpCertChange{{Slot: 1, State: OpCertState{
			Pool:     shortStoredHash(lcommon.Blake2b224Size, 0x7D),
			Sequence: 3,
			Slot:     1,
		}}},
	})
	txn := ls.db.Transaction(context.Background(), false)
	defer txn.Release()
	counters, err := ls.chainDepStateOpCertCounters(t.Context(), txn, QueryPoint{})
	require.ErrorContains(t, err, "op-cert counter issuer key")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, counters)
}
