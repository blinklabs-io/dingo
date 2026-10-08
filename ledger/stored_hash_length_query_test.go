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
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// storedQueryRowsStore returns malformed rows from the reads behind the
// LocalStateQuery results that carry stored hashes as result keys.
type storedQueryRowsStore struct {
	metadata.MetadataStore
	activeCredentials []models.StakeCredentialRef
	accounts          map[string]*models.Account
	drepDelegators    []models.StakeCredentialRef
	votes             []*models.GovernanceVote
	utxos             []models.Utxo
	opCertSequences   map[string]uint64
}

func (s *storedQueryRowsStore) LatestPoolOpCertSequences(
	txn types.Txn,
) (map[string]uint64, error) {
	if s.opCertSequences == nil {
		return s.MetadataStore.LatestPoolOpCertSequences(txn)
	}
	return s.opCertSequences, nil
}

func (s *storedQueryRowsStore) GetActiveAccountCredentials(
	types.Txn,
) ([]models.StakeCredentialRef, error) {
	return s.activeCredentials, nil
}

func (s *storedQueryRowsStore) GetAccountsByCredential(
	[]models.StakeCredentialRef,
	bool,
	types.Txn,
) (map[string]*models.Account, error) {
	return s.accounts, nil
}

func (s *storedQueryRowsStore) GetDRepDelegators(
	uint8,
	[]byte,
	types.Txn,
) ([]models.StakeCredentialRef, error) {
	return s.drepDelegators, nil
}

func (s *storedQueryRowsStore) GetGovernanceVotes(
	uint,
	types.Txn,
) ([]*models.GovernanceVote, error) {
	return s.votes, nil
}

func (s *storedQueryRowsStore) GetUtxosByRefs(
	[]models.UtxoId,
	types.Txn,
) ([]models.Utxo, error) {
	return s.utxos, nil
}

func (s *storedQueryRowsStore) GetUtxosByAddress(
	[]models.UtxoAddressPattern,
	int,
	types.Txn,
) ([]models.Utxo, error) {
	return s.utxos, nil
}

func newStoredQueryRowsLedger(
	t *testing.T,
	configure func(*database.Database, *storedQueryRowsStore),
) *LedgerState {
	t.Helper()
	store := &storedQueryRowsStore{}
	db, err := dbtest.NewDatabaseWithMetadataWrapper(
		t,
		dbtest.Options{Config: &database.Config{DataDir: ""}},
		func(inner metadata.MetadataStore) metadata.MetadataStore {
			store.MetadataStore = inner
			return store
		},
	)
	require.NoError(t, err)
	configure(db, store)
	return newPoolDistr2Ledger(t, db)
}

func TestAllDRepDelegatorsRejectsMalformedStoredCredential(t *testing.T) {
	t.Parallel()
	short := shortStoredHash(lcommon.Blake2b224Size, 0x71)
	ls := newStoredQueryRowsLedger(t, func(
		_ *database.Database,
		s *storedQueryRowsStore,
	) {
		ref := models.StakeCredentialRef{Tag: 0, Key: short}
		s.activeCredentials = []models.StakeCredentialRef{ref}
		s.accounts = map[string]*models.Account{
			ref.MapKey(): {
				StakingKey: short,
				Drep:       bytes.Repeat([]byte{0x72}, lcommon.Blake2b224Size),
			},
		}
	})
	delegators, err := ls.allDRepDelegators(context.Background(), nil)
	require.ErrorContains(t, err, "drep delegator")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, delegators)
}

func TestDRepDelegatorsRejectsMalformedStoredCredential(t *testing.T) {
	t.Parallel()
	ls := newStoredQueryRowsLedger(t, func(
		_ *database.Database,
		s *storedQueryRowsStore,
	) {
		s.drepDelegators = []models.StakeCredentialRef{{
			Tag: 0,
			Key: shortStoredHash(lcommon.Blake2b224Size, 0x73),
		}}
	})
	delegators, err := ls.drepDelegators(t.Context(), &models.Drep{
		Credential: bytes.Repeat([]byte{0x74}, lcommon.Blake2b224Size),
	}, nil)
	require.ErrorContains(t, err, "drep delegator")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, delegators)
}

func TestFilteredDelegationsRejectsMalformedStoredPool(t *testing.T) {
	t.Parallel()
	cred := bytes.Repeat([]byte{0x75}, lcommon.Blake2b224Size)
	ls := newStoredQueryRowsLedger(t, func(
		_ *database.Database,
		s *storedQueryRowsStore,
	) {
		ref := models.StakeCredentialRef{Tag: 0, Key: cred}
		s.accounts = map[string]*models.Account{
			ref.MapKey(): {
				StakingKey: cred,
				Pool:       shortStoredHash(lcommon.Blake2b224Size, 0x76),
				Active:     true,
			},
		}
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
	ls := newStoredQueryRowsLedger(t, func(
		_ *database.Database,
		s *storedQueryRowsStore,
	) {
		s.votes = []*models.GovernanceVote{{
			VoterType:       models.VoterTypeSPO,
			VoterCredential: shortStoredHash(lcommon.Blake2b224Size, 0x77),
		}}
	})
	state, err := ls.governanceProposalState(t.Context(), &models.GovernanceProposal{
		AnchorURL:     "https://example.invalid/proposal.json",
		AnchorHash:    bytes.Repeat([]byte{0x78}, lcommon.Blake2b256Size),
		ReturnAddress: append([]byte{0xe0}, make([]byte, 28)...),
		GovActionCbor: []byte{0x80},
	},
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
	s *storedQueryRowsStore,
) lcommon.Address {
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
	s.utxos = []models.Utxo{{
		TxId:       shortTxID,
		OutputIdx:  0,
		PaymentKey: paymentKey[:],
	}}
	return addr
}

func TestQueryUtxoByTxInRejectsMalformedStoredTransactionID(t *testing.T) {
	t.Parallel()
	ls := newStoredQueryRowsLedger(t, func(
		db *database.Database,
		s *storedQueryRowsStore,
	) {
		seedShortTxIDUtxo(t, db, s)
	})
	result, err := ls.queryShelleyUtxoByTxIn(t.Context(), []ledger.ShelleyTransactionInput{
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
	var addr lcommon.Address
	ls := newStoredQueryRowsLedger(t, func(
		db *database.Database,
		s *storedQueryRowsStore,
	) {
		addr = seedShortTxIDUtxo(t, db, s)
	})
	result, err := ls.queryShelleyUtxoByAddress(
		context.Background(),
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
	db := newTestDB(t)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        0,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  shortStoredHash(lcommon.Blake2b224Size, 0x7C),
			TotalStake:   types.Uint64(1_000_000),
			CapturedSlot: 1,
		},
		nil,
	))
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
	ls := newStoredQueryRowsLedger(t, func(
		db *database.Database,
		s *storedQueryRowsStore,
	) {
		s.opCertSequences = map[string]uint64{
			string(shortStoredHash(lcommon.Blake2b224Size, 0x7D)): 3,
		}
	})
	txn := ls.db.Transaction(context.Background(), false)
	defer txn.Release()
	counters, err := ls.chainDepStateOpCertCounters(t.Context(), txn, QueryPoint{})
	require.ErrorContains(t, err, "op-cert counter issuer key")
	require.ErrorContains(t, err, "invalid blake2b-224 hash")
	require.Nil(t, counters)
}
