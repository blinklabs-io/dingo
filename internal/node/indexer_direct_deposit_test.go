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

package node

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/factledger"
	"github.com/blinklabs-io/dingo/internal/test/leiosclosurefixture"
	dledger "github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// directDepositTx is a Dijkstra transaction body with the given certificates,
// withdrawals and direct deposits.
func directDepositTx(
	t *testing.T,
	account common.Address,
	certs []common.CertificateWrapper,
	withdraw, deposit uint64,
) *dijkstra.DijkstraTransaction {
	t.Helper()
	body := dijkstra.DijkstraTransactionBody{TxFee: 1, TxCertificates: certs}
	if withdraw > 0 {
		body.TxWithdrawals = map[*common.Address]uint64{&account: withdraw}
	}
	builder := fixtures.NewDijkstraTransactionBuilder().WithBody(body)
	if deposit > 0 {
		builder = builder.WithDirectDeposit(account, deposit)
	}
	tx, err := builder.Build()
	require.NoError(t, err)
	return tx
}

// rewardAccount returns a reward account whose stake credential is a key hash,
// or a script hash when script is set, its credential tag, and the
// certificates registering it.
func rewardAccount(
	t *testing.T,
	seed string,
	script bool,
) (common.Blake2b224, common.Address, uint8, []common.CertificateWrapper) {
	t.Helper()
	stakeKey := common.NewBlake2b224([]byte(seed))
	addrType, credType := uint8(common.AddressTypeNoneKey),
		uint(common.CredentialTypeAddrKeyHash)
	if script {
		addrType, credType = common.AddressTypeNoneScript,
			uint(common.CredentialTypeScriptHash)
	}
	account, err := common.NewAddressFromParts(
		addrType, common.AddressNetworkTestnet, nil, stakeKey.Bytes(),
	)
	require.NoError(t, err)
	tag, err := models.CredentialTagFromUint(credType)
	require.NoError(t, err)
	return stakeKey, account, uint8(tag), []common.CertificateWrapper{{
		Type: uint(common.CertificateTypeRegistration),
		Certificate: &common.RegistrationCertificate{
			CertType: uint(common.CertificateTypeRegistration),
			StakeCredential: common.Credential{
				CredType:   credType,
				Credential: stakeKey,
			},
		},
	}}
}

// A Dijkstra direct deposit credits an account after the certificates of its
// own transaction, and a later transaction may withdraw it. When the account
// is registered in the same segment as its deposit, the IndexerDB creates the
// account only as it indexes the blocks, so the ledger's credit has to land
// at that point and not before the segment's blocks are indexed.
func TestIndexerWriterCreditsDirectDepositToAccountRegisteredInSegment(
	t *testing.T,
) {
	t.Parallel()
	const deposited = 20_000_000
	tests := []struct {
		name string
		// script registers a script-hash stake credential.
		script bool
		// blocks holds the transactions of each block.
		blocks func(
			t *testing.T,
			account common.Address,
			register []common.CertificateWrapper,
		) [][]*dijkstra.DijkstraTransaction
		want uint64
	}{
		{
			name: "deposit in a later block",
			blocks: func(
				t *testing.T,
				account common.Address,
				register []common.CertificateWrapper,
			) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{
					{directDepositTx(t, account, register, 0, 0)},
					{directDepositTx(t, account, nil, 0, deposited)},
				}
			},
			want: deposited,
		},
		{
			name: "registration and deposit in one block",
			blocks: func(
				t *testing.T,
				account common.Address,
				register []common.CertificateWrapper,
			) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{{
					directDepositTx(t, account, register, 0, 0),
					directDepositTx(t, account, nil, 0, deposited),
				}}
			},
			want: deposited,
		},
		{
			name: "registration and deposit in one transaction",
			blocks: func(
				t *testing.T,
				account common.Address,
				register []common.CertificateWrapper,
			) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{{
					directDepositTx(t, account, register, 0, deposited),
				}}
			},
			want: deposited,
		},
		{
			name:   "script credential registered and credited in one block",
			script: true,
			blocks: func(
				t *testing.T,
				account common.Address,
				register []common.CertificateWrapper,
			) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{{
					directDepositTx(t, account, register, 0, deposited),
					directDepositTx(t, account, nil, 5_000_000, 0),
				}}
			},
			want: deposited - 5_000_000,
		},
		{
			name: "withdrawal of the deposit in a later block",
			blocks: func(
				t *testing.T,
				account common.Address,
				register []common.CertificateWrapper,
			) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{
					{directDepositTx(t, account, register, 0, 0)},
					{directDepositTx(t, account, nil, 0, deposited)},
					{directDepositTx(t, account, nil, deposited, 0)},
				}
			},
			want: 0,
		},
		{
			name: "withdrawal of the deposit in the same block",
			blocks: func(
				t *testing.T,
				account common.Address,
				register []common.CertificateWrapper,
			) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{{
					directDepositTx(t, account, register, 0, 0),
					directDepositTx(t, account, nil, 0, deposited),
					directDepositTx(t, account, nil, 5_000_000, 0),
				}}
			},
			want: deposited - 5_000_000,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stakeKey, account, tag, register := rewardAccount(
				t, "indexer-direct-deposit-stake-key", tc.script,
			)
			db := newIndexerTestDB(t)
			consensus := consensusOf(t, db)
			ledgerDB, journal := newJournalDB(t)

			prev := common.Blake2b256{}
			var last models.Block
			for i, txs := range tc.blocks(t, account, register) {
				last = storeDijkstraBlock(
					t, consensus, uint64(i+1), uint64(100+i), prev, txs,
				)
				prev = common.NewBlake2b256(last.Hash)
				for _, tx := range txs {
					for _, effect := range tx.Body.TxDirectDeposits {
						seedJournalCredit(
							t, journal, tag, stakeKey.Bytes(),
							tx.Hash().Bytes(), effect, last.Slot,
						)
					}
				}
			}
			bf := NewBackfill(db, nil, slog.Default())
			bf.SetDelegatorInactivityEnabled(false)
			catchUpWithJournal(t, db, ledgerDB, bf, last)
			require.Equal(
				t,
				strconv.FormatUint(tc.want, 10),
				rewardOf(t, db, tag, stakeKey),
			)
		})
	}
}

// A direct deposit in a certified closure's transaction is credited when the
// ledger applies that transaction, as a ranking block's is, so a withdrawal
// later in the segment finds it.
func TestIndexerWriterCreditsClosureDirectDeposit(t *testing.T) {
	t.Parallel()
	stakeKey, account, tag, register := rewardAccount(
		t, "indexer-closure-deposit-stake-key", false,
	)
	const deposited = 20_000_000
	tests := []struct {
		name string
		// closure holds the closure's transactions, later the transactions
		// of the block after the certifying block.
		closure, later func(t *testing.T) []*dijkstra.DijkstraTransaction
		want           uint64
	}{
		{
			name: "withdrawal in a later block",
			closure: func(t *testing.T) []*dijkstra.DijkstraTransaction {
				return []*dijkstra.DijkstraTransaction{
					directDepositTx(t, account, register, 0, deposited),
				}
			},
			later: func(t *testing.T) []*dijkstra.DijkstraTransaction {
				return []*dijkstra.DijkstraTransaction{
					directDepositTx(t, account, nil, 5_000_000, 0),
				}
			},
			want: deposited - 5_000_000,
		},
		{
			name: "withdrawal in the same closure",
			closure: func(t *testing.T) []*dijkstra.DijkstraTransaction {
				return []*dijkstra.DijkstraTransaction{
					directDepositTx(t, account, register, 0, deposited),
					directDepositTx(t, account, nil, deposited, 0),
				}
			},
			want: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newIndexerTestDB(t)
			consensus := consensusOf(t, db)
			ledgerDB, journal := newJournalDB(t)

			txs := tc.closure(t)
			raw := make([]cbor.RawMessage, len(txs))
			for i, tx := range txs {
				raw[i] = tx.Cbor()
			}
			scenario, err := leiosclosurefixture.NewWithTransactions(raw)
			require.NoError(t, err)
			scenario.StoreChain(t, consensus)
			last := leiosclosurefixture.Model(scenario.Certifying)
			// The ledger dates a closure's credits at the ranking block that
			// certifies it.
			for _, tx := range txs {
				for _, effect := range tx.Body.TxDirectDeposits {
					seedJournalCredit(
						t, journal, tag, stakeKey.Bytes(),
						tx.Hash().Bytes(), effect, last.Slot,
					)
				}
			}
			if tc.later != nil {
				last = storeDijkstraBlock(
					t, consensus, last.Number+1, last.Slot+10,
					common.NewBlake2b256(last.Hash), tc.later(t),
				)
			}
			bf := NewBackfill(db, nil, slog.Default())
			bf.SetDelegatorInactivityEnabled(false)
			bf.DisableNonceComputation()
			bf.SetClosureSource(dledger.NewDatabaseClosureSource(consensus))
			catchUpWithJournal(t, db, ledgerDB, bf, last)
			require.Equal(
				t,
				strconv.FormatUint(tc.want, 10),
				rewardOf(t, db, tag, stakeKey),
			)
		})
	}
}

// newJournalDB returns a ledger database the test seeds the reward-credit
// journal in, and its SQLite tables.
func newJournalDB(t *testing.T) (*database.Database, *sql.DB) {
	t.Helper()
	ledgerDB, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Role:    database.RoleIndexer,
	})
	require.NoError(t, err)
	journal, err := dbtest.RawSQLiteMetadata(t, ledgerDB)
	require.NoError(t, err)
	return ledgerDB, journal
}

// storeDijkstraBlock stores a Dijkstra block holding txs in consensus and
// returns its stored form.
func storeDijkstraBlock(
	t *testing.T,
	consensus *database.Database,
	number, slot uint64,
	prev common.Blake2b256,
	txs []*dijkstra.DijkstraTransaction,
) models.Block {
	t.Helper()
	blk, err := fixtures.NewBlockBuilder(
		ledger.GetEraById(dijkstra.EraIdDijkstra),
	).
		WithBlockNumber(number).
		WithSlot(slot).
		WithPreviousHash(prev).
		WithTransactions(toTransactions(txs)...).
		Build()
	require.NoError(t, err)
	stored := models.Block{
		Slot: blk.SlotNumber(), Hash: blk.Hash().Bytes(),
		PrevHash: blk.PrevHash().Bytes(), Number: blk.BlockNumber(),
		Cbor: blk.Cbor(), Type: uint(blk.Type()),
	}
	require.NoError(t, consensus.BlockCreate(stored, nil))
	return stored
}

// catchUpWithJournal sets the ledger's tip to last and catches the IndexerDB
// db up to it through bf, copying the facts the test seeded in ledgerDB.
func catchUpWithJournal(
	t *testing.T,
	db, ledgerDB *database.Database,
	bf *Backfill,
	last models.Block,
) {
	t.Helper()
	consensus := consensusOf(t, db)
	require.NoError(t, consensus.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	store, err := indexerdb.NewStore(db, db, 10)
	require.NoError(t, err)
	facts, err := indexer.NewMetadataFacts(factledger.New(t, ledgerDB), db)
	require.NoError(t, err)
	w := indexer.NewWriter(
		store, indexer.NewDatabaseSource(consensus), bf, slog.Default(), 10,
	).WithFacts(facts)
	require.NoError(t, w.CatchUp(context.Background()))
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, last.Slot, tip.Slot)
}

// rewardOf returns the reward balance of the active account of the stake
// credential tag and stakeKey.
func rewardOf(
	t *testing.T,
	db *database.Database,
	tag uint8,
	stakeKey common.Blake2b224,
) string {
	t.Helper()
	raw := openRaw(t, filepath.Join(db.Config().DataDir, "metadata.sqlite"))
	var reward sql.NullString
	require.NoError(t, raw.QueryRow(
		`SELECT reward FROM account
		 WHERE staking_key = ? AND credential_tag = ? AND active = 1`,
		stakeKey.Bytes(), tag,
	).Scan(&reward))
	return reward.String
}

func toTransactions(txs []*dijkstra.DijkstraTransaction) []common.Transaction {
	out := make([]common.Transaction, len(txs))
	for i, tx := range txs {
		out[i] = tx
	}
	return out
}

func seedJournalCredit(
	t *testing.T,
	journal *sql.DB,
	tag uint8,
	stakeKey, txHash []byte,
	amount uint64,
	slot uint64,
) {
	t.Helper()
	_, err := journal.Exec(`INSERT INTO account_reward_delta
		(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal, post_snapshot)
		VALUES (?, ?, ?, ?, ?, 0, 0)`,
		stakeKey, tag, txHash, strconv.FormatUint(amount, 10), slot)
	require.NoError(t, err)
}
