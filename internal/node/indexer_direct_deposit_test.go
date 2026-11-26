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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/indexer"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// journalLedger serves the reward-credit journal a test seeded in a store, as
// the ledger does, and no state at an anchor.
type journalLedger struct {
	metadata.FactRowReader
	db *database.Database
}

func (j journalLedger) RewardStakeInputsPendingState() (string, error) {
	return j.db.Metadata().GetSyncState(models.RewardStakeInputsPendingKey, nil)
}

func (j journalLedger) RewardPrecomputeCursors() (map[uint64]string, error) {
	keys, err := j.db.ListSyncStateKeysByPrefix(
		models.RewardPrecomputeCursorPrefix, nil,
	)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]string, len(keys))
	for _, k := range keys {
		epoch, err := strconv.ParseUint(
			strings.TrimPrefix(k, models.RewardPrecomputeCursorPrefix), 10, 64,
		)
		if err != nil {
			continue
		}
		if out[epoch], err = j.db.Metadata().GetSyncState(k, nil); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (journalLedger) AnchorOpCertCounters(
	context.Context, uint64,
) ([]metadata.OpCertCounter, error) {
	return nil, nil
}

func (journalLedger) AnchorStateReady(context.Context) error { return nil }

func (journalLedger) AnchorImportedAccounts(
	context.Context, uint64, []byte, int,
) ([]metadata.ImportedAccount, []byte, error) {
	return nil, nil, nil
}

func (journalLedger) EachAnchorRewardJournal(
	context.Context, uint64, func(metadata.RewardJournalEntry) error,
) error {
	return nil
}

func (journalLedger) AnchorUtxoDigest(
	context.Context, uint64,
) (metadata.UtxoSetDigest, error) {
	return metadata.UtxoSetDigest{}, nil
}

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

// A Dijkstra direct deposit credits an account after the certificates of its
// own transaction, and a later transaction may withdraw it. When the account
// is registered in the same segment as its deposit, the IndexerDB creates the
// account only as it indexes the blocks, so the ledger's credit has to land
// at that point and not before the segment's blocks are indexed.
func TestIndexerWriterCreditsDirectDepositToAccountRegisteredInSegment(
	t *testing.T,
) {
	t.Parallel()
	stakeKey := common.NewBlake2b224([]byte("indexer-direct-deposit-stake-key"))
	account, err := common.NewAddressFromParts(
		common.AddressTypeNoneKey,
		common.AddressNetworkTestnet,
		nil,
		stakeKey.Bytes(),
	)
	require.NoError(t, err)
	register := func() []common.CertificateWrapper {
		return []common.CertificateWrapper{{
			Type: uint(common.CertificateTypeRegistration),
			Certificate: &common.RegistrationCertificate{
				CertType: uint(common.CertificateTypeRegistration),
				StakeCredential: common.Credential{
					CredType:   common.CredentialTypeAddrKeyHash,
					Credential: stakeKey,
				},
			},
		}}
	}
	const deposited = 20_000_000
	tests := []struct {
		name string
		// blocks holds the transactions of each block.
		blocks func(t *testing.T) [][]*dijkstra.DijkstraTransaction
		want   uint64
	}{
		{
			name: "deposit in a later block",
			blocks: func(t *testing.T) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{
					{directDepositTx(t, account, register(), 0, 0)},
					{directDepositTx(t, account, nil, 0, deposited)},
				}
			},
			want: deposited,
		},
		{
			name: "registration and deposit in one block",
			blocks: func(t *testing.T) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{{
					directDepositTx(t, account, register(), 0, 0),
					directDepositTx(t, account, nil, 0, deposited),
				}}
			},
			want: deposited,
		},
		{
			name: "registration and deposit in one transaction",
			blocks: func(t *testing.T) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{{
					directDepositTx(t, account, register(), 0, deposited),
				}}
			},
			want: deposited,
		},
		{
			name: "withdrawal of the deposit in a later block",
			blocks: func(t *testing.T) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{
					{directDepositTx(t, account, register(), 0, 0)},
					{directDepositTx(t, account, nil, 0, deposited)},
					{directDepositTx(t, account, nil, deposited, 0)},
				}
			},
			want: 0,
		},
		{
			name: "withdrawal of the deposit in the same block",
			blocks: func(t *testing.T) [][]*dijkstra.DijkstraTransaction {
				return [][]*dijkstra.DijkstraTransaction{{
					directDepositTx(t, account, register(), 0, 0),
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
			ctx := context.Background()
			db := newIndexerTestDB(t)
			consensus := consensusOf(t, db)

			// The ledger journals each deposit at its block's slot, under
			// the transaction's hash, once it has applied the transaction.
			ledgerDB, err := dbtest.NewDatabase(t, &database.Config{
				DataDir: t.TempDir(),
				Role:    database.RoleIndexer,
			})
			require.NoError(t, err)
			journal, err := dbtest.RawSQLiteMetadata(t, ledgerDB)
			require.NoError(t, err)

			prev := common.Blake2b256{}
			var last models.Block
			for i, txs := range tc.blocks(t) {
				blk, err := fixtures.NewBlockBuilder(
					ledger.GetEraById(dijkstra.EraIdDijkstra),
				).
					WithBlockNumber(uint64(i + 1)).
					WithSlot(uint64(100 + i)).
					WithPreviousHash(prev).
					WithTransactions(toTransactions(txs)...).
					Build()
				require.NoError(t, err)
				last = models.Block{
					Slot: blk.SlotNumber(), Hash: blk.Hash().Bytes(),
					PrevHash: blk.PrevHash().Bytes(), Number: blk.BlockNumber(),
					Cbor: blk.Cbor(), Type: uint(blk.Type()),
				}
				require.NoError(t, consensus.BlockCreate(last, nil))
				prev = blk.Hash()
				for _, tx := range txs {
					for _, effect := range tx.Body.TxDirectDeposits {
						seedJournalCredit(
							t, journal, stakeKey.Bytes(), tx.Hash().Bytes(),
							effect, last.Slot,
						)
					}
				}
			}
			require.NoError(t, consensus.SetTip(ochainsync.Tip{
				Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
				BlockNumber: last.Number,
			}, nil))

			store, err := indexerdb.NewStore(db, db, 10)
			require.NoError(t, err)
			facts, err := indexer.NewMetadataFacts(
				journalLedger{
					FactRowReader: ledgerDB.Metadata().(metadata.FactRowReader),
					db:            ledgerDB,
				},
				db,
			)
			require.NoError(t, err)
			bf := NewBackfill(db, nil, slog.Default())
			bf.SetDelegatorInactivityEnabled(false)
			w := indexer.NewWriter(
				store, indexer.NewDatabaseSource(consensus), bf,
				slog.Default(), 10,
			).WithFacts(facts)

			require.NoError(t, w.CatchUp(ctx))

			tip, err := store.Tip(nil)
			require.NoError(t, err)
			require.Equal(t, last.Slot, tip.Slot)
			raw := openRaw(t, filepath.Join(db.Config().DataDir, "metadata.sqlite"))
			var reward sql.NullString
			require.NoError(t, raw.QueryRow(
				`SELECT reward FROM account
				 WHERE staking_key = ? AND credential_tag = 0 AND active = 1`,
				stakeKey.Bytes(),
			).Scan(&reward))
			require.Equal(t, strconv.FormatUint(tc.want, 10), reward.String)
		})
	}
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
	stakeKey, txHash []byte,
	amount uint64,
	slot uint64,
) {
	t.Helper()
	_, err := journal.Exec(`INSERT INTO account_reward_delta
		(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal, post_snapshot)
		VALUES (?, 0, ?, ?, ?, 0, 0)`,
		stakeKey, txHash, strconv.FormatUint(amount, 10), slot)
	require.NoError(t, err)
}
