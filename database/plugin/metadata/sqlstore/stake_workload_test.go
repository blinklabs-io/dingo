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

package sqlstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// newWALSQLiteStore opens a migrated, file-backed store whose WAL is never
// checkpointed, so every page a commit dirtied stays in the -wal file as one
// frame for stakeWALProfile to attribute.
func newWALSQLiteStore(tb testing.TB) (*Store, string) {
	tb.Helper()
	dir := tb.TempDir()
	path := filepath.Join(dir, "meta.sqlite")
	// The dsn must match the one seedMigratedSQLite restores into; the
	// template is restored over this connection's database.
	db, err := OpenDB(
		"sqlite",
		"file:"+path+"?_pragma=busy_timeout(30000)"+
			"&_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)"+
			"&_pragma=foreign_keys(1)",
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	db.SetMaxOpenConns(1)
	seedMigratedSQLite(tb, db)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(tb, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(tb, err)
	require.NoError(tb, store.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	_, err = db.Exec("PRAGMA wal_autocheckpoint = 0")
	require.NoError(tb, err)
	return store, path
}

func walFrameCount(tb testing.TB, path string) int {
	tb.Helper()
	info, err := os.Stat(path + "-wal")
	require.NoError(tb, err)
	const header = 32
	const frameHeader = 24
	size := info.Size()
	if size <= header {
		return 0
	}
	return int((size - header) / int64(frameHeader+4096))
}

// stakeWALProfile counts the WAL frames written between two frame counts,
// grouped by the b-tree (table or index) that owns each page.
func stakeWALProfile(
	tb testing.TB,
	db *sql.DB,
	path string,
	from, to int,
) map[string]int {
	tb.Helper()
	data, err := os.ReadFile(path + "-wal")
	require.NoError(tb, err)
	pageSize := int(binary.BigEndian.Uint32(data[8:12]))
	require.Equal(tb, 4096, pageSize)
	owner := map[uint32]string{}
	rows, err := db.Query("SELECT pageno, name FROM dbstat")
	require.NoError(tb, err)
	for rows.Next() {
		var page uint32
		var name string
		require.NoError(tb, rows.Scan(&page, &name))
		owner[page] = name
	}
	require.NoError(tb, rows.Err())
	require.NoError(tb, rows.Close())
	out := map[string]int{}
	for frame := from; frame < to; frame++ {
		off := 32 + frame*(24+pageSize)
		page := binary.BigEndian.Uint32(data[off : off+4])
		name, ok := owner[page]
		if !ok {
			name = "(unowned)"
		}
		out[name]++
	}
	return out
}

func stakeBytes(label string, i int, prefix byte) []byte {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", label, i)))
	out := make([]byte, 28)
	copy(out, sum[:28])
	out[0] = prefix
	return out
}

type stakeWorkloadCred struct {
	stake   lcommon.Blake2b224
	payment lcommon.Blake2b224
	addr    lcommon.Address
	pool    []byte
	utxos   []stakeWorkloadUtxo
	reward  uint64
}

type stakeWorkloadUtxo struct {
	txID   []byte
	idx    int
	amount uint64
}

type stakeWorkload struct {
	t       *testing.T
	store   *Store
	rng     *rand.Rand
	creds   []*stakeWorkloadCred
	pools   [][]byte
	slot    uint64
	txCount int
}

func newStakeWorkload(
	t *testing.T,
	store *Store,
	credCount, poolCount int,
) *stakeWorkload {
	t.Helper()
	w := &stakeWorkload{
		t:     t,
		store: store,
		rng:   rand.New(rand.NewSource(7)),
		slot:  1000,
	}
	for i := range poolCount {
		w.pools = append(w.pools, stakeBytes("pool", i, 0x50))
	}
	for i := range credCount {
		stake := lcommon.NewBlake2b224(stakeBytes("stake", i, 0x20))
		payment := lcommon.NewBlake2b224(stakeBytes("pay", i, 0x30))
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey,
			lcommon.AddressNetworkTestnet,
			payment.Bytes(),
			stake.Bytes(),
		)
		require.NoError(t, err)
		w.creds = append(w.creds, &stakeWorkloadCred{
			stake: stake, payment: payment, addr: addr,
			pool: w.pools[w.rng.Intn(poolCount)],
		})
	}
	return w
}

type stakeTxSpec struct {
	spend       *stakeWorkloadCred
	outputs     []*stakeWorkloadCred
	register    bool
	delegate    []byte
	withdrawal  uint64
	outputValue uint64
}

// apply writes one transaction through SetTransaction inside txn.
func (w *stakeWorkload) apply(txn types.Txn, spec stakeTxSpec) {
	w.t.Helper()
	w.txCount++
	txID := stakeBytes("tx", w.txCount, 0x70)
	builder := mockledger.NewTransactionBuilder()
	if spec.spend != nil && len(spec.spend.utxos) > 0 {
		spent := spec.spend.utxos[0]
		spec.spend.utxos = spec.spend.utxos[1:]
		input, err := mockledger.NewTransactionInputBuilder().
			WithTxId(spent.txID).
			WithIndex(uint32(spent.idx)).Build() //nolint:gosec
		require.NoError(w.t, err)
		builder.WithInputs(input)
	}
	cred := spec.outputs[0]
	var certs []lcommon.Certificate
	deposits := map[int]uint64{}
	if spec.register {
		certs = append(certs, &lcommon.StakeRegistrationCertificate{
			CertType: uint(lcommon.CertificateTypeStakeRegistration),
			StakeCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: cred.stake,
			},
		})
		deposits[len(certs)-1] = 2_000_000
	}
	if spec.delegate != nil {
		var pool lcommon.PoolKeyHash
		copy(pool[:], spec.delegate)
		certs = append(certs, &lcommon.StakeDelegationCertificate{
			CertType: uint(lcommon.CertificateTypeStakeDelegation),
			StakeCredential: &lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: cred.stake,
			},
			PoolKeyHash: pool,
		})
	}
	var outputs []lcommon.TransactionOutput
	var produced []stakeWorkloadUtxo
	for idx, out := range spec.outputs {
		amount := spec.outputValue + uint64(idx)
		o, err := mockledger.NewTransactionOutputBuilder().
			WithAddress(out.addr.String()).
			WithLovelace(amount).Build()
		require.NoError(w.t, err)
		outputs = append(outputs, o)
		produced = append(produced, stakeWorkloadUtxo{txID, idx, amount})
	}
	tx := builder.WithCertificates(certs...)
	tx.WithId(txID)
	tx.WithOutputs(outputs...)
	tx.WithValid(true)
	if spec.withdrawal > 0 {
		rewardAddr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeNoneKey,
			lcommon.AddressNetworkTestnet,
			nil,
			cred.stake.Bytes(),
		)
		require.NoError(w.t, err)
		tx.WithWithdrawals(
			map[*lcommon.Address]uint64{&rewardAddr: spec.withdrawal},
		)
	}
	point := ocommon.Point{Slot: w.slot, Hash: txID}
	require.NoError(w.t, w.store.SetTransaction(
		tx, point, 0, deposits, false, txn, 0,
	))
	for i, out := range spec.outputs {
		out.utxos = append(out.utxos, produced[i])
	}
}

func (w *stakeWorkload) block(fn func(txn types.Txn)) {
	w.t.Helper()
	w.slot += 20
	txn := w.store.Transaction(context.Background())
	fn(txn)
	require.NoError(w.t, txn.Commit())
}

// register registers and delegates every credential, paying it a first UTxO,
// in blocks of perBlock transactions.
func (w *stakeWorkload) register(perBlock int) {
	for start := 0; start < len(w.creds); start += perBlock {
		end := min(start+perBlock, len(w.creds))
		w.block(func(txn types.Txn) {
			for _, c := range w.creds[start:end] {
				w.apply(txn, stakeTxSpec{
					outputs:     []*stakeWorkloadCred{c},
					register:    true,
					delegate:    c.pool,
					outputValue: 5_000_000 + uint64(w.rng.Intn(1000)),
				})
			}
		})
	}
}

// epochBoundary writes the reward state a boundary persists: every
// credential's snapshot stake, its reward output, and the journalled credit.
func (w *stakeWorkload) epochBoundary(epoch uint64) {
	w.t.Helper()
	w.slot += 20
	boundary := w.slot
	var inputs []*models.RewardStakeInput
	var outputs []*models.RewardAccountOutput
	var credits []models.AccountRewardCredit
	for _, c := range w.creds {
		var stake uint64
		for _, u := range c.utxos {
			stake += u.amount
		}
		inputs = append(inputs, &models.RewardStakeInput{
			PoolKeyHash:   c.pool,
			StakingKey:    c.stake.Bytes(),
			Epoch:         epoch,
			Stake:         types.Uint64(stake),
			Registered:    true,
			CapturedSlot:  boundary,
			BoundarySlot:  boundary,
			CredentialTag: 0,
		})
		amount := 1_000 + stake/1_000
		outputs = append(outputs, &models.RewardAccountOutput{
			StakingKey:   c.stake.Bytes(),
			PoolKeyHash:  c.pool,
			RewardType:   "member",
			Epoch:        epoch,
			Amount:       types.Uint64(amount),
			Spendable:    true,
			CapturedSlot: boundary,
			BoundarySlot: boundary,
		})
		credits = append(credits, models.AccountRewardCredit{
			StakingKey: c.stake.Bytes(),
			SourceHash: models.StakeRewardSourceHash(
				epoch, c.pool, 0, c.stake.Bytes(), "member",
			),
			Amount: amount,
			Slot:   boundary,
		})
		c.reward += amount
	}
	txn := w.store.Transaction(context.Background())
	require.NoError(w.t, w.store.SaveRewardStakeInputs(inputs, txn))
	require.NoError(w.t, w.store.SaveRewardAccountOutputs(outputs, txn))
	require.NoError(w.t, w.store.AddAccountRewardsByCredential(credits, txn))
	require.NoError(w.t, txn.Commit())
}

// syncBlocks applies blocks of payments, re-delegations and withdrawals.
func (w *stakeWorkload) syncBlocks(blocks, perBlock int) (redelegations int) {
	for range blocks {
		w.block(func(txn types.Txn) {
			for i := range perBlock {
				from := w.creds[w.rng.Intn(len(w.creds))]
				to := w.creds[w.rng.Intn(len(w.creds))]
				for len(from.utxos) == 0 {
					from = w.creds[w.rng.Intn(len(w.creds))]
				}
				value := uint64(1_000_000 + w.rng.Intn(1_000_000))
				switch {
				case i%7 == 3:
					pool := w.pools[w.rng.Intn(len(w.pools))]
					if string(pool) != string(from.pool) {
						redelegations++
					}
					w.apply(txn, stakeTxSpec{
						spend:       from,
						outputs:     []*stakeWorkloadCred{from},
						delegate:    pool,
						outputValue: value,
					})
					from.pool = pool
				case i%7 == 5 && from.reward > 0:
					w.apply(txn, stakeTxSpec{
						spend:       from,
						outputs:     []*stakeWorkloadCred{from},
						withdrawal:  from.reward,
						outputValue: value,
					})
					from.reward = 0
				default:
					w.apply(txn, stakeTxSpec{
						spend:       from,
						outputs:     []*stakeWorkloadCred{to, from},
						outputValue: value,
					})
				}
			}
		})
	}
	return redelegations
}

// stakeStateDigest hashes every row the stake and reward bookkeeping stores,
// in primary-key independent order, so two runs of the same workload can be
// compared byte for byte.
func stakeStateDigest(tb testing.TB, db *sql.DB) string {
	tb.Helper()
	queries := []string{
		`SELECT hex(staking_key), credential_tag, hex(coalesce(pool_key_hash,x'')), utxo_stake, reward_stake, total_stake, registered, pool_delegation_slot, pool_delegation_block_index, pool_delegation_cert_index, updated_slot, calculation_version FROM reward_live_stake ORDER BY credential_tag, staking_key`,
		`SELECT hex(staking_key), credential_tag, hex(coalesce(pool,x'')), hex(coalesce(drep,x'')), reward, active, added_slot FROM account ORDER BY credential_tag, staking_key`,
		`SELECT epoch, hex(pool_key_hash), credential_tag, hex(staking_key), stake, owner, registered, captured_slot, boundary_slot FROM reward_stake_input ORDER BY epoch, pool_key_hash, credential_tag, staking_key`,
		`SELECT epoch, hex(pool_key_hash), credential_tag, hex(staking_key), reward_type, amount, spendable, guarded, folded, captured_slot, boundary_slot FROM reward_account_output ORDER BY epoch, pool_key_hash, credential_tag, staking_key, reward_type`,
		`SELECT withdrawal, hex(tx_hash), credential_tag, hex(staking_key), added_slot, amount, coalesce(previous_reward,''), post_snapshot FROM account_reward_delta ORDER BY withdrawal, tx_hash, credential_tag, staking_key, added_slot`,
		`SELECT hex(staking_key), credential_tag, hex(coalesce(pool_key_hash,x'')), added_slot FROM stake_delegation ORDER BY credential_tag, staking_key, added_slot, hex(coalesce(pool_key_hash,x''))`,
		`SELECT name, seq FROM sqlite_sequence WHERE name IN ('account', 'reward_live_stake', 'account_reward_delta', 'reward_stake_input', 'reward_account_output') ORDER BY name`,
		`SELECT id, hex(staking_key), credential_tag FROM account ORDER BY id`,
		`SELECT id, hex(staking_key), credential_tag FROM reward_live_stake ORDER BY id`,
	}
	h := sha256.New()
	for _, query := range queries {
		rows, err := db.Query(query)
		require.NoError(tb, err)
		cols, err := rows.Columns()
		require.NoError(tb, err)
		var lines []string
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			require.NoError(tb, rows.Scan(ptrs...))
			lines = append(lines, fmt.Sprint(vals...))
		}
		require.NoError(tb, rows.Err())
		require.NoError(tb, rows.Close())
		if !strings.Contains(query, "ORDER BY id") {
			sort.Strings(lines)
		}
		fmt.Fprintf(h, "%s\n%d\n", query, len(lines))
		for _, l := range lines {
			fmt.Fprintln(h, l)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}
