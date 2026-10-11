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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// sharedCredentialTxFixture builds one transaction that touches the same
// stake credential three separate ways: a stake registration certificate, a
// consumed input, and a produced output. This is the overlap
// refreshRewardLiveStakeRefs's callers legitimately produce -- a wallet
// registering its own base address pays fees from and returns change to that
// same address in the same transaction -- and is what
// TestSetTransactionRefreshesSharedCredentialOnce and
// BenchmarkRefreshRewardLiveStakeAggregateRepeatedTouch exercise.
type sharedCredentialTxFixture struct {
	ref            models.StakeCredentialRef
	tx             lcommon.Transaction
	point          ocommon.Point
	certDeposits   map[int]uint64
	consumedTxID   []byte
	consumedAmount uint64
	producedAmount uint64
}

func buildSharedCredentialTx(
	t *testing.T,
	seed byte,
) sharedCredentialTxFixture {
	t.Helper()

	paymentHash := lcommon.NewBlake2b224(
		[]byte{seed, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
			0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
			0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b},
	)
	stakingHash := lcommon.NewBlake2b224(
		[]byte{seed, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a,
			0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35,
			0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b},
	)

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		paymentHash.Bytes(),
		stakingHash.Bytes(),
	)
	require.NoError(t, err)

	ref := models.NewStakeCredentialRef(0, stakingHash.Bytes())

	cert := &lcommon.StakeRegistrationCertificate{
		CertType: uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: stakingHash,
		},
	}

	consumedTxID := make([]byte, 32)
	consumedTxID[0] = seed
	consumedTxID[1] = 0xaa
	input, err := mockledger.NewTransactionInputBuilder().
		WithTxId(consumedTxID).
		WithIndex(0).
		Build()
	require.NoError(t, err)

	const producedAmount = 3_000_000
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(addr.String()).
		WithLovelace(producedAmount).
		Build()
	require.NoError(t, err)

	txID := make([]byte, 32)
	txID[0] = seed
	txID[1] = 0xbb
	// WithCertificates is a *MockTransaction-only builder method (not part
	// of the TransactionBuilder interface), so it has to run before any
	// interface-returning call narrows tx's static type; see
	// writeDepositHeldCertWithDeposits in pool_deposit_held_test.go for the
	// same pattern.
	tx := mockledger.NewTransactionBuilder().WithCertificates(cert)
	tx.WithId(txID)
	tx.WithInputs(input)
	tx.WithOutputs(output)
	tx.WithValid(true)

	point := ocommon.Point{Slot: 1000 + uint64(seed), Hash: txID}

	return sharedCredentialTxFixture{
		ref:            ref,
		tx:             tx,
		point:          point,
		certDeposits:   map[int]uint64{0: 2_000_000},
		consumedTxID:   consumedTxID,
		consumedAmount: 5_000_000,
		producedAmount: producedAmount,
	}
}

// seedConsumedUtxo inserts the live UTxO that the fixture's transaction will
// consume, under the fixture's shared stake credential, so the transaction's
// input processing has a real row to mark spent instead of hitting the
// gap-tolerant "producer not found" path.
func seedConsumedUtxo(
	t *testing.T,
	store *Store,
	fx sharedCredentialTxFixture,
) {
	t.Helper()
	_, err := store.writeDB.ExecContext(context.Background(), `
INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, added_slot, deleted_slot, amount)
VALUES (?, ?, ?, ?, ?, 0, ?)`,
		fx.consumedTxID,
		0,
		fx.ref.Key,
		int64(fx.ref.Tag),
		int64(1),
		decimalUint64(types.Uint64(fx.consumedAmount)),
	)
	require.NoError(t, err)
}

// TestSetTransactionRefreshesSharedCredentialOnce proves
// refreshRewardLiveStakeAggregate's per-credential live-UTxO scan
// (sumCredentialUtxoStake) runs at most once per transaction for a stake
// credential named by that transaction's certificate, consumed input, and
// produced output alike, instead of once per occurrence. Before the
// setTransaction merge, a certificate-refresh call ran immediately after
// applyTransactionCertificates and a second, separate refresh ran after the
// UTxO consumed/produced processing -- so the same credential here would be
// recomputed twice for one final, identical result.
func TestSetTransactionRefreshesSharedCredentialOnce(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	fx := buildSharedCredentialTx(t, 0x01)
	seedConsumedUtxo(t, store, fx)

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.SetTransaction(
		fx.tx,
		fx.point,
		0,
		fx.certDeposits,
		false,
		nil, 0,
	))
	got := store.sumCredentialUtxoStakeCalls.Load() - before
	require.Equal(
		t,
		int64(1),
		got,
		"expected exactly one sumCredentialUtxoStake call for the shared credential",
	)

	// The stored total must still reflect both UTxO mutations: the consumed
	// input's amount removed and the produced output's amount added. Merging
	// the refresh calls must not change the computed value, only how many
	// times it is computed.
	var utxoStake string
	require.NoError(t, store.writeDB.QueryRow(`
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(fx.ref.Tag), fx.ref.Key,
	).Scan(&utxoStake))
	require.Equal(
		t,
		fmt.Sprintf("%d", fx.producedAmount),
		utxoStake,
		"expected the consumed UTxO removed and the produced UTxO added",
	)

	var registered bool
	require.NoError(t, store.writeDB.QueryRow(`
SELECT registered FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(fx.ref.Tag), fx.ref.Key,
	).Scan(&registered))
	require.True(
		t,
		registered,
		"expected the stake registration certificate to be reflected",
	)
}

// TestSetTransactionSharedOuterTxnKeepsCredentialsIndependent drives two
// SetTransaction calls through one caller-managed, block-scoped *sql.Tx --
// the shape every real block application uses via (*database.Txn).Metadata(),
// obtained once per block and passed to every transaction in it -- rather
// than the nil txn every other test in this file passes (which makes
// withWriteTransaction open and commit an independent transaction per call).
// It proves two things the per-transaction, nil-txn tests cannot:
//
//  1. Merged/deduped stake-credential refs from the first SetTransaction call
//     do not carry over and affect the second call's own dedup set. Each
//     fixture uses its own distinct credential, so a leak would show up as
//     the second call's sumCredentialUtxoStake delta being something other
//     than exactly 1, or as one credential's final utxo_stake reflecting the
//     other transaction's amounts.
//  2. The Tx-scoped *sql.Stmt retention stmtForQueryer/txScopedStmt creates
//     against the shared *sql.Tx (see prepared_stmt.go) stays bounded by the
//     distinct cached queries consulted, not by how many SetTransaction calls
//     share the transaction -- the mechanism the prepared-statement cache
//     fix addressed.
func TestSetTransactionSharedOuterTxnKeepsCredentialsIndependent(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	fx1 := buildSharedCredentialTx(t, 0x01)
	fx2 := buildSharedCredentialTx(t, 0x02)
	require.NotEqual(
		t,
		fx1.ref.MapKey(),
		fx2.ref.MapKey(),
		"fixtures must use distinct credentials to prove no cross-transaction leakage",
	)
	seedConsumedUtxo(t, store, fx1)
	seedConsumedUtxo(t, store, fx2)

	ctx := context.Background()
	txn := store.Transaction(ctx)
	sqlTransaction, ok := txn.(*sqlTxn)
	require.True(t, ok)
	require.NoError(t, sqlTransaction.beginErr)

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.SetTransaction(
		fx1.tx, fx1.point, 0, fx1.certDeposits, false, txn, 0,
	))
	afterFirst := store.sumCredentialUtxoStakeCalls.Load()
	require.Equal(
		t,
		int64(1),
		afterFirst-before,
		"expected exactly one sumCredentialUtxoStake call for fx1's credential",
	)

	require.NoError(t, store.SetTransaction(
		fx2.tx, fx2.point, 0, fx2.certDeposits, false, txn, 0,
	))
	afterSecond := store.sumCredentialUtxoStakeCalls.Load()
	require.Equal(
		t,
		int64(1),
		afterSecond-afterFirst,
		"expected exactly one sumCredentialUtxoStake call for fx2's credential; "+
			"anything else means the second call's merge picked up state left "+
			"over from the first",
	)

	// Measure retention before commit closes the Tx-scoped statements out
	// from under retainedTxStmtCount.
	retained := retainedTxStmtCount(t, sqlTransaction.tx)
	require.NoError(t, txn.Commit())
	require.LessOrEqual(
		t,
		retained,
		len(hotStatements),
		"expected Tx-scoped statement retention bounded by the distinct cached "+
			"queries consulted, not by the number of SetTransaction calls "+
			"sharing this transaction",
	)

	for _, fx := range []sharedCredentialTxFixture{fx1, fx2} {
		var utxoStake string
		require.NoError(t, store.writeDB.QueryRow(`
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
			int64(fx.ref.Tag), fx.ref.Key,
		).Scan(&utxoStake))
		require.Equal(
			t,
			fmt.Sprintf("%d", fx.producedAmount),
			utxoStake,
			"expected each credential's own consumed/produced UTxOs reflected "+
				"independently of the other transaction sharing the outer txn",
		)
	}
}

// TestSetGapBlockTransactionRefreshesSharedCredentialOnce proves
// SetGapBlockTransaction dedupes a stake credential named by both a
// certificate and a produced output before refreshing -- the same class of
// redundant-refresh bug setTransaction's own mergeStakeCredentialRefs call
// fixes. Before this fix, certificateRefs and each produced output's own ref
// were appended to one shared slice with no deduplication between the two
// sources, so a credential named by both would trigger sumCredentialUtxoStake
// twice for the same final total.
func TestSetGapBlockTransactionRefreshesSharedCredentialOnce(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	fx := buildSharedCredentialTx(t, 0x05)

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.SetGapBlockTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, nil, 0,
	))
	got := store.sumCredentialUtxoStakeCalls.Load() - before
	require.Equal(
		t,
		int64(1),
		got,
		"expected exactly one sumCredentialUtxoStake call for the credential "+
			"named by both the certificate and the produced output",
	)
}

// TestSetGenesisTransactionRefreshesSharedCredentialOnce proves
// SetGenesisTransaction dedupes a stake credential repeated across multiple
// genesis outputs -- the same class of fix as
// TestSetGapBlockTransactionRefreshesSharedCredentialOnce. Before this fix,
// every output appended its own ref with no dedup at all, so a credential
// holding several genesis UTxOs (a common real pattern) would trigger
// sumCredentialUtxoStake once per output instead of once overall.
func TestSetGenesisTransactionRefreshesSharedCredentialOnce(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	stakingKey := credentialKeyForIndex(0)
	txHash := bytes.Repeat([]byte{0x09}, 32)
	blockHash := bytes.Repeat([]byte{0x0a}, 32)

	outputs := []models.Utxo{
		{
			TxId:          txHash,
			OutputIdx:     0,
			StakingKey:    stakingKey,
			CredentialTag: 0,
			Amount:        1_000_000,
		},
		{
			TxId:          txHash,
			OutputIdx:     1,
			StakingKey:    stakingKey,
			CredentialTag: 0,
			Amount:        2_000_000,
		},
	}

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(
		t,
		store.SetGenesisTransaction(txHash, blockHash, outputs, nil),
	)
	got := store.sumCredentialUtxoStakeCalls.Load() - before
	require.Equal(
		t,
		int64(1),
		got,
		"expected exactly one sumCredentialUtxoStake call for the credential "+
			"shared by two genesis outputs",
	)

	var utxoStake string
	require.NoError(t, store.writeDB.QueryRow(`
SELECT utxo_stake FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
		int64(0), stakingKey,
	).Scan(&utxoStake))
	require.Equal(
		t,
		"3000000",
		utxoStake,
		"expected both genesis outputs' amounts reflected in one merged refresh",
	)
}

// TestMergeStakeCredentialRefsDedupes covers mergeStakeCredentialRefs
// directly: overlapping credentials across and within input slices must
// collapse to one entry each, non-overlapping credentials must all survive,
// and an all-empty input must return no rows (setTransaction relies on this
// to skip refreshRewardLiveStakeRefs entirely for a transaction that touches
// no stake credential at all).
func TestMergeStakeCredentialRefsDedupes(t *testing.T) {
	t.Parallel()

	a := models.NewStakeCredentialRef(0, []byte("credential-a"))
	b := models.NewStakeCredentialRef(0, []byte("credential-b"))
	c := models.NewStakeCredentialRef(1, []byte("credential-a")) // distinct tag

	t.Run("all empty", func(t *testing.T) {
		t.Parallel()
		got := mergeStakeCredentialRefs(nil, []models.StakeCredentialRef{}, nil)
		require.Empty(t, got)
	})

	t.Run("dedupes within and across slices", func(t *testing.T) {
		t.Parallel()
		got := mergeStakeCredentialRefs(
			[]models.StakeCredentialRef{a, b},
			[]models.StakeCredentialRef{a},
			[]models.StakeCredentialRef{b, c},
		)
		require.ElementsMatch(t, []models.StakeCredentialRef{a, b, c}, got)
	})

	t.Run("no overlap keeps every entry", func(t *testing.T) {
		t.Parallel()
		got := mergeStakeCredentialRefs(
			[]models.StakeCredentialRef{a},
			[]models.StakeCredentialRef{b},
			[]models.StakeCredentialRef{c},
		)
		require.ElementsMatch(t, []models.StakeCredentialRef{a, b, c}, got)
	})
}

// BenchmarkRefreshRewardLiveStakeAggregateRepeatedTouch measures the exact
// saving TestSetTransactionRefreshesSharedCredentialOnce proves
// functionally: refreshing the same heavily used stake credential twice in
// one write transaction (the pre-fix setTransaction behavior for a
// credential named by both a certificate and a UTxO) against refreshing it
// once (the merged behavior). sumCredentialUtxoStake's cost scales with the
// credential's live UTxO count (see BenchmarkSumCredentialUtxoStake), so the
// redundant second call gets more expensive, not less, for exactly the
// heavily used addresses this matters most for.
func BenchmarkRefreshRewardLiveStakeAggregateRepeatedTouch(b *testing.B) {
	for _, n := range []int{100, 1_000, 7_000} {
		store := newMigratedSQLiteStore(b)
		ref := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
		amounts := make([]uint64, n)
		for i := range amounts {
			amounts[i] = uint64(1_000_000 + i)
		}
		seedCredentialUtxos(b, store, n, ref, amounts, nil)

		b.Run(fmt.Sprintf("n=%d/refreshed_twice_unmerged", n), func(b *testing.B) {
			for b.Loop() {
				err := store.withWriteTransaction(
					nil,
					func(db queryer, ctx context.Context) error {
						if err := store.refreshRewardLiveStakeAggregate(ctx, db, ref, 1); err != nil {
							return err
						}
						return store.refreshRewardLiveStakeAggregate(ctx, db, ref, 1)
					},
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/refreshed_once_merged", n), func(b *testing.B) {
			for b.Loop() {
				err := store.withWriteTransaction(
					nil,
					func(db queryer, ctx context.Context) error {
						return store.refreshRewardLiveStakeAggregate(ctx, db, ref, 1)
					},
				)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestSetTransactionIncrementalDeltaMatchesFullScan drives the production
// entry point (SetTransaction, not the internal helper directly) for a
// credential whose running total is already warm -- established the way an
// earlier block's touch would -- so this transaction's own certificate,
// consumed input, and produced output all take
// refreshRewardLiveStakeAggregateDelta's incremental path together, exactly
// as setTransactionWithAccumulator wires them. It proves the result matches
// a fresh authoritative scan and that no full scan ran.
func TestSetTransactionIncrementalDeltaMatchesFullScan(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x07)
	seedConsumedUtxo(t, store, fx)
	establishRunningTotal(t, store, fx.ref, 1)
	require.Equal(
		t,
		fx.consumedAmount,
		readUtxoStake(t, store, fx.ref),
		"baseline must include the UTxO this transaction is about to spend",
	)

	before := store.sumCredentialUtxoStakeCalls.Load()
	require.NoError(t, store.SetTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil, 0,
	))
	require.Equal(
		t,
		before,
		store.sumCredentialUtxoStakeCalls.Load(),
		"a warm running total must not fall back to a full scan through "+
			"SetTransaction",
	)

	got := readUtxoStake(t, store, fx.ref)
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, want, got, "must match a fresh authoritative scan")
	require.Equal(t, fx.producedAmount, got)
}

func TestSetTransactionBatchedInputsReturnConsumedStake(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x31)
	secondTxID := bytes.Repeat([]byte{0x32}, 32)
	secondInput, err := mockledger.NewTransactionInputBuilder().
		WithTxId(secondTxID).
		WithIndex(0).
		Build()
	require.NoError(t, err)

	txID := bytes.Repeat([]byte{0x33}, 32)
	tx, err := mockledger.NewTransactionBuilder().
		WithId(txID).
		WithInputs(fx.tx.Consumed()[0], secondInput).
		WithOutputs(fx.tx.Produced()[0].Output).
		WithValid(true).
		Build()
	require.NoError(t, err)
	seedConsumedUtxo(t, store, fx)
	_, err = store.writeDB.ExecContext(ctx, `
INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, added_slot, deleted_slot, amount)
VALUES (?, 0, ?, ?, 1, 0, ?)`,
		secondTxID,
		fx.ref.Key,
		int64(fx.ref.Tag),
		decimalUint64(types.Uint64(7_000_000)),
	)
	require.NoError(t, err)
	establishRunningTotal(t, store, fx.ref, 1)
	require.Equal(t, uint64(12_000_000), readUtxoStake(t, store, fx.ref))

	point := ocommon.Point{Slot: fx.point.Slot + 1, Hash: txID}
	require.NoError(t, store.SetTransaction(
		tx, point, 0, nil, false, nil, 0,
	))
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, uint64(3_000_000), want)
	require.Equal(t, want, readUtxoStake(t, store, fx.ref))
}

// TestSetTransactionReapplyAppliesNoSecondDelta covers the invariant the
// incremental path rests on: a delta must state the change this write made to
// the utxo table, not the change the transaction describes. Re-applying an
// already-stored transaction mutates nothing -- the produced output collides
// with insertUtxoQueryIgnoreConflict's ON CONFLICT DO NOTHING, and the
// consumed input's UPDATE matches no row because this same transaction
// already spent it -- so the credential's running total must not move.
//
// Counting those no-op mutations again drives the stored value away from the
// authoritative scan in both directions at once (a spurious gain for the
// output, a spurious loss for the input), and a loss larger than the
// credential's recorded total fails applyUtxoStakeDelta's underflow guard,
// which aborts block application rather than merely reporting wrong stake.
func TestSetTransactionReapplyAppliesNoSecondDelta(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x41)
	seedConsumedUtxo(t, store, fx)
	establishRunningTotal(t, store, fx.ref, 1)

	require.NoError(t, store.SetTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil, 0,
	))
	require.Equal(t, fx.producedAmount, readUtxoStake(t, store, fx.ref))

	require.NoError(t, store.SetTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil, 0,
	))
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, fx.producedAmount, want, "the utxo table must not move")
	require.Equal(
		t,
		want,
		readUtxoStake(t, store, fx.ref),
		"a re-applied transaction must not move the running total",
	)
}

// TestSetTransactionLeiosClosureSkippedInputAppliesNoDelta covers the same
// invariant on the Leios closure path, where an input already spent by a
// *different* certified endorser-block transaction is deliberately a no-op
// (see setTransactionWithAccumulator's tolerateConsumedInputConflict branch).
// The row stays deleted with its original spender, so this write removed no
// live stake and must subtract none.
func TestSetTransactionLeiosClosureSkippedInputAppliesNoDelta(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x43)
	seedConsumedUtxo(t, store, fx)

	otherSpender := make([]byte, 32)
	otherSpender[0] = 0xfe
	_, err := store.writeDB.ExecContext(ctx, `
UPDATE utxo SET deleted_slot = 900, spent_at_tx_id = ?
WHERE tx_id = ? AND output_idx = 0`, otherSpender, fx.consumedTxID)
	require.NoError(t, err)

	// A second live UTxO keeps the credential's reward_live_stake row in
	// place, so the write exercises the warm incremental path rather than
	// refreshRewardLiveStakeAggregateDelta's cold-start full-scan fallback.
	const retained = 9_000_000
	otherTx := make([]byte, 32)
	otherTx[0] = 0x77
	require.NoError(t, store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			return insertLiveUtxoTx(ctx, db, fx.ref, otherTx[0], retained)
		},
	))
	establishRunningTotal(t, store, fx.ref, 1)
	require.Equal(t, uint64(retained), readUtxoStake(t, store, fx.ref))

	require.NoError(t, store.SetTransactionLeiosClosure(
		fx.tx, fx.point, 0, fx.certDeposits, false, nil, 0,
	))
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, uint64(retained+fx.producedAmount), want)
	require.Equal(
		t,
		want,
		readUtxoStake(t, store, fx.ref),
		"an already-spent input must contribute no loss delta",
	)
}

// TestSetGapBlockTransactionReapplyAppliesNoSecondDelta is the same check for
// SetGapBlockTransaction, the other caller on the incremental path. It is the
// likeliest place for a colliding produced output in practice: gap closure
// replays blocks whose outputs a Mithril snapshot import may already have
// created.
func TestSetGapBlockTransactionReapplyAppliesNoSecondDelta(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	fx := buildSharedCredentialTx(t, 0x44)

	require.NoError(t, store.SetGapBlockTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, nil, 0,
	))
	first := readUtxoStake(t, store, fx.ref)
	require.Equal(t, fx.producedAmount, first)

	require.NoError(t, store.SetGapBlockTransaction(
		fx.tx, fx.point, 0, fx.certDeposits, nil, 0,
	))
	want, err := store.sumCredentialUtxoStake(ctx, store.writeDB, fx.ref)
	require.NoError(t, err)
	require.Equal(t, fx.producedAmount, want, "the utxo table must not move")
	require.Equal(
		t,
		want,
		readUtxoStake(t, store, fx.ref),
		"a replayed gap block must not double-count its produced outputs",
	)
}

// fakePrepareOnlyQueryer implements queryer with a PrepareContext that
// always fails, so insertTransaction returns before touching the *sql.Stmt
// it would otherwise store -- this test only needs to observe the dialect
// flag insertTransaction sets before calling PrepareContext, not to execute
// a real statement.
type fakePrepareOnlyQueryer struct {
	prepareErr error
}

// TestInsertTransactionDetectsMySQLThroughCountingQueryer is the regression
// test for the type assertion insertTransaction uses to detect a MySQL
// dialect: db.(dialectQueryer) alone misses a dialectQueryer wrapped in
// countingQueryer, which is exactly what every real caller passes once
// Config.PromRegistry is set (see Store.instrumentedQueryer). Without
// unwrapDialectQueryer, a.mysql stays false on a metrics-enabled MySQL
// store, and insertTransaction takes the RETURNING-id QueryRowContext path
// MySQL cannot serve instead of the ExecContext/LastInsertId path this test
// proves gets selected.
func TestInsertTransactionDetectsMySQLThroughCountingQueryer(t *testing.T) {
	t.Parallel()
	prepareErr := errors.New("prepare not needed for this assertion")
	inner := dialectQueryer{
		queryer: fakePrepareOnlyQueryer{prepareErr: prepareErr},
		dialect: "mysql",
	}
	wrapped := countingQueryer{queryer: inner, counter: nil}

	acc := &transactionBatchAccumulator{}
	_, _, err := acc.insertTransaction(context.Background(), wrapped)
	require.ErrorIs(t, err, prepareErr)
	require.True(
		t,
		acc.mysql,
		"expected insertTransaction to detect the mysql dialect through countingQueryer",
	)
}

func TestInsertTransactionReportsFreshAndReplayedRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	txn := store.Transaction(context.Background())
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	acc := &transactionBatchAccumulator{}
	defer acc.Reset()

	args := []any{
		[]byte{0x10}, []byte{0x20}, []byte{0x30}, 1, 2,
		"3", "4", "5", 6, true,
	}
	id, isNew, err := acc.insertTransaction(ctx, db, args...)
	require.NoError(t, err)
	require.True(t, isNew)
	require.NotZero(t, id)

	replayArgs := append([]any(nil), args...)
	replayArgs[1] = []byte{0x21}
	replayArgs[3] = uint64(7)
	replayArgs[6] = "8"
	replayArgs[8] = uint32(9)
	replayID, isNew, err := acc.insertTransaction(ctx, db, replayArgs...)
	require.NoError(t, err)
	require.False(t, isNew)
	require.Equal(t, id, replayID)

	var (
		blockHash  []byte
		slot       uint64
		collateral string
		blockIndex uint32
	)
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT block_hash, slot, collateral_fee, block_index
		 FROM "transaction" WHERE id = ?`, id,
	).Scan(&blockHash, &slot, &collateral, &blockIndex))
	require.Equal(t, []byte{0x21}, blockHash)
	require.Equal(t, uint64(7), slot)
	require.Equal(t, "8", collateral)
	require.Equal(t, uint32(9), blockIndex)
	require.NoError(t, txn.Rollback())
}

func TestTransactionBatchAccumulatorResetClosesStatement(t *testing.T) {
	store := newMigratedSQLiteStore(t)
	txn := store.Transaction(context.Background())
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)

	acc := &transactionBatchAccumulator{}
	oldStmtArgs := []any{
		[]byte{0x01}, []byte{0x02}, nil, 1, 0,
		"0", "0", "0", 0, true,
	}
	_, _, err = acc.insertTransaction(ctx, db, oldStmtArgs...)
	require.NoError(t, err)
	stmt := acc.transactionInsert
	require.NotNil(t, stmt)

	acc.Reset()
	require.Nil(t, acc.transactionInsert)
	_, err = stmt.ExecContext(ctx, oldStmtArgs...)
	require.Error(t, err, "reset must close statements bound to the batch transaction")

	require.NoError(t, txn.Rollback())
	var count int
	require.NoError(t, store.readDB.QueryRowContext(
		context.Background(),
		`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`,
		[]byte{0x01},
	).Scan(&count))
	require.Zero(t, count, "rollback must discard writes made through the accumulator")
}

// feelessTransaction stands in for a transaction body whose Fee is nil, which
// is what TransactionBodyBase returns for any body that does not override it --
// the synthetic transaction carrying imported certificates among them. Fee is
// overridden here only so a single type can cover both the nil and non-nil
// cases; the write-path reproduction lives in ledgerstate.
type feelessTransaction struct {
	lcommon.TransactionBodyBase
	fee *big.Int
}

// TestTransactionFeeTreatsNilAsZero unit-tests the accessor. It does not by
// itself prove the write path is guarded -- reverting the setTransaction call
// site leaves this green -- so the reproduction that exercises
// persistImportedCommitteeCertificates end to end lives in
// ledgerstate/tests_test.go.
func TestTransactionFeeTreatsNilAsZero(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() {
		require.Equal(
			t,
			uint64(0),
			uint64(transactionFee(&feelessTransaction{})),
		)
	})

	// A real fee is still recorded unchanged.
	require.Equal(
		t,
		uint64(174301),
		uint64(transactionFee(
			&feelessTransaction{fee: big.NewInt(174301)},
		)),
	)
}
