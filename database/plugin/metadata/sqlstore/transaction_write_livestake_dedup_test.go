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
	"fmt"
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
		nil,
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
