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
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// The scenario below holds one asset across several UTxOs, an empty asset
// name, a spent-but-unpruned holding and the quantities at the edges of a
// 64-bit unsigned amount. Every write path must leave a store that every read
// site answers identically.

type parityAsset struct {
	policy byte
	name   string
	amount uint64
}

type parityInput struct {
	seed byte
	idx  uint32
}

type parityOutput struct {
	address int
	assets  []parityAsset
}

type parityTx struct {
	seed   byte
	slot   uint64
	spends []parityInput
	outs   []parityOutput
}

func parityPolicy(b byte) []byte { return bytes.Repeat([]byte{b}, 28) }

func parityTxID(seed byte) []byte {
	id := make([]byte, 32)
	id[0] = seed
	id[1] = 0xaa
	return id
}

// parityAddresses are two base addresses with distinct credentials.
func parityAddresses(t testing.TB) []lcommon.Address {
	t.Helper()
	ret := make([]lcommon.Address, 0, 2)
	for _, seed := range []byte{0x71, 0x72} {
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey,
			lcommon.AddressNetworkTestnet,
			bytes.Repeat([]byte{seed}, 28),
			bytes.Repeat([]byte{seed + 0x10}, 28),
		)
		require.NoError(t, err)
		ret = append(ret, addr)
	}
	return ret
}

// parityStages returns the transactions applied in separate write
// transactions. Stage two spends an output stage one created, so the spent
// holding is retained rather than elided inside one batch.
func parityStages() [][]parityTx {
	return [][]parityTx{
		{
			{seed: 0xa1, slot: 100, outs: []parityOutput{
				{address: 0, assets: []parityAsset{
					{1, "", 1},
					{1, "tok", 2},
					{2, "x", 1 << 63},
				}},
				{address: 0, assets: []parityAsset{
					{1, "tok", 3},
					{3, "big", math.MaxUint64},
				}},
				{address: 1, assets: []parityAsset{
					{4, "m", math.MaxInt64},
				}},
			}},
			{seed: 0xa2, slot: 101, outs: []parityOutput{
				{address: 1, assets: []parityAsset{
					{1, "tok", 4},
					{1, "", 5},
				}},
			}},
		},
		{
			{seed: 0xa3, slot: 102,
				spends: []parityInput{{seed: 0xa1, idx: 1}},
				outs: []parityOutput{
					{address: 0, assets: []parityAsset{{1, "tok", 6}}},
				}},
		},
	}
}

type parityAssetKey struct {
	policy byte
	name   string
}

func parityAssetKeys() []parityAssetKey {
	return []parityAssetKey{
		{1, ""}, {1, "tok"}, {2, "x"}, {3, "big"}, {4, "m"}, {9, "none"},
	}
}

func (tx parityTx) ledger(
	t testing.TB,
	addrs []lcommon.Address,
) lcommon.Transaction {
	t.Helper()
	outputs := make([]lcommon.TransactionOutput, 0, len(tx.outs))
	for _, out := range tx.outs {
		builder := mockledger.NewTransactionOutputBuilder().
			WithAddress(addrs[out.address].String()).
			WithLovelace(2_000_000)
		assets := make([]mockledger.Asset, 0, len(out.assets))
		for _, asset := range out.assets {
			assets = append(assets, mockledger.Asset{
				PolicyId:  parityPolicy(asset.policy),
				AssetName: []byte(asset.name),
				Amount:    asset.amount,
			})
		}
		builder = builder.WithAssets(assets...)
		built, err := builder.Build()
		require.NoError(t, err)
		outputs = append(outputs, built)
	}
	inputs := make([]lcommon.TransactionInput, 0, len(tx.spends))
	for _, spend := range tx.spends {
		input, err := mockledger.NewTransactionInputBuilder().
			WithTxId(parityTxID(spend.seed)).WithIndex(spend.idx).Build()
		require.NoError(t, err)
		inputs = append(inputs, input)
	}
	builder := mockledger.NewTransactionBuilder()
	builder.WithId(parityTxID(tx.seed))
	builder.WithInputs(inputs...)
	builder.WithOutputs(outputs...)
	builder.WithValid(true)
	return builder
}

func (tx parityTx) point() ocommon.Point {
	return ocommon.Point{Slot: tx.slot, Hash: parityTxID(tx.seed)}
}

// parityModels is the importer's view of every output in stages: spent
// outputs carry the spending transaction's slot.
func parityModels(
	t testing.TB,
	addrs []lcommon.Address,
	stages [][]parityTx,
) []models.Utxo {
	t.Helper()
	spentAt := map[parityInput]uint64{}
	for _, stage := range stages {
		for _, tx := range stage {
			for _, spend := range tx.spends {
				spentAt[spend] = tx.slot
			}
		}
	}
	var ret []models.Utxo
	for _, stage := range stages {
		for _, tx := range stage {
			for i, out := range tx.outs {
				addr := addrs[out.address]
				model := models.Utxo{
					TxId:          parityTxID(tx.seed),
					OutputIdx:     uint32(i), //nolint:gosec
					PaymentKey:    addr.PaymentKeyHash().Bytes(),
					StakingKey:    addr.StakeKeyHash().Bytes(),
					CredentialTag: 0,
					Address:       []byte(addr.String()),
					AddedSlot:     tx.slot,
					Amount:        2_000_000,
					DeletedSlot: spentAt[parityInput{
						seed: tx.seed, idx: uint32(i), //nolint:gosec
					}],
				}
				for _, asset := range out.assets {
					model.Assets = append(model.Assets, models.Asset{
						PolicyId: parityPolicy(asset.policy),
						Name:     []byte(asset.name),
						Amount:   types.Uint64(asset.amount),
					})
				}
				ret = append(ret, model)
			}
		}
	}
	return ret
}

type parityPath struct {
	name string
	// coreOnly paths need the core storage mode's overlay.
	coreOnly bool
	// ledgerBacked paths write transaction rows too.
	ledgerBacked bool
	apply        func(t *testing.T, store *Store, api bool)
}

func applyParityStage(
	t *testing.T,
	store *Store,
	stage []parityTx,
	apply func(txn types.Txn, acc types.MetadataBatchAccumulator, tx parityTx),
	enableOverlay bool,
) {
	t.Helper()
	txn := store.Transaction(context.Background())
	acc := store.NewBatchAccumulator()
	if enableOverlay {
		var inputs []models.UtxoId
		created := map[parityInput]struct{}{}
		for _, tx := range stage {
			for i := range tx.outs {
				created[parityInput{seed: tx.seed, idx: uint32(i)}] = struct{}{} //nolint:gosec
			}
		}
		for _, tx := range stage {
			for _, spend := range tx.spends {
				if _, ok := created[spend]; !ok {
					inputs = append(inputs, models.UtxoId{
						Hash: parityTxID(spend.seed), Idx: spend.idx,
					})
				}
			}
		}
		enabled, err := store.EnableBatchUtxoOverlay(acc, txn, inputs, nil, nil)
		require.NoError(t, err)
		require.True(t, enabled)
	}
	for _, tx := range stage {
		apply(txn, acc, tx)
	}
	require.NoError(t, store.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
}

func parityPaths() []parityPath {
	ledgerStages := func(
		t *testing.T,
		store *Store,
		apply func(types.Txn, types.MetadataBatchAccumulator, parityTx),
		overlay bool,
	) {
		for _, stage := range parityStages() {
			applyParityStage(t, store, stage, apply, overlay)
		}
	}
	return []parityPath{
		{
			name: "per-row", ledgerBacked: true,
			apply: func(t *testing.T, store *Store, _ bool) {
				addrs := parityAddresses(t)
				for _, stage := range parityStages() {
					for _, tx := range stage {
						require.NoError(t, store.SetTransaction(
							tx.ledger(t, addrs), tx.point(), 0, nil, true,
							nil, 0,
						))
					}
				}
			},
		},
		{
			name: "batched", ledgerBacked: true,
			apply: func(t *testing.T, store *Store, api bool) {
				addrs := parityAddresses(t)
				ledgerStages(t, store,
					func(txn types.Txn, acc types.MetadataBatchAccumulator, tx parityTx) {
						if api {
							require.NoError(t, store.SetTransactionBatchedHistorical(
								tx.ledger(t, addrs), tx.point(), 0, nil, true,
								true, acc, txn, 0,
							))
							return
						}
						require.NoError(t, store.SetTransactionBatched(
							tx.ledger(t, addrs), tx.point(), 0, nil, true,
							acc, txn, 0,
						))
					}, false)
			},
		},
		{
			name: "overlay", coreOnly: true, ledgerBacked: true,
			apply: func(t *testing.T, store *Store, _ bool) {
				addrs := parityAddresses(t)
				ledgerStages(t, store,
					func(txn types.Txn, acc types.MetadataBatchAccumulator, tx parityTx) {
						require.NoError(t, store.SetTransactionBatched(
							tx.ledger(t, addrs), tx.point(), 0, nil, true,
							acc, txn, 0,
						))
					}, true)
			},
		},
		{
			name: "import",
			apply: func(t *testing.T, store *Store, _ bool) {
				addrs := parityAddresses(t)
				require.NoError(t, store.ImportUtxos(
					parityModels(t, addrs, parityStages()), nil,
				))
			},
		},
	}
}

func parityStore(t *testing.T, api bool) *Store {
	t.Helper()
	if api {
		return newAPIModeSQLiteStore(t, nil)
	}
	return newMigratedSQLiteStore(t)
}

func parityAssetsString(assets []models.Asset) string {
	lines := make([]string, 0, len(assets))
	for _, asset := range assets {
		lines = append(lines, fmt.Sprintf(
			"%x/%x=%d", asset.PolicyId, asset.Name, uint64(asset.Amount),
		))
	}
	sort.Strings(lines)
	return strings.Join(lines, ",")
}

func parityUtxosString(utxos []models.Utxo) []string {
	lines := make([]string, 0, len(utxos))
	for _, utxo := range utxos {
		lines = append(lines, fmt.Sprintf(
			"%x#%d deleted=%d [%s]", utxo.TxId, utxo.OutputIdx,
			utxo.DeletedSlot, parityAssetsString(utxo.Assets),
		))
	}
	sort.Strings(lines)
	return lines
}

// paritySnapshot answers every asset read site the store exposes and renders
// the answers, identity bytes and amounts only, as text. ledgerBacked adds
// the reads that need transaction rows.
func paritySnapshot(t *testing.T, store *Store, api, ledgerBacked bool) string {
	t.Helper()
	var out strings.Builder
	addrs := parityAddresses(t)
	section := func(name string, lines ...string) {
		fmt.Fprintf(&out, "## %s\n%s\n", name, strings.Join(lines, "\n"))
	}
	result := func(err error) string {
		if err != nil {
			return "error: " + err.Error()
		}
		return "ok"
	}
	for _, key := range parityAssetKeys() {
		policy := parityPolicy(key.policy)
		name := []byte(key.name)
		label := fmt.Sprintf("%d/%q", key.policy, key.name)

		quantity, err := store.GetAssetQuantityByPolicyAndName(
			lcommon.NewBlake2b224(policy), name, nil,
		)
		section("quantity "+label, fmt.Sprint(quantity), result(err))

		byAsset, err := store.GetUtxosByAssets(policy, name, nil)
		section("utxos-by-asset "+label,
			append(parityUtxosString(byAsset), result(err))...)

		history, err := store.GetUtxosWithHistory(&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			FilterByAsset:     true,
			AssetPolicyID:     policy,
			AssetName:         name,
			Status:            models.UtxoHistoryStatusAll,
		}, nil)
		var historyUtxos []models.Utxo
		for _, item := range history {
			historyUtxos = append(historyUtxos, item.Utxo)
		}
		section("history-by-asset "+label,
			append(parityUtxosString(historyUtxos), result(err))...)

		if ledgerBacked {
			txs, err := store.GetAssetTransactions(
				lcommon.NewBlake2b224(policy), name, nil, nil, 0, 0, false,
				nil,
			)
			lines := make([]string, 0, len(txs)+1)
			for _, tx := range txs {
				lines = append(lines, fmt.Sprintf("%x@%d", tx.Hash, tx.Slot))
			}
			section("transactions "+label, append(lines, result(err))...)
		}
		if api {
			holders, total, err := store.GetAssetHolders(
				models.AssetHolderQuery{
					PolicyID: policy, AssetName: name, Limit: 100,
				}, nil,
			)
			lines := make([]string, 0, len(holders)+2)
			for _, holder := range holders {
				lines = append(lines, fmt.Sprintf(
					"%s=%d", holder.Address, holder.Quantity,
				))
			}
			section("holders "+label,
				append(lines, fmt.Sprint(total), result(err))...)
		}
	}
	policyOnly, err := store.GetUtxosByAssets(parityPolicy(1), nil, nil)
	section("utxos-by-policy 1",
		append(parityUtxosString(policyOnly), result(err))...)

	for i, addr := range addrs {
		stake := addr.StakeKeyHash().Bytes()
		balances, total, err := store.GetAccountAssetBalances(
			0, stake, 100, 0, "asc", models.AccountAggregateLimits{}, nil,
		)
		lines := make([]string, 0, len(balances)+2)
		for _, balance := range balances {
			lines = append(lines, fmt.Sprintf(
				"%x/%x=%s", balance.PolicyID, balance.Name, balance.Quantity,
			))
		}
		section(fmt.Sprintf("account %d", i),
			append(lines, fmt.Sprint(total), result(err))...)
		if !api {
			continue
		}
		balance, err := store.GetUtxoBalanceByAddress(
			addr, models.UtxoAddressMatchPaymentCred,
			models.AddressBalanceLimits{}, nil,
		)
		lines = lines[:0]
		for _, asset := range balance.Assets {
			lines = append(lines, fmt.Sprintf(
				"%x/%x=%d", asset.PolicyId, asset.Name, asset.Amount,
			))
		}
		section(fmt.Sprintf("address-balance %d", i),
			append(lines, fmt.Sprint(balance.UtxoCount), result(err))...)
		utxos, err := store.GetUtxosByAddress(
			[]models.UtxoAddressPattern{{
				PaymentPart:    addr.PaymentKeyHash().Bytes(),
				DelegationPart: addr.StakeKeyHash().Bytes(),
			}}, 100, nil,
		)
		section(fmt.Sprintf("utxos-by-address %d", i),
			append(parityUtxosString(utxos), result(err))...)
	}
	return out.String()
}

// TestAssetReadsAgreeAcrossWritePaths writes the scenario through every write
// path and requires each read site to answer the same, then pins the answers
// that matter against literals so agreement cannot hide a shared defect.
func TestAssetReadsAgreeAcrossWritePaths(t *testing.T) {
	t.Parallel()
	for _, api := range []bool{false, true} {
		mode := "core"
		if api {
			mode = "api"
		}
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			snapshots := map[string]string{}
			for _, path := range parityPaths() {
				if path.coreOnly && api {
					continue
				}
				store := parityStore(t, api)
				path.apply(t, store, api)
				snapshots[path.name] = paritySnapshot(
					t, store, api, path.ledgerBacked,
				)
			}
			require.NotEmpty(t, snapshots)
			var reference string
			for _, name := range []string{"per-row", "batched", "overlay"} {
				snapshot, ok := snapshots[name]
				if !ok {
					continue
				}
				if reference == "" {
					reference = snapshot
					continue
				}
				require.Equal(t, reference, snapshot, "%s differs", name)
			}
			// The importer writes no transaction rows, so compare it on
			// the reads that do not need them.
			importStore := parityStore(t, api)
			for _, path := range parityPaths() {
				if path.name == "import" {
					path.apply(t, importStore, api)
				}
			}
			require.Equal(t,
				paritySnapshot(t, importStore, api, false),
				stripTransactions(reference),
			)

			require.Contains(t, reference, "## quantity 1/\"tok\"\n12\nok",
				"live tok: 2 + 4 + 6, with the spent 3 excluded")
			require.Contains(t, reference, "## quantity 1/\"\"\n6\nok")
			require.Contains(t, reference,
				fmt.Sprintf("## quantity 2/\"x\"\n%d\nok", uint64(1)<<63))
			require.Contains(t, reference, "## quantity 3/\"big\"\n0\nok",
				"the only holding is spent")
			require.Contains(t, reference,
				fmt.Sprintf("626967=%d]", uint64(math.MaxUint64)),
				"the spent holding stays readable through history")
			require.Contains(t, reference,
				fmt.Sprintf("## quantity 4/\"m\"\n%d\nok", int64(math.MaxInt64)))
			require.Contains(t, reference, "## quantity 9/\"none\"\n0\nok")
		})
	}
}

// stripTransactions drops the transaction sections of a snapshot.
func stripTransactions(snapshot string) string {
	var out strings.Builder
	for _, section := range strings.SplitAfter(snapshot, "\n## ") {
		if strings.HasPrefix(strings.TrimPrefix(section, "## "), "transactions ") {
			continue
		}
		out.WriteString(section)
	}
	return out.String()
}

// TestAssetReadsAgreeAfterRollback rolls every write path back to slot 100,
// removing the later outputs and restoring the output spent at slot 102, and
// requires the reads to agree and to forget the removed holdings.
func TestAssetReadsAgreeAfterRollback(t *testing.T) {
	t.Parallel()
	var reference string
	for _, path := range parityPaths() {
		store := newForeignKeySQLiteStore(t)
		path.apply(t, store, false)
		require.NoError(t, store.DeleteUtxosAfterSlot(100, nil))
		require.NoError(t, store.SetUtxosNotDeletedAfterSlot(100, nil))
		snapshot := stripTransactions(paritySnapshot(t, store, false, false))
		if reference == "" {
			reference = snapshot
			continue
		}
		require.Equal(t, reference, snapshot, "%s differs", path.name)
	}
	require.Contains(t, reference, "## quantity 1/\"\"\n1\nok",
		"only the slot-100 empty-name holding survives")
	require.Contains(t, reference, "## quantity 1/\"tok\"\n5\nok",
		"only the slot-100 tok holdings survive, the restored one included")
	require.Contains(t, reference,
		fmt.Sprintf("## quantity 3/\"big\"\n%d\nok", uint64(math.MaxUint64)),
		"the holding spent at slot 102 is live again")
}

// TestCreateUtxoAssetsReadBack covers the model-level insert path.
func TestCreateUtxoAssetsReadBack(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	for _, item := range parityModels(t, parityAddresses(t), parityStages()) {
		utxo := item
		require.NoError(t, store.CreateUtxo(nil, &utxo))
	}
	imported := parityStore(t, false)
	for _, path := range parityPaths() {
		if path.name == "import" {
			path.apply(t, imported, false)
		}
	}
	require.Equal(t,
		paritySnapshot(t, imported, false, false),
		paritySnapshot(t, store, false, false),
	)
}
