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

package blockfrost

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/stretchr/testify/require"
)

func testKeyAddress(
	t *testing.T,
	paymentByte byte,
	stakeByte byte,
) (lcommon.Address, []byte, []byte) {
	t.Helper()
	payment := bytes.Repeat([]byte{paymentByte}, lcommon.AddressHashSize)
	var staking []byte
	addrType := uint8(lcommon.AddressTypeKeyNone)
	if stakeByte != 0 {
		staking = bytes.Repeat([]byte{stakeByte}, lcommon.AddressHashSize)
		addrType = lcommon.AddressTypeKeyKey
	}
	addr, err := lcommon.NewAddressFromParts(
		addrType,
		lcommon.AddressNetworkTestnet,
		payment,
		staking,
	)
	require.NoError(t, err)
	return addr, payment, staking
}

// seedAddressOutput stores one output of a transaction, its output CBOR, and
// optionally marks it spent by another transaction.
func seedAddressOutput(
	t *testing.T,
	raw *sql.DB,
	db *database.Database,
	addr lcommon.Address,
	txHash []byte,
	slot uint64,
	amount uint64,
	assets []models.Asset,
	spentBy []byte,
) *models.Transaction {
	t.Helper()
	tx := &models.Transaction{
		Hash:  txHash,
		Slot:  slot,
		Valid: true,
		Outputs: []models.Utxo{{
			TxId:          txHash,
			OutputIdx:     0,
			PaymentKey:    addr.PaymentKeyHash().Bytes(),
			PaymentScript: models.PaymentScriptFromAddress(addr),
			StakingKey:    stakingKeyOf(addr),
			CredentialTag: 0,
			AddedSlot:     slot,
			Amount:        types.Uint64(amount),
			Assets:        assets,
		}},
	}
	insertAdapterTransaction(t, raw, tx)
	storePointerOutputCbor(t, db, txHash, 0, addr, amount)
	if spentBy != nil {
		_, err := raw.Exec(
			`UPDATE utxo SET spent_at_tx_id = ?, deleted_slot = ? WHERE tx_id = ?`,
			spentBy,
			slot+1,
			txHash,
		)
		require.NoError(t, err)
	}
	return tx
}

func stakingKeyOf(addr lcommon.Address) []byte {
	if addr.StakeKeyHash() == lcommon.NewBlake2b224(nil) {
		return nil
	}
	return addr.StakeKeyHash().Bytes()
}

func linkAddressTransaction(
	t *testing.T,
	raw *sql.DB,
	addr lcommon.Address,
	transactionID uint,
	slot uint64,
) {
	t.Helper()
	_, err := raw.Exec(`
INSERT INTO address_transaction (
    payment_key, payment_script, staking_key, credential_tag,
    transaction_id, slot, tx_index
) VALUES (?, ?, ?, 0, ?, ?, 0)`,
		addr.PaymentKeyHash().Bytes(), models.PaymentScriptFromAddress(addr),
		stakingKeyOf(addr), transactionID, slot,
	)
	require.NoError(t, err)
}

func TestNodeAdapterAddressAssetUTXOs(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0x71, 0)
	policy := bytes.Repeat([]byte{0x72}, lcommon.AddressHashSize)
	other := bytes.Repeat([]byte{0x73}, lcommon.AddressHashSize)

	seedAddressOutput(t, raw, db, addr, fill32(0x01), 1, 2_000_000,
		[]models.Asset{{PolicyId: policy, Name: []byte("A"), Amount: 5}}, nil)
	seedAddressOutput(t, raw, db, addr, fill32(0x02), 2, 3_000_000,
		[]models.Asset{{PolicyId: policy, Name: []byte("B"), Amount: 6}}, nil)
	seedAddressOutput(t, raw, db, addr, fill32(0x03), 3, 4_000_000,
		[]models.Asset{{PolicyId: policy, Name: []byte("A"), Amount: 7}}, nil)
	seedAddressOutput(t, raw, db, addr, fill32(0x04), 4, 5_000_000, nil, nil)
	// A spent holding of the asset is not a current UTxO.
	insertAdapterTransaction(t, raw, &models.Transaction{
		Hash: fill32(0x99), Slot: 6, Valid: true,
	})
	seedAddressOutput(t, raw, db, addr, fill32(0x05), 5, 6_000_000,
		[]models.Asset{{PolicyId: policy, Name: []byte("A"), Amount: 9}},
		fill32(0x99))
	// Another address holding the same asset is not this address's UTxO.
	otherAddr, _, _ := testKeyAddress(t, 0x74, 0)
	seedAddressOutput(t, raw, db, otherAddr, fill32(0x06), 6, 1_000_000,
		[]models.Asset{{PolicyId: policy, Name: []byte("A"), Amount: 1}}, nil)
	_ = other

	ctx := context.Background()
	rows, total, err := adapter.AddressAssetUTXOs(
		ctx, addr.String(), hex.EncodeToString(policy), []byte("A"),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, rows, 2)
	require.Equal(t, hex.EncodeToString(fill32(0x01)), rows[0].TxHash)
	require.Equal(t, hex.EncodeToString(fill32(0x03)), rows[1].TxHash)
	require.Equal(t, addr.String(), rows[0].Address)
	unit := hex.EncodeToString(policy) + hex.EncodeToString([]byte("A"))
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "2000000"},
		{Unit: unit, Quantity: "5"},
	}, rows[0].Amount)

	rows, total, err = adapter.AddressAssetUTXOs(
		ctx, addr.String(), hex.EncodeToString(policy), []byte("A"),
		PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc},
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, rows, 1)
	require.Equal(t, hex.EncodeToString(fill32(0x03)), rows[0].TxHash)

	rows, total, err = adapter.AddressAssetUTXOs(
		ctx, addr.String(), hex.EncodeToString(policy), []byte("missing"),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	require.Zero(t, total)
	require.Empty(t, rows)

	_, _, err = adapter.AddressAssetUTXOs(
		ctx, "not-an-address", hex.EncodeToString(policy), []byte("A"),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.ErrorIs(t, err, ErrInvalidAddress)
}

func TestNodeAdapterAddressTotal(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, payment, _ := testKeyAddress(t, 0x81, 0)
	policy := bytes.Repeat([]byte{0x82}, lcommon.AddressHashSize)
	asset := []models.Asset{{PolicyId: policy, Name: []byte("T"), Amount: 2}}
	assetUnit := hex.EncodeToString(policy) + hex.EncodeToString([]byte("T"))

	// tx1 pays 10 ADA and 2 T and is spent by tx3; tx2 pays 5 ADA and is live.
	tx3 := &models.Transaction{Hash: fill32(0x13), Slot: 9, Valid: true}
	insertAdapterTransaction(t, raw, tx3)
	tx1 := seedAddressOutput(t, raw, db, addr, fill32(0x11), 10, 10_000_000,
		asset, fill32(0x13))
	tx2 := seedAddressOutput(t, raw, db, addr, fill32(0x12), 11, 5_000_000,
		nil, nil)
	linkAddressTransaction(t, raw, addr, tx1.ID, 10)
	linkAddressTransaction(t, raw, addr, tx2.ID, 11)
	linkAddressTransaction(t, raw, addr, tx3.ID, 12)

	// An output at a different address form sharing the payment credential
	// belongs to the credential, not to the exact address.
	sibling, _, _ := testKeyAddress(t, 0x81, 0x83)
	seedAddressOutput(
		t,
		raw,
		db,
		sibling,
		fill32(0x14),
		13,
		7_000_000,
		nil,
		nil,
	)

	got, err := adapter.AddressTotal(context.Background(), addr.String())
	require.NoError(t, err)
	require.Equal(t, addr.String(), got.Address)
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "15000000"},
		{Unit: assetUnit, Quantity: "2"},
	}, got.ReceivedSum)
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "10000000"},
		{Unit: assetUnit, Quantity: "2"},
	}, got.SentSum)
	require.Equal(t, 3, got.TxCount)

	// The bare payment credential form aggregates every address form.
	hrp := "addr_vkh"
	conv, err := bech32.ConvertBits(payment, 8, 5, true)
	require.NoError(t, err)
	cred, err := bech32.Encode(hrp, conv)
	require.NoError(t, err)
	got, err = adapter.AddressTotal(context.Background(), cred)
	require.NoError(t, err)
	require.Equal(t, "22000000", got.ReceivedSum[0].Quantity)
	require.Equal(t, "10000000", got.SentSum[0].Quantity)
}

func TestNodeAdapterAddressTotalUnknownAddress(t *testing.T) {
	t.Parallel()
	adapter, _, _ := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0x91, 0)
	_, err := adapter.AddressTotal(context.Background(), addr.String())
	require.ErrorIs(t, err, ErrAddressNotFound)
	_, err = adapter.AddressTotal(context.Background(), "nonsense")
	require.ErrorIs(t, err, ErrInvalidAddress)
}

func TestNodeAdapterAddressExtended(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xa1, 0xa2)
	policy := bytes.Repeat([]byte{0xa3}, lcommon.AddressHashSize)
	policyHex := hex.EncodeToString(policy)
	nft := []byte("nft")
	coin := []byte("coin")
	plain := []byte("plain")

	// The NFT's initial mint transaction carries CIP-25 metadata.
	metadata, err := cbor.Encode(map[uint64]any{
		721: map[string]any{
			policyHex: map[string]any{"nft": map[string]any{"name": "NFT"}},
		},
	})
	require.NoError(t, err)
	mint := &models.Transaction{
		Hash: fill32(0x21), Slot: 5, Valid: true, Metadata: metadata,
	}
	insertAdapterTransaction(t, raw, mint)
	_, err = raw.Exec(`
INSERT INTO asset_mint_burn (tx_hash, policy_id, name, fingerprint, slot, quantity, tx_index)
VALUES (?, ?, ?, ?, 5, '1', 0)`, mint.Hash, policy, nft, []byte("fp"))
	require.NoError(t, err)

	_, err = db.Metadata().UpsertTokenRegistryEntries(
		t.Context(),
		[]models.TokenRegistryEntry{{
			Subject:  tokenRegistrySubjectFor(policyHex, coin),
			Name:     "coin",
			Decimals: new(8),
		}},
		time.Now(),
		nil,
	)
	require.NoError(t, err)

	seedAddressOutput(
		t,
		raw,
		db,
		addr,
		fill32(0x22),
		6,
		3_000_000,
		[]models.Asset{
			{PolicyId: policy, Name: nft, Amount: 1},
			{PolicyId: policy, Name: coin, Amount: 100},
			{PolicyId: policy, Name: plain, Amount: 4},
		},
		nil,
	)

	got, err := adapter.AddressExtended(context.Background(), addr.String())
	require.NoError(t, err)
	require.Equal(t, addr.String(), got.Address)
	require.Equal(t, "shelley", got.Type)
	require.NotNil(t, got.StakeAddress)
	require.False(t, got.Script)
	byUnit := map[string]AddressExtendedAmountInfo{}
	for _, amount := range got.Amount {
		byUnit[amount.Unit] = amount
	}
	require.Len(t, byUnit, 4)
	lovelace := byUnit["lovelace"]
	require.Equal(t, "3000000", lovelace.Quantity)
	require.NotNil(t, lovelace.Decimals)
	require.Equal(t, 6, *lovelace.Decimals)
	require.False(t, lovelace.HasNFTOnchainMetadata)

	nftAmount := byUnit[policyHex+hex.EncodeToString(nft)]
	require.True(t, nftAmount.HasNFTOnchainMetadata)
	require.Nil(t, nftAmount.Decimals)
	coinAmount := byUnit[policyHex+hex.EncodeToString(coin)]
	require.False(t, coinAmount.HasNFTOnchainMetadata)
	require.NotNil(t, coinAmount.Decimals)
	require.Equal(t, 8, *coinAmount.Decimals)
	plainAmount := byUnit[policyHex+hex.EncodeToString(plain)]
	require.False(t, plainAmount.HasNFTOnchainMetadata)
	require.Nil(t, plainAmount.Decimals)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = adapter.AddressExtended(cancelled, addr.String())
	require.ErrorIs(t, err, context.Canceled)
}

// Key and script hashes share one namespace, so a bare credential counts only
// the outputs locked by the kind of credential it names.
func TestNodeAdapterAddressTotalSeparatesKeyAndScriptCredentials(
	t *testing.T,
) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	keyAddr, payment, _ := testKeyAddress(t, 0x85, 0)
	scriptAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	keyTx := seedAddressOutput(t, raw, db, keyAddr, fill32(0x31), 10,
		2_000_000, nil, nil)
	scriptTx := seedAddressOutput(t, raw, db, scriptAddr, fill32(0x32), 11,
		9_000_000, nil, nil)
	linkAddressTransaction(t, raw, keyAddr, keyTx.ID, 10)
	linkAddressTransaction(t, raw, scriptAddr, scriptTx.ID, 11)

	conv, err := bech32.ConvertBits(payment, 8, 5, true)
	require.NoError(t, err)
	for _, tc := range []struct {
		hrp  string
		want string
	}{
		{"addr_vkh", "2000000"},
		{"script", "9000000"},
	} {
		cred, err := bech32.Encode(tc.hrp, conv)
		require.NoError(t, err)
		got, err := adapter.AddressTotal(context.Background(), cred)
		require.NoError(t, err, tc.hrp)
		require.Equal(t, tc.want, got.ReceivedSum[0].Quantity, tc.hrp)
		require.Equal(t, 1, got.TxCount, tc.hrp)
	}

	// A hash used only as a key credential has no script history, so its
	// spent-out key transactions do not make the script form resolve.
	spentKeyAddr, spentPayment, _ := testKeyAddress(t, 0x87, 0)
	spender := &models.Transaction{Hash: fill32(0x34), Slot: 13, Valid: true}
	insertAdapterTransaction(t, raw, spender)
	spentTx := seedAddressOutput(t, raw, db, spentKeyAddr, fill32(0x33), 12,
		3_000_000, nil, fill32(0x34))
	linkAddressTransaction(t, raw, spentKeyAddr, spentTx.ID, 12)
	linkAddressTransaction(t, raw, spentKeyAddr, spender.ID, 13)
	conv, err = bech32.ConvertBits(spentPayment, 8, 5, true)
	require.NoError(t, err)
	keyCred, err := bech32.Encode("addr_vkh", conv)
	require.NoError(t, err)
	got, err := adapter.AddressTotal(context.Background(), keyCred)
	require.NoError(t, err)
	require.Equal(t, 2, got.TxCount)
	scriptCred, err := bech32.Encode("script", conv)
	require.NoError(t, err)
	_, err = adapter.AddressTotal(context.Background(), scriptCred)
	require.ErrorIs(t, err, ErrAddressNotFound)
	_, err = adapter.Address(context.Background(), scriptCred)
	require.ErrorIs(t, err, ErrAddressNotFound)
}

// Every UTxO listing matches the exact address, so a foreign-network address
// is rejected rather than listing nothing.
func TestNodeAdapterAddressUTXOsRejectForeignNetwork(t *testing.T) {
	t.Parallel()
	adapter, _, _ := newDBBackedAdapter(t)
	mainnet, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkMainnet,
		bytes.Repeat([]byte{0x86}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	page := PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc}
	_, _, err = adapter.AddressUTXOs(
		context.Background(), mainnet.String(), page,
	)
	require.ErrorIs(t, err, ErrInvalidAddress)
	_, _, err = adapter.AddressAssetUTXOs(
		context.Background(),
		mainnet.String(),
		hex.EncodeToString(bytes.Repeat([]byte{0x87}, lcommon.AddressHashSize)),
		[]byte("A"),
		page,
	)
	require.ErrorIs(t, err, ErrInvalidAddress)
}

func seedSummaryOutputs(
	t *testing.T,
	adapter *NodeAdapter,
	raw *sql.DB,
	db *database.Database,
	addr lcommon.Address,
	count int,
	assets func(i int) []models.Asset,
) {
	t.Helper()
	for i := range count {
		var held []models.Asset
		if assets != nil {
			held = assets(i)
		}
		seedAddressOutput(
			t, raw, db, addr, fill32(byte(0x40+i)), uint64(10+i),
			2_000_000, held, nil,
		)
	}
	require.NotNil(t, adapter)
}

func TestNodeAdapterAddressStopsOnCancelledContext(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xb1, 0xb2)
	seedSummaryOutputs(t, adapter, raw, db, addr, 2, nil)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := adapter.Address(cancelled, addr.String())
	require.ErrorIs(t, err, context.Canceled)

	info, err := adapter.Address(context.Background(), addr.String())
	require.NoError(t, err)
	require.Equal(t, "4000000", info.Amount[0].Quantity)
}

// Batches smaller than the UTxO set must give the totals one unbounded read
// would, including sums past the int64 range and the (policy id, name)
// asset order, and must not count outputs at sibling address forms.
func TestNodeAdapterAddressBatchedBalanceIsExact(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xb3, 0xb4)
	policy := bytes.Repeat([]byte{0xb5}, lcommon.AddressHashSize)
	big := models.Asset{PolicyId: policy, Name: []byte("b"), Amount: 1 << 63}
	small := models.Asset{PolicyId: policy, Name: []byte("a"), Amount: 3}
	seedSummaryOutputs(
		t, adapter, raw, db, addr, 5,
		func(i int) []models.Asset {
			if i < 2 {
				return []models.Asset{big, small}
			}
			return nil
		},
	)
	sibling, _, _ := testKeyAddress(t, 0xb3, 0xb6)
	seedAddressOutput(
		t, raw, db, sibling, fill32(0x7f), 30, 9_000_000, nil, nil,
	)

	want, err := adapter.Address(context.Background(), addr.String())
	require.NoError(t, err)
	adapter.summaryLimits = addressSummaryLimits{batch: 2}
	got, err := adapter.Address(context.Background(), addr.String())
	require.NoError(t, err)
	require.Equal(t, want, got)

	policyHex := hex.EncodeToString(policy)
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "10000000"},
		{Unit: policyHex + hex.EncodeToString([]byte("a")), Quantity: "6"},
		{
			Unit:     policyHex + hex.EncodeToString([]byte("b")),
			Quantity: "18446744073709551616",
		},
	}, got.Amount)
}

func TestNodeAdapterAddressReadBudget(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xb7, 0xb8)
	policy := bytes.Repeat([]byte{0xb9}, lcommon.AddressHashSize)
	seedSummaryOutputs(
		t, adapter, raw, db, addr, 4,
		func(i int) []models.Asset {
			return []models.Asset{{
				PolicyId: policy, Name: []byte{byte(i)}, Amount: 1,
			}}
		},
	)

	adapter.summaryLimits = addressSummaryLimits{batch: 2, maxUtxos: 3}
	_, err := adapter.Address(context.Background(), addr.String())
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)
	_, err = adapter.AddressExtended(context.Background(), addr.String())
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)

	adapter.summaryLimits = addressSummaryLimits{batch: 2, maxUtxos: 4}
	_, err = adapter.Address(context.Background(), addr.String())
	require.NoError(t, err)

	adapter.summaryLimits = addressSummaryLimits{maxAssets: 3}
	_, err = adapter.Address(context.Background(), addr.String())
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)
	adapter.summaryLimits = addressSummaryLimits{maxAssets: 4}
	_, err = adapter.Address(context.Background(), addr.String())
	require.NoError(t, err)
}

func TestNodeAdapterAddressExtendedCapsEnrichment(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xc1, 0xc2)
	policy := bytes.Repeat([]byte{0xc3}, lcommon.AddressHashSize)
	seedAddressOutput(
		t, raw, db, addr, fill32(0x51), 6, 3_000_000,
		[]models.Asset{
			{PolicyId: policy, Name: []byte("a"), Amount: 1},
			{PolicyId: policy, Name: []byte("b"), Amount: 1},
			{PolicyId: policy, Name: []byte("c"), Amount: 1},
		},
		nil,
	)

	adapter.summaryLimits = addressSummaryLimits{maxEnrichedAssets: 2}
	_, err := adapter.AddressExtended(context.Background(), addr.String())
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)

	// The base summary has no per-asset enrichment to cap.
	_, err = adapter.Address(context.Background(), addr.String())
	require.NoError(t, err)

	adapter.summaryLimits = addressSummaryLimits{maxEnrichedAssets: 3}
	got, err := adapter.AddressExtended(context.Background(), addr.String())
	require.NoError(t, err)
	require.Len(t, got.Amount, 4)
}

// seedSiblingOutputs stores count live outputs at sibling, each with its
// output CBOR, at slots 1..count.
func seedSiblingOutputs(
	t *testing.T,
	raw *sql.DB,
	db *database.Database,
	sibling lcommon.Address,
	count int,
) {
	t.Helper()
	siblingCbor, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: sibling,
		OutputAmount:  1_000_000,
	})
	require.NoError(t, err)
	tx, err := raw.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(`
INSERT INTO utxo (tx_id, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, output_idx, payment_script)
VALUES (?, ?, ?, 0, ?, 0, '1000000', 0, ?)`)
	require.NoError(t, err)
	ids := make([][]byte, count)
	for i := range count {
		id := make([]byte, 32)
		binary.BigEndian.PutUint64(id, uint64(i)+1)
		id[31] = 0xee
		ids[i] = id
		_, err = stmt.Exec(
			id,
			sibling.PaymentKeyHash().Bytes(),
			stakingKeyOf(sibling),
			uint64(i)+1,
			models.PaymentScriptFromAddress(sibling),
		)
		require.NoError(t, err)
	}
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())
	for start := 0; start < count; start += 1000 {
		chunk := ids[start:min(start+1000, count)]
		require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
			for _, id := range chunk {
				if err := db.Blob().SetUtxo(
					txn.Blob(), id, 0, siblingCbor,
				); err != nil {
					return err
				}
			}
			return nil
		}))
	}
}

// An enterprise address shares its coarse credential match with every base
// address of the same payment credential. Sibling outputs ahead of the
// address's own outputs must not exhaust the database's per-call candidate
// cap and fail the summary.
func TestNodeAdapterAddressSkipsManySiblingForms(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xd1, 0)
	sibling, _, _ := testKeyAddress(t, 0xd1, 0xd3)
	seedSiblingOutputs(t, raw, db, sibling, 10_050)
	seedAddressOutput(
		t, raw, db, addr, fill32(0xd7), 20_000, 2_000_000, nil, nil,
	)
	seedAddressOutput(
		t, raw, db, addr, fill32(0xd8), 20_001, 3_000_000, nil, nil,
	)

	info, err := adapter.Address(context.Background(), addr.String())
	require.NoError(t, err)
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "5000000"},
	}, info.Amount)
}

// The read budget counts every output the summary reads and decodes,
// including sibling forms the exact comparison then discards.
func TestNodeAdapterAddressBudgetCountsSiblingCandidates(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xd4, 0)
	sibling, _, _ := testKeyAddress(t, 0xd4, 0xd5)
	seedSiblingOutputs(t, raw, db, sibling, 5)
	seedAddressOutput(
		t, raw, db, addr, fill32(0xd6), 100, 2_000_000, nil, nil,
	)

	adapter.summaryLimits = addressSummaryLimits{batch: 2, maxUtxos: 5}
	_, err := adapter.Address(context.Background(), addr.String())
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)

	adapter.summaryLimits = addressSummaryLimits{batch: 2, maxUtxos: 6}
	info, err := adapter.Address(context.Background(), addr.String())
	require.NoError(t, err)
	require.Equal(t, "2000000", info.Amount[0].Quantity)
}

// A bare payment credential aggregates in SQL, and its distinct asset count
// is held to the same budget as a full address.
func TestNodeAdapterAddressPaymentCredAssetBudget(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, payment, _ := testKeyAddress(t, 0xe1, 0xe2)
	policy := bytes.Repeat([]byte{0xe3}, lcommon.AddressHashSize)
	seedAddressOutput(
		t, raw, db, addr, fill32(0x61), 7, 2_000_000,
		[]models.Asset{
			{PolicyId: policy, Name: []byte("b"), Amount: 2},
			{PolicyId: policy, Name: []byte("a"), Amount: 1},
			{PolicyId: policy, Name: []byte("c"), Amount: 3},
		},
		nil,
	)
	credID, err := bech32EncodeData("addr_vkh", payment)
	require.NoError(t, err)

	adapter.summaryLimits = addressSummaryLimits{maxAssets: 2}
	_, err = adapter.Address(context.Background(), credID)
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)

	adapter.summaryLimits = addressSummaryLimits{maxAssets: 3}
	info, err := adapter.Address(context.Background(), credID)
	require.NoError(t, err)
	policyHex := hex.EncodeToString(policy)
	require.Equal(t, []AddressAmountInfo{
		{Unit: "lovelace", Quantity: "2000000"},
		{Unit: policyHex + hex.EncodeToString([]byte("a")), Quantity: "1"},
		{Unit: policyHex + hex.EncodeToString([]byte("b")), Quantity: "2"},
		{Unit: policyHex + hex.EncodeToString([]byte("c")), Quantity: "3"},
	}, info.Amount)
}

// A bare payment credential's read is held to the output budget too.
func TestNodeAdapterAddressPaymentCredUtxoBudget(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, payment, _ := testKeyAddress(t, 0xe4, 0)
	seedSummaryOutputs(t, adapter, raw, db, addr, 3, nil)
	credID, err := bech32EncodeData("addr_vkh", payment)
	require.NoError(t, err)

	adapter.summaryLimits = addressSummaryLimits{maxUtxos: 2}
	_, err = adapter.Address(context.Background(), credID)
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)
	_, err = adapter.AddressExtended(context.Background(), credID)
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)

	adapter.summaryLimits = addressSummaryLimits{maxUtxos: 3}
	info, err := adapter.Address(context.Background(), credID)
	require.NoError(t, err)
	require.Equal(t, "6000000", info.Amount[0].Quantity)
}

func TestNodeAdapterAddressTotalReadBudget(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xd1, 0xd2)
	policy := bytes.Repeat([]byte{0xd3}, lcommon.AddressHashSize)
	insertAdapterTransaction(
		t, raw, &models.Transaction{Hash: fill32(0x7e), Slot: 9, Valid: true},
	)
	// Four outputs, two of them spent, each holding its own asset.
	for i := range 4 {
		var spentBy []byte
		if i < 2 {
			spentBy = fill32(0x7e)
		}
		seedAddressOutput(
			t, raw, db, addr, fill32(byte(0x60+i)), uint64(10+i), 2_000_000,
			[]models.Asset{{
				PolicyId: policy, Name: []byte{byte(i)}, Amount: 1,
			}},
			spentBy,
		)
	}
	ctx := context.Background()

	adapter.summaryLimits = addressSummaryLimits{batch: 2, maxTotalOutputs: 3}
	_, err := adapter.AddressTotal(ctx, addr.String())
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)

	adapter.summaryLimits = addressSummaryLimits{batch: 2, maxTotalOutputs: 4}
	got, err := adapter.AddressTotal(ctx, addr.String())
	require.NoError(t, err)
	require.Equal(t, "8000000", got.ReceivedSum[0].Quantity)
	require.Equal(t, "4000000", got.SentSum[0].Quantity)

	adapter.summaryLimits = addressSummaryLimits{maxAssets: 3}
	_, err = adapter.AddressTotal(ctx, addr.String())
	require.ErrorIs(t, err, ErrAddressSummaryTooLarge)
	adapter.summaryLimits = addressSummaryLimits{maxAssets: 4}
	_, err = adapter.AddressTotal(ctx, addr.String())
	require.NoError(t, err)
}

func TestNodeAdapterAddressTotalStopsOnCancelledContext(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	addr, _, _ := testKeyAddress(t, 0xd4, 0xd5)
	seedSummaryOutputs(t, adapter, raw, db, addr, 2, nil)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := adapter.AddressTotal(cancelled, addr.String())
	require.ErrorIs(t, err, context.Canceled)
}
