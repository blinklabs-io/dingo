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
	"encoding/hex"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
			spentBy, slot+1, txHash,
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
    payment_key, staking_key, credential_tag, transaction_id, slot, tx_index
) VALUES (?, ?, 0, ?, ?, 0)`,
		addr.PaymentKeyHash().Bytes(), stakingKeyOf(addr), transactionID, slot,
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
	seedAddressOutput(t, raw, db, sibling, fill32(0x14), 13, 7_000_000, nil, nil)

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

	seedAddressOutput(t, raw, db, addr, fill32(0x22), 6, 3_000_000, []models.Asset{
		{PolicyId: policy, Name: nft, Amount: 1},
		{PolicyId: policy, Name: coin, Amount: 100},
		{PolicyId: policy, Name: plain, Amount: 4},
	}, nil)

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
