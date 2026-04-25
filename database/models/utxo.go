// Copyright 2025 Blink Labs Software
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

package models

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Sentinel errors for UtxoWithOrderingQuery validation.
var (
	ErrNilUtxoWithOrderingQuery = errors.New(
		"nil UtxoWithOrderingQuery",
	)
	ErrEmptyAssetPolicyID = errors.New("empty asset policy id")
)

// AppendUtxoAddressOrBranch appends an OR branch for the given address
// to the ors/args slices. It uses standard "?" placeholders that work
// across SQLite, MySQL, and Postgres via GORM.
func AppendUtxoAddressOrBranch(
	ors *[]string,
	args *[]any,
	addr ledger.Address,
) {
	zeroHash := lcommon.NewBlake2b224(nil)
	pk := addr.PaymentKeyHash()
	sk := addr.StakeKeyHash()
	hasPayment := pk != zeroHash
	hasStake := sk != zeroHash
	switch {
	case hasPayment && hasStake:
		*ors = append(
			*ors,
			"(utxo.payment_key = ? AND utxo.staking_key = ?)",
		)
		*args = append(*args, pk.Bytes(), sk.Bytes())
	case hasPayment:
		*ors = append(*ors, "(utxo.payment_key = ?)")
		*args = append(*args, pk.Bytes())
	case hasStake:
		*ors = append(*ors, "(utxo.staking_key = ?)")
		*args = append(*args, sk.Bytes())
	}
}

// Utxo represents an unspent transaction output
type Utxo struct {
	TransactionID           *uint        `gorm:"index"`
	CollateralReturnForTxID *uint        `gorm:"uniqueIndex"` // Unique: a transaction has at most one collateral return output
	TxId                    []byte       `gorm:"uniqueIndex:tx_id_output_idx;size:32"`
	PaymentKey              []byte       `gorm:"index;size:28"`
	StakingKey              []byte       `gorm:"index;size:28"`
	Assets                  []Asset      `gorm:"foreignKey:UtxoID;constraint:OnDelete:CASCADE"`
	Cbor                    []byte       `gorm:"-"`       // This is here for convenience but not represented in the metadata DB
	DatumHash               []byte       `gorm:"size:32"` // Optional datum hash (32 bytes)
	Datum                   []byte       `gorm:"-"`       // Inline datum CBOR, not stored in metadata DB
	ScriptRef               []byte       `gorm:"-"`       // Reference script bytes, not stored in metadata DB
	SpentAtTxId             []byte       `gorm:"index;size:32"`
	ReferencedByTxId        []byte       `gorm:"index;size:32"`
	CollateralByTxId        []byte       `gorm:"index;size:32"`
	ID                      uint         `gorm:"primarykey"`
	AddedSlot               uint64       `gorm:"index"`
	DeletedSlot             uint64       `gorm:"index"`
	Amount                  types.Uint64 `gorm:"index"`
	OutputIdx               uint32       `gorm:"uniqueIndex:tx_id_output_idx"`
}

// UtxoWithOrdering includes UTxO with transaction ordering metadata
type UtxoWithOrdering struct {
	Utxo
	TxSlot       uint64 `gorm:"column:tx_slot"`
	TxBlockIndex uint32 `gorm:"column:tx_block_index"`
}

// UtxoOrderingCursor is the keyset position for SearchUtxos.
//
// Text form (non-empty): slot:block_index:output_idx. GetUtxosByAddressWithOrdering
// uses INNER JOIN transaction so these come from real chain position (no COALESCE).
type UtxoOrderingCursor struct {
	Slot       uint64
	BlockIndex uint32
	OutputIdx  uint32
}

// UtxoWithOrderingQuery drives GetUtxosByAddressWithOrdering (single MetadataStore entry).
//
// Address matching (exactly one of these applies):
//   - MatchAllAddresses true: do not filter by payment/stake keys (all live UTxOs, subject
//     to asset filter if set). SearchUtxos sets this when the predicate is nil or is
//     asset-only (no address pattern).
//   - MatchAllAddresses false and len(Addresses) == 0: match no rows (caller uses this when
//     a predicate was given but no Cardano address parts could be decoded).
//   - MatchAllAddresses false and len(Addresses) > 0: match UTxOs that satisfy ANY branch
//     (OR): exact / payment-only / stake-only per address, same rules as legacy
//     addressWhereClause.
//
// After + Limit: keyset pagination; Limit <= 0 means no SQL LIMIT. SearchUtxos sets Limit to
// effective page size + 1.
//
// FilterByAsset: when true, AssetPolicyID is required; AssetName nil matches any name under
// the policy (same semantics as GetUtxosByAssets).
type UtxoWithOrderingQuery struct {
	MatchAllAddresses bool
	Addresses         []ledger.Address
	After             *UtxoOrderingCursor
	Limit             int
	FilterByAsset     bool
	AssetPolicyID     []byte
	AssetName         []byte
}

func (u *Utxo) TableName() string {
	return "utxo"
}

func (u *Utxo) Decode() (ledger.TransactionOutput, error) {
	return ledger.NewTransactionOutputFromCbor(u.Cbor)
}

func UtxoLedgerToModel(
	utxo ledger.Utxo,
	slot uint64,
) Utxo {
	outAddr := utxo.Output.Address()
	ret := Utxo{
		TxId:      utxo.Id.Id().Bytes(),
		Cbor:      utxo.Output.Cbor(),
		AddedSlot: slot,
		Amount:    types.Uint64(utxo.Output.Amount().Uint64()),
		OutputIdx: utxo.Id.Index(),
	}
	var zeroHash ledger.Blake2b224
	pkh := outAddr.PaymentKeyHash()
	if pkh != zeroHash {
		ret.PaymentKey = pkh.Bytes()
	}
	skh := outAddr.StakeKeyHash()
	if skh != zeroHash {
		ret.StakingKey = skh.Bytes()
	}
	if dh := utxo.Output.DatumHash(); dh != nil {
		ret.DatumHash = dh.Bytes()
	}
	if multiAsset := utxo.Output.Assets(); multiAsset != nil {
		ret.Assets = ConvertMultiAssetToModels(multiAsset)
	}

	return ret
}

// UtxoSlot allows providing a slot number with a ledger.Utxo object
type UtxoSlot struct {
	Utxo ledger.Utxo
	Slot uint64
}
