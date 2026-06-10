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
	credentialTag, _ := StakeCredentialTagFromAddress(addr)
	switch {
	case hasPayment && hasStake:
		*ors = append(
			*ors,
			"(utxo.payment_key = ? AND utxo.credential_tag = ? AND utxo.staking_key = ?)",
		)
		*args = append(*args, pk.Bytes(), credentialTag, sk.Bytes())
	case hasPayment:
		*ors = append(*ors, "(utxo.payment_key = ?)")
		*args = append(*args, pk.Bytes())
	case hasStake:
		*ors = append(*ors, "(utxo.credential_tag = ? AND utxo.staking_key = ?)")
		*args = append(*args, credentialTag, sk.Bytes())
	}
}

// Utxo represents an unspent transaction output
type Utxo struct {
	TransactionID           *uint        `gorm:"index"`
	CollateralReturnForTxID *uint        `gorm:"uniqueIndex"` // Unique: a transaction has at most one collateral return output
	TxId                    []byte       `gorm:"uniqueIndex:tx_id_output_idx;size:32"`
	PaymentKey              []byte       `gorm:"index;size:28"`
	StakingKey              []byte       `gorm:"index;size:28;index:idx_utxo_deleted_staking_amount,priority:3"`
	CredentialTag           uint8        `gorm:"not null;default:0;index:idx_utxo_deleted_staking_amount,priority:2"`
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
	DeletedSlot             uint64       `gorm:"index:idx_utxo_deleted_staking_amount,priority:1;index:idx_utxo_deleted_payment_script,priority:1"`
	Amount                  types.Uint64 `gorm:"index:idx_utxo_deleted_staking_amount,priority:3;index:idx_utxo_deleted_payment_script,priority:3"`
	OutputIdx               uint32       `gorm:"uniqueIndex:tx_id_output_idx"`
	// PaymentScript is true when the output's payment credential is a
	// script hash (as opposed to a key hash). It is derived from the
	// address type at index time and used to compute the network's
	// script-locked supply (see GetScriptLockedSupply). The composite
	// index (deleted_slot, payment_script, amount) lets the supply sum
	// scan only live script UTxOs.
	PaymentScript bool `gorm:"index:idx_utxo_deleted_payment_script,priority:2"`
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
// uses the producing transaction position for ordering.
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
	// The low bit of the address type distinguishes a script payment
	// credential from a key-hash credential. Byron addresses (type
	// 0b1000) never have this bit set, so they are correctly treated
	// as non-script.
	if outAddr.Type()&lcommon.AddressTypeScriptBit == lcommon.AddressTypeScriptBit {
		ret.PaymentScript = true
	}
	skh := outAddr.StakeKeyHash()
	if skh != zeroHash {
		ret.StakingKey = skh.Bytes()
		credentialTag, ok := StakeCredentialTagFromAddress(outAddr)
		if ok {
			ret.CredentialTag = credentialTag
		}
	}
	if dh := utxo.Output.DatumHash(); dh != nil {
		ret.DatumHash = append([]byte(nil), dh[:]...)
	}
	if multiAsset := utxo.Output.Assets(); multiAsset != nil {
		ret.Assets = ConvertMultiAssetToModels(multiAsset)
	}

	return ret
}

func StakeCredentialTagFromAddress(addr ledger.Address) (uint8, bool) {
	zeroHash := lcommon.NewBlake2b224(nil)
	if addr.StakeKeyHash() == zeroHash {
		return 0, false
	}
	switch addr.StakingPayload().(type) {
	case lcommon.AddressPayloadKeyHash:
		return 0, true
	case lcommon.AddressPayloadScriptHash:
		return 1, true
	default:
		return 0, false
	}
}

// UtxoSlot allows providing a slot number with a ledger.Utxo object
type UtxoSlot struct {
	Utxo ledger.Utxo
	Slot uint64
}
