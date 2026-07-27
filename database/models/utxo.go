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
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Sentinel errors for UtxoWithOrderingQuery validation.
var (
	ErrNilUtxoWithOrderingQuery = errors.New(
		"nil UtxoWithOrderingQuery",
	)
	ErrEmptyAssetPolicyID       = errors.New("empty asset policy id")
	ErrEmptyUtxoAddressPattern  = errors.New("empty UTxO address pattern")
	ErrExactAddressRequiresCbor = errors.New(
		"exact address matching requires output CBOR",
	)
)

// UtxoAddressPattern carries explicit address-match intent through the shared
// query API. Fields within one pattern are ANDed; multiple patterns are ORed.
// ExactAddress is the complete serialized address, while PaymentPart and
// DelegationPart deliberately match credentials across address forms.
type UtxoAddressPattern struct {
	ExactAddress   []byte
	PaymentPart    []byte
	DelegationPart []byte
}

// ExactUtxoAddressPattern builds an exact pattern from a decoded address.
func ExactUtxoAddressPattern(addr ledger.Address) (UtxoAddressPattern, error) {
	addrBytes, err := addr.Bytes()
	if err != nil {
		return UtxoAddressPattern{}, fmt.Errorf("encode exact address: %w", err)
	}
	return UtxoAddressPattern{ExactAddress: addrBytes}, nil
}

// RequiresExactAddressFilter reports whether candidate rows must be checked
// against decoded output CBOR after the coarse SQL credential predicate.
func RequiresExactAddressFilter(patterns []UtxoAddressPattern) bool {
	for i := range patterns {
		if len(patterns[i].ExactAddress) > 0 {
			return true
		}
	}
	return false
}

// MatchesUtxoAddressPatterns applies the full address-pattern contract to a
// decoded output address.
func MatchesUtxoAddressPatterns(
	addr ledger.Address,
	patterns []UtxoAddressPattern,
) (bool, error) {
	if len(patterns) == 0 {
		return false, nil
	}
	for i := range patterns {
		if len(patterns[i].ExactAddress) == 0 &&
			len(patterns[i].PaymentPart) == 0 &&
			len(patterns[i].DelegationPart) == 0 {
			return false, ErrEmptyUtxoAddressPattern
		}
	}
	addrBytes, err := addr.Bytes()
	if err != nil {
		return false, fmt.Errorf("encode output address: %w", err)
	}
	for i := range patterns {
		pattern := patterns[i]
		if len(pattern.ExactAddress) > 0 &&
			!bytes.Equal(addrBytes, pattern.ExactAddress) {
			continue
		}
		if len(pattern.PaymentPart) > 0 &&
			!bytes.Equal(addr.PaymentKeyHash().Bytes(), pattern.PaymentPart) {
			continue
		}
		if len(pattern.DelegationPart) > 0 &&
			!bytes.Equal(addr.StakeKeyHash().Bytes(), pattern.DelegationPart) {
			continue
		}
		return true, nil
	}
	return false, nil
}

// AppendUtxoAddressPatternOrBranch appends one coarse SQL branch. Exact
// addresses are narrowed by their stored credentials and later compared by
// complete serialized bytes after CBOR resolution.
func AppendUtxoAddressPatternOrBranch(
	ors *[]string,
	args *[]any,
	pattern UtxoAddressPattern,
) error {
	var ands []string
	var branchArgs []any
	if len(pattern.ExactAddress) > 0 {
		addr, err := lcommon.NewAddressFromBytes(pattern.ExactAddress)
		if err != nil {
			return fmt.Errorf("decode exact address: %w", err)
		}
		var exactOrs []string
		var exactArgs []any
		if err := AppendUtxoAddressOrBranchMode(
			&exactOrs,
			&exactArgs,
			addr,
			UtxoAddressMatchPaymentCred,
		); err != nil {
			return err
		}
		if len(exactOrs) == 0 {
			ands = append(
				ands,
				"((utxo.payment_key IS NULL OR LENGTH(utxo.payment_key) = 0) AND (utxo.staking_key IS NULL OR LENGTH(utxo.staking_key) = 0))",
			)
		} else {
			ands = append(ands, exactOrs[0])
			branchArgs = append(branchArgs, exactArgs...)
		}
	}
	if len(pattern.PaymentPart) > 0 {
		if len(pattern.PaymentPart) != lcommon.AddressHashSize {
			return fmt.Errorf(
				"payment part length %d, expected %d",
				len(pattern.PaymentPart),
				lcommon.AddressHashSize,
			)
		}
		ands = append(ands, "utxo.payment_key = ?")
		branchArgs = append(branchArgs, pattern.PaymentPart)
	}
	if len(pattern.DelegationPart) > 0 {
		if len(pattern.DelegationPart) != lcommon.AddressHashSize {
			return fmt.Errorf(
				"delegation part length %d, expected %d",
				len(pattern.DelegationPart),
				lcommon.AddressHashSize,
			)
		}
		ands = append(ands, "utxo.staking_key = ?")
		branchArgs = append(branchArgs, pattern.DelegationPart)
	}
	if len(ands) == 0 {
		return ErrEmptyUtxoAddressPattern
	}
	*ors = append(*ors, "("+strings.Join(ands, " AND ")+")")
	*args = append(*args, branchArgs...)
	return nil
}

// UtxoAddressMatchMode selects how an address matches utxo rows.
type UtxoAddressMatchMode int

const (
	// UtxoAddressMatchExact requests full-address identity. Metadata-only
	// aggregate queries reject this mode because exact identity requires
	// output CBOR; use UtxoAddressPattern through the coordinated Database.
	UtxoAddressMatchExact UtxoAddressMatchMode = iota
	// UtxoAddressMatchPaymentCred aggregates across every address
	// form sharing the payment credential, mirroring Blockfrost's
	// bare-credential (addr_vkh/script) lookups.
	UtxoAddressMatchPaymentCred
)

// AppendUtxoAddressOrBranch appends an OR branch for the given address
// to the ors/args slices. It uses standard "?" placeholders that work
// across SQLite, MySQL, and Postgres via GORM. Payment-only addresses
// use payment-credential matching; see AppendUtxoAddressOrBranchMode
// for exact full-address semantics.
func AppendUtxoAddressOrBranch(
	ors *[]string,
	args *[]any,
	addr ledger.Address,
) error {
	return AppendUtxoAddressOrBranchMode(
		ors, args, addr, UtxoAddressMatchPaymentCred,
	)
}

// AppendUtxoAddressOrBranchMode appends an OR branch for the given
// address with an explicit match mode.
func AppendUtxoAddressOrBranchMode(
	ors *[]string,
	args *[]any,
	addr ledger.Address,
	mode UtxoAddressMatchMode,
) error {
	if mode == UtxoAddressMatchExact {
		return ErrExactAddressRequiresCbor
	}
	zeroHash := lcommon.NewBlake2b224(nil)
	pk := addr.PaymentKeyHash()
	sk := addr.StakeKeyHash()
	hasPayment := pk != zeroHash
	hasStake := sk != zeroHash
	paymentScript := PaymentScriptFromAddress(addr)
	switch {
	case hasPayment && hasStake:
		credentialTag, ok := StakeCredentialTagFromAddress(addr)
		if !ok {
			return errors.New("derive stake credential tag from address")
		}
		*ors = append(
			*ors,
			"(utxo.payment_script = ? AND utxo.payment_key = ? AND utxo.credential_tag = ? AND utxo.staking_key = ?)",
		)
		*args = append(
			*args,
			paymentScript,
			pk.Bytes(),
			credentialTag,
			sk.Bytes(),
		)
	case hasPayment:
		*ors = append(*ors, "(utxo.payment_script = ? AND utxo.payment_key = ?)")
		*args = append(*args, paymentScript, pk.Bytes())
	case hasStake:
		credentialTag, ok := StakeCredentialTagFromAddress(addr)
		if !ok {
			return errors.New("derive stake credential tag from address")
		}
		*ors = append(*ors, "(utxo.credential_tag = ? AND utxo.staking_key = ?)")
		*args = append(*args, credentialTag, sk.Bytes())
	}
	return nil
}

// Utxo represents an unspent transaction output
type Utxo struct {
	TransactionID           *uint   `gorm:"index"`
	CollateralReturnForTxID *uint   `gorm:"uniqueIndex"` // Unique: a transaction has at most one collateral return output
	TxId                    []byte  `gorm:"uniqueIndex:tx_id_output_idx;size:32"`
	PaymentKey              []byte  `gorm:"index;size:28"`
	StakingKey              []byte  `gorm:"index;size:28;index:idx_utxo_deleted_staking_amount,priority:3;index:idx_utxo_staking_deleted_amount,priority:2"`
	CredentialTag           uint8   `gorm:"not null;default:0;index:idx_utxo_deleted_staking_amount,priority:2;index:idx_utxo_staking_deleted_amount,priority:1"`
	Assets                  []Asset `gorm:"foreignKey:UtxoID;constraint:OnDelete:CASCADE"`
	Cbor                    []byte  `gorm:"-"`       // This is here for convenience but not represented in the metadata DB
	DatumHash               []byte  `gorm:"size:32"` // Optional datum hash (32 bytes)
	Datum                   []byte  `gorm:"-"`       // Inline datum CBOR, not stored in metadata DB
	ScriptRef               []byte  `gorm:"-"`       // Reference script bytes, not stored in metadata DB
	// SpentAtTxId, ReferencedByTxId, and CollateralByTxId are nullable FKs to
	// transaction(hash); they are unset until a UTxO is spent/referenced.
	// They use types.NullableHash so an empty value serializes to SQL NULL
	// (not an empty blob), which is required for the FK to be skipped — see
	// the type docs for the FOREIGN KEY constraint failed (787) issue.
	SpentAtTxId      types.NullableHash `gorm:"index;size:32"`
	ReferencedByTxId types.NullableHash `gorm:"index;size:32"`
	CollateralByTxId types.NullableHash `gorm:"index;size:32"`
	ID               uint               `gorm:"primarykey"`
	AddedSlot        uint64             `gorm:"index"`
	DeletedSlot      uint64             `gorm:"index:idx_utxo_deleted_staking_amount,priority:1;index:idx_utxo_staking_deleted_amount,priority:3;index:idx_utxo_deleted_payment_script,priority:1"`
	Amount           types.Uint64       `gorm:"index:idx_utxo_deleted_staking_amount,priority:4;index:idx_utxo_staking_deleted_amount,priority:4;index:idx_utxo_deleted_payment_script,priority:3"`
	OutputIdx        uint32             `gorm:"uniqueIndex:tx_id_output_idx"`
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
// Text form (non-empty): slot:block_index:output_idx:tx_id.
// GetUtxosByAddressWithOrdering uses the producing transaction position for
// ordering. Snapshot-imported UTxOs without a producing transaction use
// AddedSlot and block index zero. TxId makes the cursor unique when those
// fallback fields collide.
type UtxoOrderingCursor struct {
	Slot       uint64
	BlockIndex uint32
	OutputIdx  uint32
	TxId       []byte
}

// UtxoWithOrderingQuery drives GetUtxosByAddressWithOrdering (single MetadataStore entry).
//
// Address matching (exactly one of these applies):
//   - MatchAllAddresses true: do not filter by payment/stake keys (all live UTxOs, subject
//     to asset filter if set). SearchUtxos sets this when the predicate is nil or is
//     asset-only (no address pattern).
//   - MatchAllAddresses false and len(AddressPatterns) == 0: match no rows (caller uses this when
//     a predicate was given but no Cardano address parts could be decoded).
//   - MatchAllAddresses false and len(AddressPatterns) > 0: match UTxOs that
//     satisfy any pattern. Fields within a pattern are ANDed; patterns are ORed.
//
// After + Limit: keyset pagination; Limit <= 0 means no SQL LIMIT. SearchUtxos sets Limit to
// effective page size + 1.
//
// FilterByAsset: when true, AssetPolicyID is required; AssetName nil matches any name under
// the policy (same semantics as GetUtxosByAssets).
type UtxoWithOrderingQuery struct {
	MatchAllAddresses bool
	AddressPatterns   []UtxoAddressPattern
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
	if PaymentScriptFromAddress(outAddr) {
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

func PaymentScriptFromAddress(addr ledger.Address) bool {
	return addr.Type()&lcommon.AddressTypeScriptBit == lcommon.AddressTypeScriptBit
}

// AddressBalance holds SQL-aggregated live-UTxO balances for an address.
type AddressBalance struct {
	Lovelace  uint64
	UtxoCount int64
	// Assets is ordered by (policy id, name) so callers emit
	// deterministic unit ordering without re-sorting.
	Assets []AssetBalance
}

// AssetBalance is one aggregated native-asset balance.
type AssetBalance struct {
	PolicyId []byte
	Name     []byte
	Amount   uint64
}

// UtxoSlot allows providing a slot number with a ledger.Utxo object
type UtxoSlot struct {
	Utxo ledger.Utxo
	Slot uint64
}
