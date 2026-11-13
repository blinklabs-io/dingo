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

package ledger

import (
	"encoding/binary"

	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Stake sum keys. A credential's key is its tag and 28-byte hash; a pointer
// address's key is the certificate position it names, which the stake
// computation resolves against certificate history at the slot it evaluates.
const (
	credentialStakePrefix = 'c'
	pointerStakePrefix    = 'p'
)

// UtxoStakeOf classifies a live output the way the metadata store's utxo rows
// do (models.UtxoLedgerToModel): the stake sum it adds its lovelace to, keyed
// by CredentialStakeKey for a stake key hash or PointerStakeKey for a
// pointer address. ok is false for an output that adds to no stake.
//
// ok is also false for CBOR that does not decode or whose amount exceeds
// uint64, and that silence is intended: the signature is the aggregator
// callback the live set sums with, which cannot fail a commit, and an output
// reaches the set only after it was decoded or encoded when staged, so the
// metadata rows classify it the same way.
func UtxoStakeOf(cbor []byte) (key []byte, lovelace uint64, ok bool) {
	out, err := gledger.NewTransactionOutputFromCbor(cbor)
	if err != nil || out == nil {
		return nil, 0, false
	}
	amount, err := models.CheckedUint64FromBigInt(out.Amount())
	if err != nil || amount == 0 {
		return nil, 0, false
	}
	addr := out.Address()
	if skh := addr.StakeKeyHash(); skh != (lcommon.Blake2b224{}) {
		tag, _ := models.StakeCredentialTagFromAddress(addr)
		return CredentialStakeKey(tag, skh.Bytes()), amount, true
	}
	if p, ok := addr.StakingPayload().(lcommon.AddressPayloadPointer); ok {
		return PointerStakeKey(p.Slot, p.TxIndex, p.CertIndex), amount, true
	}
	return nil, 0, false
}

// CredentialStakeKey is the stake sum key of the stake credential (tag, hash).
func CredentialStakeKey(tag uint8, hash []byte) []byte {
	key := make([]byte, 0, 2+len(hash))
	key = append(key, credentialStakePrefix, tag)
	return append(key, hash...)
}

// PointerStakeKey is the stake sum key of pointer addresses naming the
// certificate at (slot, txIndex, certIndex).
func PointerStakeKey(slot, txIndex, certIndex uint64) []byte {
	key := make([]byte, 0, 25)
	key = append(key, pointerStakePrefix)
	key = binary.BigEndian.AppendUint64(key, slot)
	key = binary.BigEndian.AppendUint64(key, txIndex)
	return binary.BigEndian.AppendUint64(key, certIndex)
}

// ParsePointerStakeKey returns the certificate position a PointerStakeKey
// names.
func ParsePointerStakeKey(
	key []byte,
) (slot, txIndex, certIndex uint64, ok bool) {
	if len(key) != 25 || key[0] != pointerStakePrefix {
		return 0, 0, 0, false
	}
	return binary.BigEndian.Uint64(key[1:]),
		binary.BigEndian.Uint64(key[9:]),
		binary.BigEndian.Uint64(key[17:]),
		true
}

// ParseCredentialStakeKey returns the credential a CredentialStakeKey names.
func ParseCredentialStakeKey(key []byte) (tag uint8, hash []byte, ok bool) {
	if len(key) < 2 || key[0] != credentialStakePrefix {
		return 0, nil, false
	}
	return key[1], key[2:], true
}
