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

package kupo

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/btcsuite/btcd/btcutil/bech32"
)

type patternKind uint8

const (
	patternAll patternKind = iota
	patternShelley
	patternAddress
	patternCredentials
	patternAsset
	patternReference
	patternMetadata
)

type credentialPattern struct {
	wildcard bool
	hash     []byte
}

type parsedPattern struct {
	kind       patternKind
	address    lcommon.Address
	payment    credentialPattern
	delegation credentialPattern
	policyID   []byte
	assetName  []byte
	assetAny   bool
	txID       []byte
	output     *uint32
	metadata   *uint64
}

func parsePattern(value string) (parsedPattern, error) {
	if value == "*" {
		return parsedPattern{kind: patternAll}, nil
	}
	if value == "*/*" {
		return parsedPattern{kind: patternShelley}, nil
	}
	if transactionID, outputIndex, ok := strings.Cut(value, "@"); ok {
		return parseReferencePattern(transactionID, outputIndex)
	}
	if policyID, assetName, ok := strings.Cut(value, "."); ok {
		return parseAssetPattern(policyID, assetName)
	}
	if strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}") {
		label := strings.TrimSuffix(strings.TrimPrefix(value, "{"), "}")
		if value, err := strconv.ParseUint(label, 10, 64); err == nil {
			return parsedPattern{kind: patternMetadata, metadata: &value}, nil
		}
	}
	if addr, err := lcommon.NewAddress(value); err == nil {
		return patternFromAddress(addr)
	}
	if raw, err := hex.DecodeString(value); err == nil {
		if addr, err := lcommon.NewAddressFromBytes(raw); err == nil {
			return patternFromAddress(addr)
		}
	}
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return parsedPattern{}, invalidPattern(value)
	}
	payment, err := parseCredential(parts[0])
	if err != nil {
		return parsedPattern{}, err
	}
	delegation, err := parseCredential(parts[1])
	if err != nil {
		return parsedPattern{}, err
	}
	return parsedPattern{
		kind:       patternCredentials,
		payment:    payment,
		delegation: delegation,
	}, nil
}

func patternFromAddress(addr lcommon.Address) (parsedPattern, error) {
	if addr.Type() != lcommon.AddressTypeNoneKey &&
		addr.Type() != lcommon.AddressTypeNoneScript {
		return parsedPattern{kind: patternAddress, address: addr}, nil
	}
	stake := addr.StakeKeyHash().Bytes()
	if len(stake) != lcommon.AddressHashSize {
		return parsedPattern{}, invalidPattern(addr.String())
	}
	return parsedPattern{
		kind:       patternCredentials,
		payment:    credentialPattern{wildcard: true},
		delegation: credentialPattern{hash: stake},
	}, nil
}

func parseAssetPattern(policy, asset string) (parsedPattern, error) {
	ret := parsedPattern{kind: patternAsset}
	policyID, err := hex.DecodeString(policy)
	if err != nil || len(policyID) != 28 {
		return parsedPattern{}, invalidPattern(policy + "." + asset)
	}
	ret.policyID = policyID
	if asset == "*" {
		ret.assetAny = true
		return ret, nil
	}
	assetName, err := hex.DecodeString(asset)
	if err != nil || len(assetName) > 32 {
		return parsedPattern{}, invalidPattern(policy + "." + asset)
	}
	ret.assetName = assetName
	return ret, nil
}

func parseReferencePattern(index, transactionID string) (parsedPattern, error) {
	txID, err := hex.DecodeString(transactionID)
	if err != nil || len(txID) != 32 {
		return parsedPattern{}, invalidPattern(index + "@" + transactionID)
	}
	ret := parsedPattern{kind: patternReference, txID: txID}
	if index == "*" {
		return ret, nil
	}
	parsed, err := strconv.ParseUint(index, 10, 32)
	if err != nil {
		return parsedPattern{}, invalidPattern(index + "@" + transactionID)
	}
	output := uint32(parsed)
	ret.output = &output
	return ret, nil
}

func parseCredential(value string) (credentialPattern, error) {
	if value == "*" {
		return credentialPattern{wildcard: true}, nil
	}
	if raw, err := hex.DecodeString(value); err == nil {
		switch len(raw) {
		case 28:
			return credentialPattern{hash: raw}, nil
		case 32:
			hash := lcommon.Blake2b224Hash(raw)
			return credentialPattern{hash: hash.Bytes()}, nil
		}
	}
	hrp, data, err := bech32.Decode(value)
	if err != nil {
		return credentialPattern{}, invalidPattern(value)
	}
	payload, err := bech32.ConvertBits(data, 5, 8, false)
	if err != nil {
		return credentialPattern{}, invalidPattern(value)
	}
	hrpl := strings.ToLower(hrp)
	keyPrefixes := map[string]bool{
		"vk": true, "addr_vk": true, "stake_vk": true,
	}
	hashPrefixes := map[string]bool{
		"vkh": true, "addr_vkh": true, "stake_vkh": true, "script": true,
	}
	switch {
	case keyPrefixes[hrpl]:
		if len(payload) != 32 {
			return credentialPattern{}, invalidPattern(value)
		}
		hash := lcommon.Blake2b224Hash(payload)
		return credentialPattern{hash: hash.Bytes()}, nil
	case hashPrefixes[hrpl]:
		if len(payload) != 28 {
			return credentialPattern{}, invalidPattern(value)
		}
		return credentialPattern{hash: payload}, nil
	default:
		return credentialPattern{}, invalidPattern(value)
	}
}

func (p parsedPattern) addressQuery() (bool, []models.UtxoAddressPattern, error) {
	switch p.kind {
	case patternAll, patternShelley, patternAsset, patternReference,
		patternMetadata:
		return true, nil, nil
	case patternAddress:
		pattern := models.UtxoAddressPattern{}
		zeroHash := lcommon.NewBlake2b224(nil)
		if payment := p.address.PaymentKeyHash(); payment != zeroHash {
			pattern.PaymentPart = payment.Bytes()
		}
		if delegation := p.address.StakeKeyHash(); delegation != zeroHash {
			pattern.DelegationPart = delegation.Bytes()
		}
		if len(pattern.PaymentPart) == 0 && len(pattern.DelegationPart) == 0 {
			return true, nil, nil
		}
		return false, []models.UtxoAddressPattern{pattern}, nil
	case patternCredentials:
		pattern := models.UtxoAddressPattern{}
		if !p.payment.wildcard {
			pattern.PaymentPart = p.payment.hash
		}
		if !p.delegation.wildcard {
			pattern.DelegationPart = p.delegation.hash
		}
		if len(pattern.PaymentPart) == 0 && len(pattern.DelegationPart) == 0 {
			return true, nil, nil
		}
		return false, []models.UtxoAddressPattern{pattern}, nil
	default:
		return false, nil, invalidPattern("")
	}
}

func (p parsedPattern) matchesAssets(assets []models.Asset) bool {
	if p.kind != patternAsset {
		return true
	}
	for _, asset := range assets {
		if !bytes.Equal(asset.PolicyId, p.policyID) {
			continue
		}
		if p.assetAny || bytes.Equal(asset.Name, p.assetName) {
			return true
		}
	}
	return false
}

func (p parsedPattern) matchesAddress(addr lcommon.Address) bool {
	switch p.kind {
	case patternAll, patternAsset, patternReference, patternMetadata:
		return true
	case patternShelley:
		return addr.Type() != lcommon.AddressTypeByron
	case patternAddress:
		left, err := addr.Bytes()
		if err != nil {
			return false
		}
		right, err := p.address.Bytes()
		return err == nil && bytes.Equal(left, right)
	case patternCredentials:
		if !credentialMatches(p.payment, addr.PaymentKeyHash().Bytes()) {
			return false
		}
		return credentialMatches(p.delegation, addr.StakeKeyHash().Bytes())
	default:
		return false
	}
}

func credentialMatches(pattern credentialPattern, hash []byte) bool {
	if pattern.wildcard {
		return true
	}
	if !bytes.Equal(pattern.hash, hash) {
		return false
	}
	return true
}

func invalidPattern(value string) error {
	return fmt.Errorf("%w: invalid pattern %q", ErrInvalidRequest, value)
}
