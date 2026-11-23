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
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/bursa/bip32"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// cip10Labels names the transaction metadata labels Blockfrost describes,
// taken from the CIP-10 registry
// (https://cips.cardano.org/cips/cip10/registry.json) as Blockfrost lists
// them. A label missing here is reported with a null description.
var cip10Labels = map[uint64]string{
	87:    "milkomeda.com - The protocol magic for the milkomeda protocol",
	88:    "milkomeda.com - the destination address in the sidechain",
	309:   "Proof of Existence record",
	674:   "CIP-0020 - Transaction message/comment metadata",
	721:   "CIP-0025 - NFT Metadata Standard",
	777:   "CIP-0027 - Royalties Standard",
	1188:  "paradiso.app marketplace metadata",
	1189:  "paradiso.app services metadata",
	1870:  "Open Badges v2.0 compliant metadata",
	1967:  "nut.link metadata oracles registry",
	1968:  "nut.link metadata oracles data points",
	1988:  "cardahub.io marketplace metadata",
	1989:  "cardahub.io services metadata",
	6770:  "fortunes.coconutpool.com fortune teller",
	61284: "CIP-0015 - Catalyst registration",
	61285: "CIP-0015 - Catalyst registration",
	61286: "CIP-0015 - Catalyst registration",
}

// xpubStakeRole is the CIP-1852 role of staking keys. The base address of
// every other role pairs its key with the account's first staking key.
const xpubStakeRole = 2

// errXpubHardenedIndex reports a role or index in the hardened range, which
// cannot be derived from a public key.
var errXpubHardenedIndex = errors.New(
	"hardened role or index cannot be derived from an xpub",
)

// MetadataLabels returns the paginated transaction metadata labels in use
// with their transaction counts, in numeric label order.
func (a *NodeAdapter) MetadataLabels(
	ctx context.Context,
	params PaginationParams,
) ([]MetadataLabelInfo, int, error) {
	db := a.db()
	txn := db.Transaction(ctx, false)
	defer txn.Release()

	total, err := db.CountMetadataLabels(ctx, txn)
	if err != nil {
		return nil, 0, fmt.Errorf("count metadata labels: %w", err)
	}
	offset, ok := paginationOffset(params)
	if !ok || offset >= total {
		return []MetadataLabelInfo{}, total, nil
	}
	rows, err := db.ListMetadataLabelCounts(
		ctx,
		params.Count,
		offset,
		params.Order == PaginationOrderDesc,
		txn,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("list metadata labels: %w", err)
	}
	ret := make([]MetadataLabelInfo, 0, len(rows))
	for _, row := range rows {
		ret = append(ret, MetadataLabelInfo{Label: row.Label, Count: row.Count})
	}
	return ret, total, nil
}

// XpubAddress derives an address from an account extended public key as
// Blockfrost does. Role 2 yields the reward address of the staking key at
// index. Any other role yields the base address pairing the key at role and
// index with the account's first staking key (role 2, index 0).
func (a *NodeAdapter) XpubAddress(
	xpub []byte,
	role uint32,
	index uint32,
) (string, error) {
	return deriveXpubAddress(xpub, role, index, a.networkID())
}

func deriveXpubAddress(
	xpub []byte,
	role uint32,
	index uint32,
	networkID uint8,
) (string, error) {
	if len(xpub) != 64 {
		return "", fmt.Errorf("xpub is %d bytes, not 64", len(xpub))
	}
	if role >= bip32.HardenedIndex(0) || index >= bip32.HardenedIndex(0) {
		return "", errXpubHardenedIndex
	}
	account := bip32.XPub(xpub)
	if role == xpubStakeRole {
		stakeKey, err := derivePath(account, xpubStakeRole, index)
		if err != nil {
			return "", err
		}
		address, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeNoneKey,
			networkID,
			nil,
			stakeKey.Hash(),
		)
		if err != nil {
			return "", fmt.Errorf("build reward address: %w", err)
		}
		return address.String(), nil
	}
	paymentKey, err := derivePath(account, role, index)
	if err != nil {
		return "", err
	}
	stakeKey, err := derivePath(account, xpubStakeRole, 0)
	if err != nil {
		return "", err
	}
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		networkID,
		paymentKey.Hash(),
		stakeKey.Hash(),
	)
	if err != nil {
		return "", fmt.Errorf("build base address: %w", err)
	}
	return address.String(), nil
}

func derivePath(account bip32.XPub, role, index uint32) (bip32.XPub, error) {
	roleKey, err := account.Derive(role)
	if err != nil {
		return nil, fmt.Errorf("derive role %d: %w", role, err)
	}
	key, err := roleKey.Derive(index)
	if err != nil {
		return nil, fmt.Errorf("derive index %d: %w", index, err)
	}
	return key, nil
}
