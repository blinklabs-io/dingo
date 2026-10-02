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
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gshelley "github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// stakePoolParams is the ledger's StakePoolParams record as it travels in
// node-to-client results. The owners are a tag-258 set in ascending order,
// the margin a tag-30 rational, and an absent metadata is null; empty owner
// and relay lists must be empty arrays rather than null.
type stakePoolParams struct {
	cbor.StructAsArray
	Operator      lcommon.Blake2b224
	VrfKeyHash    lcommon.Blake2b256
	Pledge        uint64
	Cost          uint64
	Margin        *cbor.Rat
	RewardAccount lcommon.Address
	Owners        cbor.Set
	Relays        []lcommon.PoolRelay
	Metadata      *lcommon.PoolMetadata
}

// newStakePoolParams adapts a registration certificate. rewardAccount is
// passed separately because the certificate keeps only the key hash of the
// reward account, not its credential type.
func newStakePoolParams(
	cert *lcommon.PoolRegistrationCertificate,
	rewardAccount lcommon.Address,
) stakePoolParams {
	margin := cbor.Rat{Rat: big.NewRat(0, 1)}
	if cert.Margin.Rat != nil {
		margin = cbor.Rat(cert.Margin)
	}
	owners := slices.Clone(cert.PoolOwners)
	slices.SortFunc(owners, func(a, b lcommon.AddrKeyHash) int {
		return bytes.Compare(a[:], b[:])
	})
	ownerSet := make(cbor.Set, 0, len(owners))
	for _, owner := range owners {
		ownerSet = append(ownerSet, lcommon.Blake2b224(owner))
	}
	relays := make([]lcommon.PoolRelay, 0, len(cert.Relays))
	relays = append(relays, cert.Relays...)
	return stakePoolParams{
		Operator:      lcommon.Blake2b224(cert.Operator),
		VrfKeyHash:    lcommon.Blake2b256(cert.VrfKeyHash),
		Pledge:        cert.Pledge,
		Cost:          cert.Cost,
		Margin:        &margin,
		RewardAccount: rewardAccount,
		Owners:        ownerSet,
		Relays:        relays,
		Metadata:      cert.PoolMetadata,
	}
}

// rewardAccountAddress builds the reward address a pool registration names.
func rewardAccountAddress(
	networkID uint8,
	credentialTag uint8,
	keyHash lcommon.AddrKeyHash,
) (lcommon.Address, error) {
	addrType := uint8(lcommon.AddressTypeNoneKey)
	if credentialTag == 1 {
		addrType = lcommon.AddressTypeNoneScript
	}
	return lcommon.NewAddressFromParts(addrType, networkID, nil, keyHash[:])
}

// queryShelleyStakePoolParams answers GetStakePoolParams: the registration
// parameters of each requested pool that is currently registered. Pools that
// are not registered are omitted, and an empty request yields an empty map.
//
// Live-only: pool registrations carry no per-point history to read back.
func (ls *LedgerState) queryShelleyStakePoolParams(
	poolIds []ledger.PoolId,
) (any, error) {
	if err := checkLocalStateQueryItemLimit(
		"GetStakePoolParams",
		len(poolIds),
	); err != nil {
		return nil, err
	}
	txn := ls.db.Transaction(false)
	defer txn.Release()
	networkID := uint8(ls.NewView(txn).NetworkId()) // #nosec G115 -- 0 or 1
	result := make(map[ledger.PoolId]stakePoolParams, len(poolIds))
	for _, poolId := range poolIds {
		pool, err := ls.db.GetPool(lcommon.PoolKeyHash(poolId), false, txn)
		if err != nil {
			if errors.Is(err, models.ErrPoolNotFound) {
				continue
			}
			return nil, err
		}
		reg, _, _, ok := latestPoolRegistration(pool)
		if !ok {
			continue
		}
		cert, err := poolRegistrationCertificate(pool, reg)
		if err != nil {
			return nil, err
		}
		rewardAccount, err := rewardAccountAddress(
			networkID,
			pool.RewardAccountCredentialTag,
			cert.RewardAccount,
		)
		if err != nil {
			return nil, fmt.Errorf("pool %x reward account: %w", poolId[:], err)
		}
		result[poolId] = newStakePoolParams(cert, rewardAccount)
	}
	return []any{result}, nil
}

// shelleyExtraConfigCBOR encodes the genesis injection data as the ledger's
// StrictMaybe ShelleyExtraConfig: an empty array when there is none, else a
// one-element array holding the record of initial funds, stake pools and
// stake credentials. Each member is an embedded InjectionData, [2, map].
//
// The values come from a copy of the genesis without its base funds and
// staking, so the exported genesis accessors yield only the injected entries.
func shelleyExtraConfigCBOR(
	genesis *gshelley.ShelleyGenesis,
	networkID uint8,
) (cbor.RawMessage, error) {
	if genesis.ExtraConfig == nil {
		return cbor.Encode([]any{})
	}
	injected := *genesis
	injected.InitialFunds = nil
	injected.Staking = gshelley.GenesisStaking{}

	utxos, err := injected.GenesisUtxos()
	if err != nil {
		return nil, err
	}
	funds := make(map[cbor.ByteString]uint64, len(utxos))
	for _, utxo := range utxos {
		addr := utxo.Output.Address()
		addrBytes, err := addr.Bytes()
		if err != nil {
			return nil, err
		}
		funds[cbor.NewByteString(addrBytes)] = utxo.Output.Amount().Uint64()
	}

	certs, delegations, err := injected.InitialPools()
	if err != nil {
		return nil, err
	}
	pools := make(map[lcommon.Blake2b224]stakePoolParams, len(certs))
	for _, cert := range certs {
		rewardAccount, err := rewardAccountAddress(
			networkID,
			0,
			cert.RewardAccount,
		)
		if err != nil {
			return nil, err
		}
		pools[lcommon.Blake2b224(cert.Operator)] = newStakePoolParams(
			&cert,
			rewardAccount,
		)
	}
	credentials := make(map[lcommon.Blake2b224]lcommon.Blake2b224)
	for poolID, addrs := range delegations {
		poolHash, err := hex.DecodeString(poolID)
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			credentials[addr.StakeKeyHash()] = lcommon.NewBlake2b224(poolHash)
		}
	}

	return cbor.Encode([]any{
		[]any{
			injectionData(funds),
			injectionData(pools),
			injectionData(credentials),
		},
	})
}

// injectionData wraps entries as an embedded InjectionData.
func injectionData[M ~map[K]V, K comparable, V any](entries M) []any {
	return []any{2, entries}
}
