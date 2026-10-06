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
	"fmt"
	"math/big"
	"net"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gshelley "github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// blsKeyMinProtocolMajor is the protocol version from which the ledger
// encodes a pool's registered BLS voting key in StakePoolParams.
const blsKeyMinProtocolMajor = 12

// stakePoolParams is the ledger's StakePoolParams record. The owners are in
// ascending order, a tag-258 set from protocol version 9 and a plain array
// before it; the margin is a tag-30 rational at every version, an absent
// metadata is null, and empty owner and relay lists are empty arrays rather
// than null. A non-nil BlsKey is spliced in as the third element, making a
// ten-element array; it is set only from protocol version 12.
type stakePoolParams struct {
	Operator      lcommon.Blake2b224
	VrfKeyHash    lcommon.Blake2b256
	Pledge        uint64
	Cost          uint64
	Margin        *cbor.Rat
	RewardAccount lcommon.Address
	Owners        *cbor.SetType[lcommon.Blake2b224]
	Relays        []lcommon.PoolRelay
	Metadata      *lcommon.PoolMetadata
	BlsKey        *lcommon.LeiosKey
}

// MarshalCBOR encodes the record as a flat array, omitting the BLS key
// element when the pool has none.
func (p stakePoolParams) MarshalCBOR() ([]byte, error) {
	fields := []any{p.Operator, p.VrfKeyHash}
	if p.BlsKey != nil {
		fields = append(fields, p.BlsKey)
	}
	fields = append(
		fields,
		p.Pledge,
		p.Cost,
		p.Margin,
		p.RewardAccount,
		p.Owners,
		p.Relays,
		p.Metadata,
	)
	return cbor.Encode(fields)
}

// wireOrderRelays returns relays with their IP addresses in the ledger's
// wire order: each 32-bit word of an address is little-endian, so an IPv4
// address is byte-reversed and an IPv6 address is reversed within each
// four-byte group. Relays read back from chain data already hold wire
// order; only addresses parsed from text, as in genesis, need this.
func wireOrderRelays(relays []lcommon.PoolRelay) []lcommon.PoolRelay {
	out := slices.Clone(relays)
	for i := range out {
		if out[i].Ipv4 != nil {
			out[i].Ipv4 = reverseIPWords(*out[i].Ipv4)
		}
		if out[i].Ipv6 != nil {
			out[i].Ipv6 = reverseIPWords(*out[i].Ipv6)
		}
	}
	return out
}

func reverseIPWords(ip net.IP) *net.IP {
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	swapped := make(net.IP, len(ip))
	for i := 0; i+4 <= len(ip); i += 4 {
		for j := range 4 {
			swapped[i+j] = ip[i+3-j]
		}
	}
	return &swapped
}

// newStakePoolParams adapts a registration certificate. rewardAccount is
// passed separately because the certificate keeps only the key hash of the
// reward account, not its credential type. tagOwners selects the set tag,
// which node-to-client results carry and the Shelley-version genesis
// encoding does not.
func newStakePoolParams(
	cert *lcommon.PoolRegistrationCertificate,
	rewardAccount lcommon.Address,
	tagOwners bool,
) stakePoolParams {
	margin := cbor.Rat{Rat: big.NewRat(0, 1)}
	if cert.Margin.Rat != nil {
		margin = cbor.Rat(cert.Margin)
	}
	owners := slices.Clone(cert.PoolOwners)
	slices.SortFunc(owners, func(a, b lcommon.AddrKeyHash) int {
		return bytes.Compare(a[:], b[:])
	})
	ownerHashes := make([]lcommon.Blake2b224, 0, len(owners))
	for _, owner := range owners {
		ownerHashes = append(ownerHashes, lcommon.Blake2b224(owner))
	}
	ownerSet := cbor.NewSetType(ownerHashes, tagOwners)
	relays := make([]lcommon.PoolRelay, 0, len(cert.Relays))
	relays = append(relays, cert.Relays...)
	return stakePoolParams{
		Operator:      lcommon.Blake2b224(cert.Operator),
		VrfKeyHash:    lcommon.Blake2b256(cert.VrfKeyHash),
		Pledge:        cert.Pledge,
		Cost:          cert.Cost,
		Margin:        &margin,
		RewardAccount: rewardAccount,
		Owners:        &ownerSet,
		Relays:        relays,
		Metadata:      cert.PoolMetadata,
		BlsKey:        cert.LeiosKey,
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

// queryShelleyStakePoolParams answers GetStakePoolParams: the parameters in
// effect for each requested pool that is currently registered. A
// re-registration made during the current epoch is the ledger's future
// parameters until the next epoch boundary, so it is not reported yet. Pools
// that are not registered are omitted, and an empty request yields an empty
// map.
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
	result := make(map[ledger.PoolId]stakePoolParams, len(poolIds))
	if len(poolIds) == 0 {
		return []any{result}, nil
	}
	txn := ls.db.Transaction(false)
	defer txn.Release()
	networkID := uint8(ls.NewView(txn).NetworkId()) // #nosec G115 -- 0 or 1
	consensus, tip := ls.loadStateSnapshots()
	epoch := consensus.currentEpoch
	tipSlot := tip.currentTip.Point.Slot
	includeBlsKey := false
	if pv, err := GetProtocolVersion(consensus.currentPParams); err == nil {
		includeBlsKey = pv.Major >= blsKeyMinProtocolMajor
	}
	keyHashes := make([]lcommon.PoolKeyHash, 0, len(poolIds))
	for _, poolId := range poolIds {
		keyHashes = append(keyHashes, lcommon.PoolKeyHash(poolId))
	}
	regs, err := ls.db.Metadata().GetPoolRegistrationsEffectiveForEpoch(
		keyHashes,
		epoch.StartSlot,
		epoch.EpochId,
		tipSlot,
		txn.Metadata(),
	)
	if err != nil {
		return nil, err
	}
	for i := range regs {
		reg := &regs[i]
		cert, err := poolRegistrationCertificate(&models.Pool{
			PoolKeyHash:   reg.PoolKeyHash,
			VrfKeyHash:    reg.VrfKeyHash,
			RewardAccount: reg.RewardAccount,
			Margin:        reg.Margin,
			Pledge:        reg.Pledge,
			Cost:          reg.Cost,
		}, reg)
		if err != nil {
			return nil, err
		}
		// Genesis and snapshot-import registrations originate as textual IP
		// addresses. On-chain certificates are decoded from ledger wire bytes.
		if reg.CertificateID == 0 {
			cert.Relays = wireOrderRelays(cert.Relays)
		}
		if includeBlsKey &&
			len(reg.LeiosKeyPublic) > 0 &&
			len(reg.LeiosKeyPossessionProof) > 0 {
			cert.LeiosKey = &lcommon.LeiosKey{
				PublicKey:       slices.Clone(reg.LeiosKeyPublic),
				PossessionProof: slices.Clone(reg.LeiosKeyPossessionProof),
			}
		}
		rewardAccount, err := rewardAccountAddress(
			networkID,
			reg.RewardAccountCredentialTag,
			cert.RewardAccount,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"pool %x reward account: %w",
				reg.PoolKeyHash,
				err,
			)
		}
		result[ledger.PoolId(cert.Operator)] = newStakePoolParams(
			cert,
			rewardAccount,
			true,
		)
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
		cert.Relays = wireOrderRelays(cert.Relays)
		pools[lcommon.Blake2b224(cert.Operator)] = newStakePoolParams(
			&cert,
			rewardAccount,
			false,
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

	extra := genesis.ExtraConfig
	return cbor.Encode([]any{
		[]any{
			injectionData(extra.InitialFunds.Data != nil, funds),
			injectionData(extra.StakePools.Data != nil, pools),
			injectionData(extra.StakeCredentials.Data != nil, credentials),
		},
	})
}

// injectionData wraps entries as an embedded InjectionData, [2, map], or as
// NoInjection, [0], when the genesis has no data for the section.
func injectionData[M ~map[K]V, K comparable, V any](
	present bool,
	entries M,
) []any {
	if !present {
		return []any{0}
	}
	return []any{2, entries}
}
