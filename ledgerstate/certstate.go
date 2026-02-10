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

package ledgerstate

import (
	"cmp"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// ParseCertState decodes the CertState from raw CBOR.
// CertState = [VState, PState, DState]
func ParseCertState(data cbor.RawMessage) (*ParsedCertState, error) {
	certState, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf("decoding CertState: %w", err)
	}

	// Conway-era CertState may have 6 elements when encoded
	// as a flat structure:
	// [VState_dreps, VState_committee, VState_dormant,
	//  PState_blob, DState_blob, ???]
	// OR it may be the classic 3-element
	// [VState, PState, DState] structure.
	//
	// Strategy: if we have exactly 3 elements, use them
	// directly. Otherwise, find the structural elements.
	if len(certState) == 3 {
		return parseCertState3(certState)
	}

	// For 6+ element Conway structure, find PState and DState
	// by looking for the large structural elements.
	return parseCertStateConway(certState)
}

// parseCertState3 handles the classic 3-element CertState:
// [VState, PState, DState]
func parseCertState3(
	certState [][]byte,
) (*ParsedCertState, error) {
	result := &ParsedCertState{}
	var warnings []error

	dreps, err := parseVState(certState[0])
	if err != nil {
		return nil, fmt.Errorf("parsing VState: %w", err)
	}
	result.DReps = dreps

	pools, err := parsePState(certState[1])
	if err != nil {
		if pools == nil {
			return nil, fmt.Errorf(
				"parsing PState: %w", err,
			)
		}
		warnings = append(warnings, err)
	}
	result.Pools = pools

	accounts, err := parseDState(certState[2])
	if err != nil {
		if accounts == nil {
			return nil, fmt.Errorf(
				"parsing DState: %w", err,
			)
		}
		warnings = append(warnings, err)
	}
	result.Accounts = accounts

	return result, errors.Join(warnings...)
}

// parseCertStateConway handles the 6-element Conway CertState
// where VState, PState, DState are flattened:
//
//	[VState(3 fields), PState(N fields), DState(N fields)]
//
// or VState fields are inlined into the top-level array.
// We identify components by their CBOR types and sizes.
func parseCertStateConway(
	certState [][]byte,
) (*ParsedCertState, error) {
	result := &ParsedCertState{}
	var warnings []error

	// In the Conway-era 6-element structure observed from
	// preview network (node 10.x):
	//   [0] empty map (a0) - VState drep map (empty)
	//   [1] big integer     - VState committee/dormant
	//   [2] uint32          - VState field
	//   [3] array(7)        - PState (pool state blob)
	//   [4] indef-map (bf)  - DState credential map
	//   [5] integer 0       - DState field
	//
	// Find the elements by type signature:
	// - PState blob: large array (the pool state structure)
	// - DState blob: large map or indef-map (credential map)

	// Find the PState: largest array element
	pIdx := -1
	pSize := 0
	for i, elem := range certState {
		if len(elem) == 0 {
			continue
		}
		major := elem[0] >> 5
		if major == 4 && len(elem) > pSize {
			pIdx = i
			pSize = len(elem)
		}
	}

	// Find the DState: largest map element whose keys decode as
	// credentials ([type, hash] arrays). We sort map candidates by
	// size descending and pick the first that passes validation.
	// This prevents misidentifying the pool deposit map as DState
	// on networks where pools outnumber delegators.
	dIdx := -1
	type mapCandidate struct {
		idx  int
		size int
	}
	var mapCandidates []mapCandidate
	for i, elem := range certState {
		if len(elem) == 0 {
			continue
		}
		major := elem[0] >> 5
		isMap := major == 5 || elem[0] == 0xbf
		if isMap {
			mapCandidates = append(
				mapCandidates,
				mapCandidate{idx: i, size: len(elem)},
			)
		}
	}
	// Sort by size descending
	slices.SortFunc(
		mapCandidates,
		func(a, b mapCandidate) int {
			return cmp.Compare(b.size, a.size)
		},
	)
	for _, mc := range mapCandidates {
		if looksLikeCredentialMap(certState[mc.idx]) {
			dIdx = mc.idx
			break
		}
	}

	// Parse VState (DReps): look for a map element that is NOT the
	// DState credential map (dIdx) and not a simple integer/array.
	// In Conway 6-element layout, element [0] is the DRep map.
	// We call parseDRepMap directly because the raw element is a
	// DRep map, not a VState array [drepMap, ccHotKeys, ...].
	for i, elem := range certState {
		if len(elem) == 0 || i == pIdx || i == dIdx {
			continue
		}
		major := elem[0] >> 5
		isMap := major == 5 || elem[0] == 0xbf
		// The VState drep map is a map that is smaller than
		// the DState credential map.
		if isMap &&
			(dIdx < 0 ||
				len(elem) < len(certState[dIdx])) {
			dreps, vErr := parseDRepMap(elem)
			if dreps != nil {
				result.DReps = dreps
				if vErr != nil {
					warnings = append(warnings, vErr)
				}
				break
			}
		}
	}

	// Parse PState if found
	if pIdx >= 0 {
		pools, err := parsePStateConway(certState[pIdx])
		if err != nil {
			if pools == nil {
				return nil, fmt.Errorf(
					"parsing PState: %w", err,
				)
			}
			warnings = append(warnings, err)
		}
		result.Pools = pools
	} else {
		warnings = append(warnings, fmt.Errorf(
			"could not identify PState in Conway "+
				"CertState (%d elements); "+
				"pools will be empty",
			len(certState),
		))
	}

	// Parse DState if found - the large credential map
	if dIdx >= 0 {
		accounts, err := parseCredentialMap(certState[dIdx])
		if err != nil {
			if accounts == nil {
				return nil, fmt.Errorf(
					"parsing DState: %w", err,
				)
			}
			warnings = append(warnings, err)
		}
		result.Accounts = accounts
	} else {
		warnings = append(warnings, fmt.Errorf(
			"could not identify DState in Conway "+
				"CertState (%d elements); "+
				"accounts will be empty",
			len(certState),
		))
	}

	return result, errors.Join(warnings...)
}

// parsePStateConway decodes the Conway-era pool state where
// PState is encoded as an array of 7 elements rather than the
// traditional {poolParams, futurePoolParams, retiring, deposits}
// map.
func parsePStateConway(data []byte) ([]ParsedPool, error) {
	ps, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf("decoding PState: %w", err)
	}

	// Find map elements sorted by size. PState contains several
	// maps (pool params, future params, retiring, deposits).
	// Pool params is the largest; deposits is second-largest
	// (same key count but smaller values).
	type mapEntry struct {
		idx  int
		size int
	}
	var maps []mapEntry
	for i, elem := range ps {
		if len(elem) == 0 {
			continue
		}
		major := elem[0] >> 5
		isMap := major == 5 || elem[0] == 0xbf
		if isMap {
			maps = append(maps, mapEntry{i, len(elem)})
		}
	}

	if len(maps) == 0 {
		// Fallback: try the first element as a map
		if len(ps) > 0 {
			return parseCertPoolParamsMap(ps[0])
		}
		return nil, nil
	}

	// Sort maps by size descending to identify pool params
	// (largest) and deposits (second-largest).
	slices.SortFunc(
		maps,
		func(a, b mapEntry) int {
			return cmp.Compare(b.size, a.size)
		},
	)

	pools, warning := parseCertPoolParamsMap(ps[maps[0].idx])
	if pools == nil {
		return pools, warning
	}

	// Merge deposits from the remaining maps (if present).
	// Multiple maps share the same PoolKeyHash -> uint64 schema
	// (retiring holds epoch numbers, deposits holds lovelace).
	// We distinguish them by value range: deposit amounts are
	// >= 1 ADA (1,000,000 lovelace), epoch numbers are small.
	for _, m := range maps[1:] {
		deposits := parsePoolDeposits(ps[m.idx])
		if deposits != nil && looksLikeDeposits(deposits) {
			for i := range pools {
				if dep, ok := deposits[hex.EncodeToString(
					pools[i].PoolKeyHash,
				)]; ok {
					pools[i].Deposit = dep
				}
			}
			break
		}
	}

	return pools, warning
}

// parseCertPoolParamsMap decodes a map of pool key hash -> pool
// params for the CertState import path. Handles both definite
// and indefinite-length maps.
func parseCertPoolParamsMap(data []byte) ([]ParsedPool, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding pool params map: %w", err,
		)
	}

	pools := make([]ParsedPool, 0, len(entries))
	var skipped int
	for _, entry := range entries {
		var poolKeyHash []byte
		if _, pErr := cbor.Decode(
			entry.KeyRaw, &poolKeyHash,
		); pErr != nil {
			skipped++
			continue
		}

		pool, err := parsePoolParams(
			poolKeyHash, entry.ValueRaw,
		)
		if err != nil {
			skipped++
			continue
		}

		pools = append(pools, *pool)
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"pool params map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return pools, warning
}

// parseDState decodes the delegation state.
// DState contains:
//   - rewards: map[Credential]Coin
//   - delegations: map[Credential]PoolKeyHash
//   - ptrs: map[Ptr]Credential (legacy, skipped)
//   - various other fields
func parseDState(data []byte) ([]ParsedAccount, error) {
	ds, err := decodeRawElements(data)
	if err != nil {
		return nil, fmt.Errorf("decoding DState: %w", err)
	}

	// The DState structure has evolved across eras. In Conway:
	// DState = [dsUnified, dsFutureGenDelegs, dsGenDelegs, dsIRewards]
	// dsUnified = UMap = [credentials, ptrs]
	// credentials encodes as map of:
	//   credential -> UMElem = [rdPair, stakePool, drepDelegation, ...]
	if len(ds) < 1 {
		return nil, fmt.Errorf(
			"DState has %d elements, expected at least 1",
			len(ds),
		)
	}

	// Parse the unified map (UMap)
	return parseUMap(ds[0])
}

// parseUMap decodes the unified map containing rewards and
// delegations.
// UMap = [<credential-map>, <ptr-map>]
// credential-map: map[Credential]UMElem
// UMElem = [<RDPair>, <StakePool>, <DRep>, ...]
// RDPair = [<reward>, <deposit>]
func parseUMap(data []byte) ([]ParsedAccount, error) {
	umapArr, err := decodeRawArray(data)
	if err != nil {
		// If it's not an array, try as a direct map
		return parseCredentialMap(data)
	}

	if len(umapArr) < 1 {
		return nil, nil
	}

	return parseCredentialMap(umapArr[0])
}

// parseCredentialMap decodes the credential -> UMElem map using
// decodeMapEntries to handle indefinite-length maps (0xbf) and
// non-comparable array keys.
func parseCredentialMap(
	data []byte,
) ([]ParsedAccount, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding credential map: %w", err,
		)
	}

	accounts := make([]ParsedAccount, 0, len(entries))
	var skipped int
	var decodeErrors int
	for _, entry := range entries {
		cred, err := parseCredential(entry.KeyRaw)
		if err != nil {
			skipped++
			continue
		}

		acct := ParsedAccount{
			StakingKey: cred,
			Active:     true,
		}

		// UMElem = [RDPair, StakePool, DRep, ...]
		var elem []cbor.RawMessage
		if _, err := cbor.Decode(
			entry.ValueRaw, &elem,
		); err != nil {
			decodeErrors++
			accounts = append(accounts, acct)
			continue
		}

		// RDPair (index 0): [reward, deposit]
		if len(elem) > 0 {
			var rdPair []uint64
			if _, err := cbor.Decode(
				elem[0], &rdPair,
			); err == nil && len(rdPair) > 0 {
				acct.Reward = rdPair[0]
			}
		}

		// StakePool delegation (index 1): pool key hash
		if len(elem) > 1 {
			var poolHash []byte
			if _, err := cbor.Decode(
				elem[1], &poolHash,
			); err == nil && len(poolHash) == 28 {
				acct.PoolKeyHash = poolHash
			}
		}

		// DRep delegation (index 2): credential
		if len(elem) > 2 {
			drepCred, err := parseCredential(elem[2])
			if err == nil {
				acct.DRepCred = drepCred
			}
		}

		accounts = append(accounts, acct)
	}

	var warning error
	switch {
	case skipped > 0 && decodeErrors > 0:
		warning = fmt.Errorf(
			"credential map: skipped %d, %d decode errors out of %d entries",
			skipped, decodeErrors, len(entries),
		)
	case skipped > 0:
		warning = fmt.Errorf(
			"credential map: skipped %d of %d entries",
			skipped, len(entries),
		)
	case decodeErrors > 0:
		warning = fmt.Errorf(
			"credential map: %d decode errors out of %d entries",
			decodeErrors, len(entries),
		)
	}
	return accounts, warning
}

// parsePState decodes the pool state.
// PState = [poolParams, futurePoolParams, retiring, poolDeposits]
func parsePState(data []byte) ([]ParsedPool, error) {
	ps, err := decodeRawElements(data)
	if err != nil {
		return nil, fmt.Errorf("decoding PState: %w", err)
	}
	if len(ps) < 1 {
		return nil, nil
	}

	// Parse pool deposits if available (index 3)
	var poolDeposits map[string]uint64
	if len(ps) > 3 {
		poolDeposits = parsePoolDeposits(ps[3])
	}

	// Parse active pool registrations (index 0) using
	// decodeMapEntries to handle indefinite-length maps.
	entries, err := decodeMapEntries(ps[0])
	if err != nil {
		return nil, fmt.Errorf(
			"decoding poolParams map: %w", err,
		)
	}

	pools := make([]ParsedPool, 0, len(entries))
	var skipped int
	for _, entry := range entries {
		var poolKeyHash []byte
		if _, pErr := cbor.Decode(
			entry.KeyRaw, &poolKeyHash,
		); pErr != nil {
			skipped++
			continue
		}

		pool, err := parsePoolParams(
			poolKeyHash, entry.ValueRaw,
		)
		if err != nil {
			skipped++
			continue
		}

		if dep, ok := poolDeposits[hex.EncodeToString(poolKeyHash)]; ok {
			pool.Deposit = dep
		}

		pools = append(pools, *pool)
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"PState pool params: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return pools, warning
}

// parsePoolDeposits decodes the pool deposits map.
// Returns nil on decode failure. Skipped entries are counted
// but not reported since deposits are supplementary data.
func parsePoolDeposits(data []byte) map[string]uint64 {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil
	}

	deposits := make(map[string]uint64, len(entries))
	for _, entry := range entries {
		var keyHash []byte
		if _, err := cbor.Decode(
			entry.KeyRaw, &keyHash,
		); err != nil {
			continue
		}

		var amount uint64
		if _, err := cbor.Decode(
			entry.ValueRaw, &amount,
		); err != nil {
			continue
		}

		deposits[hex.EncodeToString(keyHash)] = amount
	}

	return deposits
}

// looksLikeDeposits returns true if the map values are plausibly
// lovelace deposit amounts rather than epoch numbers. Pool deposits
// are >= 1 ADA (1,000,000 lovelace) on all networks, while epoch
// numbers are small (currently < 1,000). We check whether the
// majority of values exceed this threshold.
func looksLikeDeposits(m map[string]uint64) bool {
	const minDepositLovelace = 1_000_000 // 1 ADA
	if len(m) == 0 {
		return false
	}
	var large int
	for _, v := range m {
		if v >= minDepositLovelace {
			large++
		}
	}
	// Require at least half the values to look like deposits.
	// Use multiplication to avoid integer division rounding.
	return large*2 >= len(m)
}

// parsePoolParams decodes a single pool's parameters.
// PoolParams = [
//
//	operator,      -- PoolKeyHash (28 bytes)
//	vrfKeyHash,    -- 32 bytes
//	pledge,        -- Coin
//	cost,          -- Coin
//	margin,        -- UnitInterval (tag 30, [num, denom])
//	rewardAccount, -- reward address bytes
//	poolOwners,    -- set of key hashes
//	relays,        -- array of relay entries
//	poolMetadata,  -- optional [url, hash]
//
// ]
func parsePoolParams(
	poolKeyHash []byte,
	data []byte,
) (*ParsedPool, error) {
	var params []cbor.RawMessage
	if _, err := cbor.Decode(data, &params); err != nil {
		return nil, fmt.Errorf("decoding pool params: %w", err)
	}
	if len(params) < 7 {
		return nil, fmt.Errorf(
			"pool params has %d elements, expected at least 7",
			len(params),
		)
	}

	pool := &ParsedPool{
		PoolKeyHash: poolKeyHash,
	}

	// VRF key hash (index 1)
	if _, err := cbor.Decode(
		params[1],
		&pool.VrfKeyHash,
	); err != nil {
		return nil, fmt.Errorf("decoding VRF key hash: %w", err)
	}

	// Pledge (index 2)
	if _, err := cbor.Decode(
		params[2],
		&pool.Pledge,
	); err != nil {
		return nil, fmt.Errorf("decoding pledge: %w", err)
	}

	// Cost (index 3)
	if _, err := cbor.Decode(
		params[3],
		&pool.Cost,
	); err != nil {
		return nil, fmt.Errorf("decoding cost: %w", err)
	}

	// Margin (index 4) - CBOR tag 30 [num, denom]
	pool.MarginNum, pool.MarginDen = parseRational(params[4])
	if pool.MarginDen == 0 {
		pool.MarginDen = 1
	}

	// Reward account (index 5)
	if _, err := cbor.Decode(
		params[5],
		&pool.RewardAccount,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding reward account: %w",
			err,
		)
	}

	// Owners (index 6) - set of 28-byte key hashes
	var owners []cbor.RawMessage
	if _, err := cbor.Decode(params[6], &owners); err == nil {
		for _, ownerRaw := range owners {
			var ownerHash []byte
			if _, err := cbor.Decode(
				ownerRaw,
				&ownerHash,
			); err == nil {
				pool.Owners = append(pool.Owners, ownerHash)
			}
		}
	}

	// Relays (index 7) - array of relay entries
	if len(params) > 7 {
		pool.Relays = parseRelays(params[7])
	}

	// Pool metadata (index 8) - null or [url, hash]
	if len(params) > 8 {
		parsePoolMetadata(params[8], pool)
	}

	return pool, nil
}

// parseRelays decodes an array of relay entries from CBOR.
func parseRelays(data []byte) []ParsedRelay {
	var relayArr []cbor.RawMessage
	if _, err := cbor.Decode(data, &relayArr); err != nil {
		return nil
	}
	relays := make([]ParsedRelay, 0, len(relayArr))
	for _, raw := range relayArr {
		relay, err := parseRelay(raw)
		if err != nil {
			continue
		}
		relays = append(relays, *relay)
	}
	return relays
}

// parseRelay decodes a single relay entry.
func parseRelay(data []byte) (*ParsedRelay, error) {
	var fields []cbor.RawMessage
	if _, err := cbor.Decode(data, &fields); err != nil {
		return nil, err
	}
	if len(fields) < 1 {
		return nil, errors.New("empty relay array")
	}
	var relayType uint8
	if _, err := cbor.Decode(
		fields[0], &relayType,
	); err != nil {
		return nil, err
	}
	relay := &ParsedRelay{Type: relayType}
	switch relayType {
	case 0: // SingleHostAddr: [0, port, ipv4, ipv6]
		if len(fields) > 1 {
			var port uint16
			if _, err := cbor.Decode(
				fields[1], &port,
			); err == nil {
				relay.Port = port
			}
		}
		if len(fields) > 2 {
			var ipv4 []byte
			if _, err := cbor.Decode(
				fields[2], &ipv4,
			); err == nil {
				relay.IPv4 = ipv4
			}
		}
		if len(fields) > 3 {
			var ipv6 []byte
			if _, err := cbor.Decode(
				fields[3], &ipv6,
			); err == nil {
				relay.IPv6 = ipv6
			}
		}
	case 1: // SingleHostName: [1, port, hostname]
		if len(fields) > 1 {
			var port uint16
			if _, err := cbor.Decode(
				fields[1], &port,
			); err == nil {
				relay.Port = port
			}
		}
		if len(fields) > 2 {
			var hostname string
			if _, err := cbor.Decode(
				fields[2], &hostname,
			); err == nil {
				relay.Hostname = hostname
			}
		}
	case 2: // MultiHostName: [2, hostname]
		if len(fields) > 1 {
			var hostname string
			if _, err := cbor.Decode(
				fields[1], &hostname,
			); err == nil {
				relay.Hostname = hostname
			}
		}
	}
	return relay, nil
}

// parsePoolMetadata decodes the optional pool metadata field.
// Pool metadata is null or [url, hash].
func parsePoolMetadata(
	data []byte,
	pool *ParsedPool,
) {
	var meta []cbor.RawMessage
	if _, err := cbor.Decode(data, &meta); err != nil {
		return // null or invalid
	}
	if len(meta) >= 2 {
		var url string
		if _, err := cbor.Decode(
			meta[0], &url,
		); err == nil {
			pool.MetadataUrl = url
		}
		var hash []byte
		if _, err := cbor.Decode(
			meta[1], &hash,
		); err == nil {
			pool.MetadataHash = hash
		}
	}
}

// parseVState decodes the voting/DRep state.
// VState = [dreps, ccHotKeys, numDormantEpochs, ...]
func parseVState(data []byte) ([]ParsedDRep, error) {
	vs, err := decodeRawElements(data)
	if err != nil {
		return nil, fmt.Errorf("decoding VState: %w", err)
	}
	if len(vs) < 1 {
		return nil, nil
	}

	// Parse DRep registrations (index 0)
	return parseDRepMap(vs[0])
}

// parseDRepMap decodes a DRep credential -> DRepState map.
// Handles both definite and indefinite-length maps via
// decodeMapEntries.
func parseDRepMap(data []byte) ([]ParsedDRep, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding drep map: %w", err,
		)
	}

	dreps := make([]ParsedDRep, 0, len(entries))
	var skipped int
	for _, entry := range entries {
		cred, err := parseCredential(entry.KeyRaw)
		if err != nil {
			skipped++
			continue
		}

		drep := ParsedDRep{
			Credential: cred,
			Active:     true,
		}

		// DRepState = [expiry, anchor, deposit, ...]
		var state []cbor.RawMessage
		if _, err := cbor.Decode(
			entry.ValueRaw, &state,
		); err == nil {
			if len(state) > 0 {
				var expiry uint64
				if _, err := cbor.Decode(
					state[0], &expiry,
				); err == nil {
					drep.ExpiryEpoch = expiry
				}
			}
			if len(state) > 1 {
				var anchor []cbor.RawMessage
				if _, err := cbor.Decode(
					state[1], &anchor,
				); err == nil && len(anchor) >= 2 {
					var url string
					if _, err := cbor.Decode(
						anchor[0], &url,
					); err == nil {
						drep.AnchorUrl = url
					}
					var hash []byte
					if _, err := cbor.Decode(
						anchor[1], &hash,
					); err == nil {
						drep.AnchorHash = hash
					}
				}
			}
			if len(state) > 2 {
				var deposit uint64
				if _, err := cbor.Decode(
					state[2], &deposit,
				); err == nil {
					drep.Deposit = deposit
				}
			}
		}

		dreps = append(dreps, drep)
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"drep map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return dreps, warning
}

// looksLikeCredentialMap samples up to 3 keys from a CBOR map
// and returns true if at least one decodes as a credential
// array ([type, hash] where type=0|1 and hash is 28 bytes).
// Only the array form is accepted — plain 28-byte byte strings
// (like pool key hashes) are rejected to distinguish the DState
// credential map from pool deposit maps.
func looksLikeCredentialMap(data []byte) bool {
	entries, err := decodeMapEntries(data)
	if err != nil || len(entries) == 0 {
		return false
	}
	limit := min(len(entries), 3)
	for i := range limit {
		if isCredentialArray(entries[i].KeyRaw) {
			return true
		}
	}
	return false
}

// isCredentialArray returns true if data decodes as a CBOR
// array of [type, hash] where type is 0 or 1 and hash is 28
// bytes. Unlike parseCredential, this rejects the plain
// byte-string fallback to avoid matching pool key hashes.
func isCredentialArray(data []byte) bool {
	var cred []cbor.RawMessage
	if _, err := cbor.Decode(data, &cred); err != nil {
		return false
	}
	if len(cred) < 2 {
		return false
	}
	var credType uint64
	if _, err := cbor.Decode(
		cred[0], &credType,
	); err != nil || credType > 1 {
		return false
	}
	var hash []byte
	if _, err := cbor.Decode(
		cred[1], &hash,
	); err != nil || len(hash) != 28 {
		return false
	}
	return true
}

// parseCredential decodes a CBOR-encoded credential.
// Credential = [type, hash] where type is 0 (key) or 1 (script),
// and hash is 28 bytes.
//
// TODO: The credential type (key=0, script=1) is extracted and
// validated but not returned. Callers that need the type should
// use a richer return type.
func parseCredential(data []byte) ([]byte, error) {
	var cred []cbor.RawMessage
	if _, err := cbor.Decode(data, &cred); err != nil {
		// Try as raw bytes (some encodings use plain byte string)
		var hash []byte
		if _, err2 := cbor.Decode(data, &hash); err2 == nil && len(hash) == 28 {
			return hash, nil
		}
		return nil, fmt.Errorf("decoding credential: %w", err)
	}
	if len(cred) < 2 {
		return nil, fmt.Errorf(
			"credential has %d elements, expected 2",
			len(cred),
		)
	}

	// Validate credential type (0 = key, 1 = script)
	var credType uint64
	if _, err := cbor.Decode(
		cred[0], &credType,
	); err != nil {
		return nil, fmt.Errorf(
			"decoding credential type: %w", err,
		)
	}
	if credType > 1 {
		return nil, fmt.Errorf(
			"invalid credential type %d, expected 0 or 1",
			credType,
		)
	}

	var hash []byte
	if _, err := cbor.Decode(cred[1], &hash); err != nil {
		return nil, fmt.Errorf("decoding credential hash: %w", err)
	}

	return hash, nil
}

// parseRational decodes a CBOR rational number (tag 30, [num, denom]).
func parseRational(data []byte) (uint64, uint64) {
	// Use gouroboros cbor.Rat which handles tag 30 natively
	var r cbor.Rat
	if _, err := cbor.Decode(data, &r); err == nil {
		rat := r.ToBigRat()
		if rat != nil &&
			rat.Num().IsUint64() &&
			rat.Denom().IsUint64() {
			num := rat.Num().Uint64()
			den := rat.Denom().Uint64()
			if den > 0 {
				return num, den
			}
		}
	}

	// Fallback: try as plain array [num, denom]
	var arr []uint64
	if _, err := cbor.Decode(data, &arr); err == nil &&
		len(arr) >= 2 && arr[1] > 0 {
		return arr[0], arr[1]
	}

	return 0, 1
}
