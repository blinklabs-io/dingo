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
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
)

// ParseSnapShots decodes the stake distribution snapshots
// (mark, set, go) from the EpochState.
// SnapShots = [mark, set, go, fee]
// Each SnapShot = [Stake, Delegations, PoolParams]
func ParseSnapShots(data cbor.RawMessage) (*ParsedSnapShots, error) {
	ss, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf("decoding SnapShots: %w", err)
	}
	if len(ss) < 3 {
		return nil, fmt.Errorf(
			"SnapShots has %d elements, expected at least 3",
			len(ss),
		)
	}

	// Warnings from snapshot parsers indicate skipped entries.
	var warnings []error

	mark, err := parseSnapShot(ss[0])
	if err != nil {
		if mark == nil {
			return nil, fmt.Errorf(
				"parsing mark snapshot: %w", err,
			)
		}
		warnings = append(warnings, fmt.Errorf(
			"mark: %w", err,
		))
	}

	set, err := parseSnapShot(ss[1])
	if err != nil {
		if set == nil {
			return nil, fmt.Errorf(
				"parsing set snapshot: %w", err,
			)
		}
		warnings = append(warnings, fmt.Errorf(
			"set: %w", err,
		))
	}

	goSnap, err := parseSnapShot(ss[2])
	if err != nil {
		if goSnap == nil {
			return nil, fmt.Errorf(
				"parsing go snapshot: %w", err,
			)
		}
		warnings = append(warnings, fmt.Errorf(
			"go: %w", err,
		))
	}

	var fee uint64
	if len(ss) > 3 {
		if _, err := cbor.Decode(ss[3], &fee); err != nil {
			// Fee might be optional or zero, don't fail
			fee = 0
		}
	}

	return &ParsedSnapShots{
		Mark: *mark,
		Set:  *set,
		Go:   *goSnap,
		Fee:  fee,
	}, errors.Join(warnings...)
}

// parseSnapShot decodes a single SnapShot.
// SnapShot = [Stake, Delegations, PoolParams]
func parseSnapShot(
	data []byte,
) (*ParsedSnapShot, error) {
	snap, err := decodeRawArray(data)
	if err != nil {
		return nil, fmt.Errorf("decoding SnapShot: %w", err)
	}
	if len(snap) < 3 {
		return nil, fmt.Errorf(
			"SnapShot has %d elements, expected 3",
			len(snap),
		)
	}

	// Parse Stake: map[Credential]Coin
	// Warnings from these parsers indicate skipped entries,
	// not fatal errors, so we collect them.
	var warnings []error
	stake, err := parseStakeMap(snap[0])
	if err != nil {
		if stake == nil {
			return nil, fmt.Errorf(
				"parsing stake map: %w", err,
			)
		}
		warnings = append(warnings, err)
	}

	// Parse Delegations: map[Credential]PoolKeyHash
	delegations, err := parseDelegationMap(snap[1])
	if err != nil {
		if delegations == nil {
			return nil, fmt.Errorf(
				"parsing delegation map: %w", err,
			)
		}
		warnings = append(warnings, err)
	}

	// Parse PoolParams: map[PoolKeyHash]PoolParams
	poolParams, err := parsePoolParamsMap(snap[2])
	if err != nil {
		if poolParams == nil {
			return nil, fmt.Errorf(
				"parsing pool params map: %w", err,
			)
		}
		warnings = append(warnings, err)
	}

	return &ParsedSnapShot{
		Stake:       stake,
		Delegations: delegations,
		PoolParams:  poolParams,
	}, errors.Join(warnings...)
}

// parseStakeMap decodes a credential -> coin map. Handles both
// definite and indefinite-length maps. Returns a warning if any
// entries were skipped due to decode errors.
func parseStakeMap(
	data cbor.RawMessage,
) (map[string]uint64, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding stake map: %w", err,
		)
	}

	result := make(map[string]uint64, len(entries))
	var skipped int
	for _, entry := range entries {
		cred, err := parseCredential(entry.KeyRaw)
		if err != nil {
			skipped++
			continue
		}

		var amount uint64
		if _, err := cbor.Decode(
			entry.ValueRaw, &amount,
		); err != nil {
			skipped++
			continue
		}

		result[hex.EncodeToString(cred)] = amount
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"stake map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return result, warning
}

// parseDelegationMap decodes a credential -> pool key hash map.
// Handles both definite and indefinite-length maps. Returns a
// warning if any entries were skipped due to decode errors.
func parseDelegationMap(
	data cbor.RawMessage,
) (map[string][]byte, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding delegation map: %w", err,
		)
	}

	result := make(map[string][]byte, len(entries))
	var skipped int
	for _, entry := range entries {
		cred, err := parseCredential(entry.KeyRaw)
		if err != nil {
			skipped++
			continue
		}

		var poolHash []byte
		if _, err := cbor.Decode(
			entry.ValueRaw, &poolHash,
		); err != nil {
			skipped++
			continue
		}

		result[hex.EncodeToString(cred)] = poolHash
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"delegation map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return result, warning
}

// parsePoolParamsMap decodes a pool key hash -> pool params map.
// Handles both definite and indefinite-length maps. Returns a
// warning if any entries were skipped due to decode errors.
func parsePoolParamsMap(
	data cbor.RawMessage,
) (map[string]*ParsedPoolParams, error) {
	entries, err := decodeMapEntries(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding pool params map: %w", err,
		)
	}

	result := make(
		map[string]*ParsedPoolParams,
		len(entries),
	)
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

		params := &ParsedPoolParams{
			PoolKeyHash:   pool.PoolKeyHash,
			VrfKeyHash:    pool.VrfKeyHash,
			Pledge:        pool.Pledge,
			Cost:          pool.Cost,
			MarginNum:     pool.MarginNum,
			MarginDen:     pool.MarginDen,
			RewardAccount: pool.RewardAccount,
			Owners:        pool.Owners,
		}

		result[hex.EncodeToString(poolKeyHash)] = params
	}

	var warning error
	if skipped > 0 {
		warning = fmt.Errorf(
			"pool params map: skipped %d of %d entries",
			skipped, len(entries),
		)
	}
	return result, warning
}

// AggregatePoolStake aggregates per-credential stake into per-pool
// totals, producing PoolStakeSnapshot models suitable for database
// storage.
func AggregatePoolStake(
	snap *ParsedSnapShot,
	epoch uint64,
	snapshotType string,
	capturedSlot uint64,
) []*models.PoolStakeSnapshot {
	// Build per-pool aggregation
	type poolAgg struct {
		totalStake     uint64
		delegatorCount uint64
	}
	poolMap := make(map[string]*poolAgg)

	for credHex, poolHash := range snap.Delegations {
		poolHex := hex.EncodeToString(poolHash)
		agg, ok := poolMap[poolHex]
		if !ok {
			agg = &poolAgg{}
			poolMap[poolHex] = agg
		}

		// Add this credential's stake to the pool total
		if stake, ok := snap.Stake[credHex]; ok {
			agg.totalStake += stake
		}
		agg.delegatorCount++
	}

	// Convert to models
	snapshots := make(
		[]*models.PoolStakeSnapshot,
		0,
		len(poolMap),
	)
	for poolHex, agg := range poolMap {
		poolKeyHash, err := hex.DecodeString(poolHex)
		if err != nil {
			// poolHex was self-encoded via hex.EncodeToString,
			// so decode should never fail.
			continue
		}

		snapshots = append(snapshots, &models.PoolStakeSnapshot{
			Epoch:          epoch,
			SnapshotType:   snapshotType,
			PoolKeyHash:    poolKeyHash,
			TotalStake:     types.Uint64(agg.totalStake),
			DelegatorCount: agg.delegatorCount,
			CapturedSlot:   capturedSlot,
		})
	}

	return snapshots
}
