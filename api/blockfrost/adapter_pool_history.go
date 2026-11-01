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
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// knownPool resolves a pool identifier to its key hash and reports
// models.ErrPoolNotFound for a pool the node has never seen register.
func (a *NodeAdapter) knownPool(
	ctx context.Context,
	poolID string,
) ([]byte, *models.Pool, error) {
	poolKeyHash, err := parsePoolID(poolID)
	if err != nil {
		return nil, nil, err
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	pool, err := db.Metadata().GetPool(
		lcommon.PoolKeyHash(poolKeyHash), true, txn.Metadata(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("get pool %x: %w", poolKeyHash, err)
	}
	if pool == nil {
		return nil, nil, fmt.Errorf(
			"pool %x: %w",
			poolKeyHash,
			models.ErrPoolNotFound,
		)
	}
	return poolKeyHash, pool, nil
}

// PoolsRetired returns the pools whose retirement has taken effect.
func (a *NodeAdapter) PoolsRetired(
	ctx context.Context,
	params PaginationParams,
) ([]PoolRetiringInfo, int, error) {
	current, err := a.currentEpochNumber()
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []PoolRetiringInfo{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	rows, total, err := db.Metadata().GetRetiredPoolsPage(
		current, params.Count, offset,
		params.Order == PaginationOrderDesc, txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get retired pools: %w", err)
	}
	ret := make([]PoolRetiringInfo, 0, len(rows))
	for _, row := range rows {
		poolID, err := poolIDString(row.PoolKeyHash)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, PoolRetiringInfo{PoolID: poolID, Epoch: row.Epoch})
	}
	return ret, total, nil
}

// PoolHistory returns a pool's per-epoch stake, size, blocks and rewards.
// The snapshot labelled E is the distribution active in epoch E+1, so a row
// reports epoch E+1 and only snapshots whose epoch has begun are listed.
func (a *NodeAdapter) PoolHistory(
	ctx context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolHistoryInfo, int, error) {
	poolKeyHash, _, err := a.knownPool(ctx, poolID)
	if err != nil {
		return nil, 0, err
	}
	current, err := a.currentEpochNumber()
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []PoolHistoryInfo{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	rows, total, err := db.Metadata().GetPoolHistory(
		poolKeyHash, current, params.Count, offset, params.Order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get pool history: %w", err)
	}
	ret := make([]PoolHistoryInfo, 0, len(rows))
	for _, row := range rows {
		epoch := row.Epoch + 1
		epochRow, err := a.ledgerState.EpochInfo(epoch)
		if err != nil {
			return nil, 0, fmt.Errorf("get epoch %d: %w", epoch, err)
		}
		endSlot := epochRow.StartSlot + uint64(epochRow.LengthInSlots) - 1
		counts, total, err := db.Metadata().CountPoolBlocksInSlotRange(
			[]lcommon.PoolKeyHash{lcommon.PoolKeyHash(poolKeyHash)},
			epochRow.StartSlot, endSlot, txn.Metadata(),
		)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"count pool blocks in epoch %d: %w",
				epoch,
				err,
			)
		}
		// Blocks at or below a bootstrap anchor are not observed; they come
		// from the snapshot's imported counts, or are unknown.
		counts, _, known, err := database.MergeImportedPoolBlockCounts(
			db.Metadata(), txn.Metadata(), epoch, epochRow.StartSlot,
			counts, total,
		)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"merge imported pool blocks in epoch %d: %w",
				epoch,
				err,
			)
		}
		if !known {
			return nil, 0, fmt.Errorf(
				"blocks of epoch %d: %w", epoch, ErrHistoryNotRetained,
			)
		}
		if !row.TotalActiveStakeKnown {
			return nil, 0, fmt.Errorf(
				"active stake of epoch %d: %w", epoch, ErrHistoryNotRetained,
			)
		}
		activeSize := 0.0
		if row.TotalActiveStake > 0 {
			activeSize = float64(row.Stake) / float64(row.TotalActiveStake)
		}
		ret = append(ret, PoolHistoryInfo{
			Epoch:           epoch,
			Blocks:          saturatingInt(counts[string(poolKeyHash)]),
			ActiveStake:     strconv.FormatUint(row.Stake, 10),
			ActiveSize:      activeSize,
			DelegatorsCount: row.DelegatorCount,
			Rewards:         strconv.FormatUint(row.MemberRewards, 10),
			Fees:            strconv.FormatUint(row.LeaderRewards, 10),
		})
	}
	return ret, total, nil
}

// PoolRelays returns the relays of the pool's latest registration.
func (a *NodeAdapter) PoolRelays(
	ctx context.Context,
	poolID string,
) ([]PoolRelayInfo, error) {
	_, pool, err := a.knownPool(ctx, poolID)
	if err != nil {
		return nil, err
	}
	ret := []PoolRelayInfo{}
	if len(pool.Registration) == 0 {
		return ret, nil
	}
	for _, relay := range pool.Registration[0].Relays {
		info := PoolRelayInfo{Port: int(relay.Port)}
		if relay.Ipv4 != nil {
			info.IPv4 = relay.Ipv4.String()
		}
		if relay.Ipv6 != nil {
			info.IPv6 = relay.Ipv6.String()
		}
		if relay.Hostname != "" {
			// A multi-host relay names an SRV record and carries no port; a
			// row stored before relay types were recorded is told apart by
			// that missing port.
			srv := relay.Port == 0
			if relay.Type != nil {
				srv = *relay.Type == lcommon.PoolRelayTypeMultiHostName
			}
			if srv {
				info.DNSSrv = relay.Hostname
			} else {
				info.DNS = relay.Hostname
			}
		}
		ret = append(ret, info)
	}
	return ret, nil
}

// PoolDelegators returns the registered delegators of a pool.
func (a *NodeAdapter) PoolDelegators(
	ctx context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolDelegatorInfo, int, error) {
	poolKeyHash, _, err := a.knownPool(ctx, poolID)
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []PoolDelegatorInfo{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	rows, total, err := db.Metadata().GetPoolDelegators(
		poolKeyHash, params.Count, offset, params.Order, txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get pool delegators: %w", err)
	}
	ret := make([]PoolDelegatorInfo, 0, len(rows))
	for _, row := range rows {
		address, err := a.stakeAddressFromTag(row.CredentialTag, row.StakingKey)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, PoolDelegatorInfo{
			Address:   address,
			LiveStake: strconv.FormatUint(row.Stake, 10),
		})
	}
	return ret, total, nil
}

// PoolBlocks returns the hashes of the blocks a pool minted.
func (a *NodeAdapter) PoolBlocks(
	ctx context.Context,
	poolID string,
	params PaginationParams,
) ([]string, int, error) {
	poolKeyHash, _, err := a.knownPool(ctx, poolID)
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []string{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	if err := checkPoolBlockHistoryRetained(
		db, txn, 0, a.ledgerState.Tip().Point.Slot,
	); err != nil {
		return nil, 0, fmt.Errorf("pool blocks: %w", err)
	}
	hashes, total, err := db.Metadata().GetPoolBlockHashesInSlotRange(
		poolKeyHash, 0, noSlotUpperBound, params.Count, offset, params.Order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get pool blocks: %w", err)
	}
	return hexHashes(hashes), total, nil
}

// PoolUpdates returns a pool's registration and retirement certificates.
func (a *NodeAdapter) PoolUpdates(
	ctx context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolUpdateInfo, int, error) {
	poolKeyHash, _, err := a.knownPool(ctx, poolID)
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []PoolUpdateInfo{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	events, total, err := db.Metadata().GetPoolCertificateEvents(
		poolKeyHash, params.Count, offset, params.Order, txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get pool updates: %w", err)
	}
	ret := make([]PoolUpdateInfo, 0, len(events))
	for _, event := range events {
		ret = append(ret, PoolUpdateInfo{
			TxHash:    hex.EncodeToString(event.TxHash),
			CertIndex: int(event.CertIndex),
			Action:    event.Action,
		})
	}
	return ret, total, nil
}
