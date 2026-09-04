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

package server

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/midnight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// kind_order tie-break used when merging the four UTxO-event tables in
// GetUtxoEvents: events at the same (block_number, tx_index) sort
// create < spend < registration < deregistration.
const (
	utxoEventKindAssetCreate = iota
	utxoEventKindAssetSpend
	utxoEventKindRegistration
	utxoEventKindDeregistration
)

const (
	// defaultEventPageSize is used when a request's capacity field is the
	// proto3 zero value (unset), so a page always has a bounded size
	// instead of triggering an unbounded store scan and response.
	defaultEventPageSize = 1000
	// maxEventPageSize caps a client-requested capacity so a single page
	// can't force an unbounded scan/response.
	maxEventPageSize = 10000
)

// effectivePageSize clamps a client-requested capacity to
// [1, maxEventPageSize], substituting defaultEventPageSize when the request
// omitted it (proto3 zero value). The store's own Find*From contract treats
// limit <= 0 as "no SQL LIMIT", so RPC handlers must never forward a
// non-positive capacity as-is.
func effectivePageSize(requested uint32) int {
	switch {
	case requested == 0:
		return defaultEventPageSize
	case requested > maxEventPageSize:
		return maxEventPageSize
	default:
		return int(requested)
	}
}

// eventStore is the narrow slice of metadata.MetadataStore used by the
// UTxO-event query RPCs. It is declared locally, rather than depending on
// the full metadata.MetadataStore interface, so this gRPC-query package
// does not carry unrelated write-path and lifecycle methods; any store
// (e.g. *sqlstore.Store) that implements these methods
// satisfies it structurally, with no explicit declaration required.
type eventStore interface {
	FindMidnightAssetCreatesFrom(
		startBlock uint64,
		startTxIndex uint32,
		limit int,
		txn types.Txn,
	) ([]models.MidnightAssetCreate, error)
	FindMidnightAssetSpendsFrom(
		startBlock uint64,
		startTxIndex uint32,
		limit int,
		txn types.Txn,
	) ([]models.MidnightAssetSpend, error)
	FindMidnightRegistrationsFrom(
		startBlock uint64,
		startTxIndex uint32,
		limit int,
		txn types.Txn,
	) ([]models.MidnightRegistration, error)
	FindMidnightDeregistrationsFrom(
		startBlock uint64,
		startTxIndex uint32,
		limit int,
		txn types.Txn,
	) ([]models.MidnightDeregistration, error)
	// ReadTransaction opens a read-only, repeatable-read transaction bound
	// to ctx. GetUtxoEvents shares one across its four Find*From calls so
	// they observe a single consistent point in time — without it, the
	// four independent reads could straddle the live indexer committing a
	// new block between them, each table then reflecting a different "as
	// of" point and corrupting the merged next_position cursor.
	ReadTransaction(ctx context.Context) types.Txn
}

// GetAssetCreates returns cNIGHT UTxO creations starting strictly after
// (start_block, start_tx_index), ordered by (block_number, tx_index)
// ascending and capped at utxo_capacity rows.
func (s *service) GetAssetCreates(
	_ context.Context,
	req *midnight.AssetCreatesRequest,
) (*midnight.AssetCreatesResponse, error) {
	if s.metadata == nil {
		return nil, status.Error(
			codes.Unimplemented,
			"method GetAssetCreates not implemented",
		)
	}
	rows, err := s.metadata.FindMidnightAssetCreatesFrom(
		uint64(req.GetStartBlock()),
		req.GetStartTxIndex(),
		effectivePageSize(req.GetUtxoCapacity()),
		nil,
	)
	if err != nil {
		return nil, s.internalError("query asset creates", err)
	}
	creates := make([]*midnight.AssetCreate, len(rows))
	for i := range rows {
		pb, err := assetCreateToProto(&rows[i])
		if err != nil {
			return nil, s.internalError("convert asset create", err)
		}
		creates[i] = pb
	}
	return &midnight.AssetCreatesResponse{Creates: creates}, nil
}

// GetAssetSpends returns cNIGHT UTxO spends starting strictly after
// (start_block, start_tx_index), ordered by (block_number, tx_index)
// ascending and capped at utxo_capacity rows.
func (s *service) GetAssetSpends(
	_ context.Context,
	req *midnight.AssetSpendsRequest,
) (*midnight.AssetSpendsResponse, error) {
	if s.metadata == nil {
		return nil, status.Error(
			codes.Unimplemented,
			"method GetAssetSpends not implemented",
		)
	}
	rows, err := s.metadata.FindMidnightAssetSpendsFrom(
		uint64(req.GetStartBlock()),
		req.GetStartTxIndex(),
		effectivePageSize(req.GetUtxoCapacity()),
		nil,
	)
	if err != nil {
		return nil, s.internalError("query asset spends", err)
	}
	spends := make([]*midnight.AssetSpend, len(rows))
	for i := range rows {
		pb, err := assetSpendToProto(&rows[i])
		if err != nil {
			return nil, s.internalError("convert asset spend", err)
		}
		spends[i] = pb
	}
	return &midnight.AssetSpendsResponse{Spends: spends}, nil
}

// GetRegistrations returns mapping-validator registrations starting strictly
// after (start_block, start_tx_index), ordered by (block_number, tx_index)
// ascending and capped at utxo_capacity rows.
func (s *service) GetRegistrations(
	_ context.Context,
	req *midnight.RegistrationsRequest,
) (*midnight.RegistrationsResponse, error) {
	if s.metadata == nil {
		return nil, status.Error(
			codes.Unimplemented,
			"method GetRegistrations not implemented",
		)
	}
	rows, err := s.metadata.FindMidnightRegistrationsFrom(
		uint64(req.GetStartBlock()),
		req.GetStartTxIndex(),
		effectivePageSize(req.GetUtxoCapacity()),
		nil,
	)
	if err != nil {
		return nil, s.internalError("query registrations", err)
	}
	registrations := make([]*midnight.Registration, len(rows))
	for i := range rows {
		pb, err := registrationToProto(&rows[i])
		if err != nil {
			return nil, s.internalError("convert registration", err)
		}
		registrations[i] = pb
	}
	return &midnight.RegistrationsResponse{Registrations: registrations}, nil
}

// GetDeregistrations returns mapping-validator deregistrations starting
// strictly after (start_block, start_tx_index), ordered by (block_number,
// tx_index) ascending and capped at utxo_capacity rows.
func (s *service) GetDeregistrations(
	_ context.Context,
	req *midnight.DeregistrationsRequest,
) (*midnight.DeregistrationsResponse, error) {
	if s.metadata == nil {
		return nil, status.Error(
			codes.Unimplemented,
			"method GetDeregistrations not implemented",
		)
	}
	rows, err := s.metadata.FindMidnightDeregistrationsFrom(
		uint64(req.GetStartBlock()),
		req.GetStartTxIndex(),
		effectivePageSize(req.GetUtxoCapacity()),
		nil,
	)
	if err != nil {
		return nil, s.internalError("query deregistrations", err)
	}
	deregistrations := make([]*midnight.Deregistration, len(rows))
	for i := range rows {
		pb, err := deregistrationToProto(&rows[i])
		if err != nil {
			return nil, s.internalError("convert deregistration", err)
		}
		deregistrations[i] = pb
	}
	return &midnight.DeregistrationsResponse{
		Deregistrations: deregistrations,
	}, nil
}

// utxoEventMergeItem is one row from any of the four UTxO-event tables,
// carrying the fields needed to merge-sort across tables and to build the
// next_position cursor.
type utxoEventMergeItem struct {
	blockNumber uint64
	txIndex     uint32
	kindOrder   int
	blockHash   []byte
	timestampMs uint64
	// buildEvent lazily converts this row to its proto representation.
	// Deferred until after the end_block_hash and tx_capacity trims below
	// so an out-of-domain field on a row that the trims would exclude from
	// the page anyway can never fail the whole response -- only rows that
	// actually make the final page are converted.
	buildEvent func() (*midnight.UtxoEvent, error)
}

// GetUtxoEvents returns a merged, kind_order-tie-broken stream of all four
// UTxO-event tables starting strictly after start_position, capped at
// tx_capacity events and optionally stopped at end_block_hash.
//
// Fetching each table's own top tx_capacity rows (ordered ascending) is
// sufficient to compute the correct global top-tx_capacity merge: any row
// beyond position tx_capacity within its own table already has tx_capacity
// smaller-or-equal rows ahead of it in that single table, so it cannot be
// among the smallest tx_capacity rows overall.
func (s *service) GetUtxoEvents(
	ctx context.Context,
	req *midnight.UtxoEventsRequest,
) (*midnight.UtxoEventsResponse, error) {
	if s.metadata == nil {
		return nil, status.Error(
			codes.Unimplemented,
			"method GetUtxoEvents not implemented",
		)
	}
	var startBlock uint64
	var startTxIndex uint32
	if pos := req.GetStartPosition(); pos != nil {
		startBlock = uint64(pos.GetBlockNumber())
		startTxIndex = pos.GetTxIndex()
	}
	limit := effectivePageSize(req.GetTxCapacity())

	// One shared read transaction for all four tables: without it, each
	// Find*From call would independently see whatever the live indexer had
	// most recently committed, so the four reads could straddle a block
	// commit and disagree on "as of" which block/tx they cover.
	txn := s.metadata.ReadTransaction(ctx)
	defer txn.Rollback() //nolint:errcheck

	creates, err := s.metadata.FindMidnightAssetCreatesFrom(
		startBlock,
		startTxIndex,
		limit,
		txn,
	)
	if err != nil {
		return nil, s.internalError("query asset creates", err)
	}
	spends, err := s.metadata.FindMidnightAssetSpendsFrom(
		startBlock,
		startTxIndex,
		limit,
		txn,
	)
	if err != nil {
		return nil, s.internalError("query asset spends", err)
	}
	registrations, err := s.metadata.FindMidnightRegistrationsFrom(
		startBlock,
		startTxIndex,
		limit,
		txn,
	)
	if err != nil {
		return nil, s.internalError("query registrations", err)
	}
	deregistrations, err := s.metadata.FindMidnightDeregistrationsFrom(
		startBlock,
		startTxIndex,
		limit,
		txn,
	)
	if err != nil {
		return nil, s.internalError("query deregistrations", err)
	}

	items := make(
		[]utxoEventMergeItem,
		0,
		len(creates)+len(spends)+len(registrations)+len(deregistrations),
	)
	for i := range creates {
		row := &creates[i]
		items = append(items, utxoEventMergeItem{
			blockNumber: row.BlockNumber,
			txIndex:     row.TxIndex,
			kindOrder:   utxoEventKindAssetCreate,
			blockHash:   row.BlockHash,
			timestampMs: row.BlockTimestampMs,
			buildEvent: func() (*midnight.UtxoEvent, error) {
				pb, err := assetCreateToProto(row)
				if err != nil {
					return nil, err
				}
				return &midnight.UtxoEvent{
					Kind: &midnight.UtxoEvent_AssetCreate{AssetCreate: pb},
				}, nil
			},
		})
	}
	for i := range spends {
		row := &spends[i]
		items = append(items, utxoEventMergeItem{
			blockNumber: row.BlockNumber,
			txIndex:     row.TxIndex,
			kindOrder:   utxoEventKindAssetSpend,
			blockHash:   row.BlockHash,
			timestampMs: row.BlockTimestampMs,
			buildEvent: func() (*midnight.UtxoEvent, error) {
				pb, err := assetSpendToProto(row)
				if err != nil {
					return nil, err
				}
				return &midnight.UtxoEvent{
					Kind: &midnight.UtxoEvent_AssetSpend{AssetSpend: pb},
				}, nil
			},
		})
	}
	for i := range registrations {
		row := &registrations[i]
		items = append(items, utxoEventMergeItem{
			blockNumber: row.BlockNumber,
			txIndex:     row.TxIndex,
			kindOrder:   utxoEventKindRegistration,
			blockHash:   row.BlockHash,
			timestampMs: row.BlockTimestampMs,
			buildEvent: func() (*midnight.UtxoEvent, error) {
				pb, err := registrationToProto(row)
				if err != nil {
					return nil, err
				}
				return &midnight.UtxoEvent{
					Kind: &midnight.UtxoEvent_Registration{Registration: pb},
				}, nil
			},
		})
	}
	for i := range deregistrations {
		row := &deregistrations[i]
		items = append(items, utxoEventMergeItem{
			blockNumber: row.BlockNumber,
			txIndex:     row.TxIndex,
			kindOrder:   utxoEventKindDeregistration,
			blockHash:   row.BlockHash,
			timestampMs: row.BlockTimestampMs,
			buildEvent: func() (*midnight.UtxoEvent, error) {
				pb, err := deregistrationToProto(row)
				if err != nil {
					return nil, err
				}
				return &midnight.UtxoEvent{
					Kind: &midnight.UtxoEvent_Deregistration{
						Deregistration: pb,
					},
				}, nil
			},
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].blockNumber != items[j].blockNumber {
			return items[i].blockNumber < items[j].blockNumber
		}
		if items[i].txIndex != items[j].txIndex {
			return items[i].txIndex < items[j].txIndex
		}
		return items[i].kindOrder < items[j].kindOrder
	})

	if endHash := req.GetEndBlockHash(); len(endHash) > 0 {
		if s.blockNumberByHash == nil {
			return nil, status.Error(
				codes.FailedPrecondition,
				"end_block_hash requires a configured block resolver",
			)
		}
		// Resolved independently of the fetched event rows: a block with no
		// Midnight events of its own would never be found by scanning
		// items, silently letting the response run past the boundary.
		boundary, found, err := s.blockNumberByHash(endHash)
		if err != nil {
			return nil, s.internalError("resolve end_block_hash", err)
		}
		if found {
			cut := len(items)
			for i, item := range items {
				if item.blockNumber > boundary {
					cut = i
					break
				}
			}
			items = items[:cut]
		}
	}

	if limit > 0 && len(items) > limit {
		// Extend forward while the next item shares the cutoff's
		// (block_number, tx_index) key, so a page never splits a single
		// transaction's events across kinds (e.g. a create and a
		// registration written by the same tx).
		cut := limit
		for cut < len(items) &&
			items[cut].blockNumber == items[cut-1].blockNumber &&
			items[cut].txIndex == items[cut-1].txIndex {
			cut++
		}
		items = items[:cut]
	}

	events := make([]*midnight.UtxoEvent, len(items))
	for i, item := range items {
		event, err := item.buildEvent()
		if err != nil {
			return nil, s.internalError("convert utxo event", err)
		}
		events[i] = event
	}
	resp := &midnight.UtxoEventsResponse{Events: events}
	if len(items) > 0 {
		last := items[len(items)-1]
		blockNumber, err := midnightBlockNumber(last.blockNumber)
		if err != nil {
			return nil, s.internalError(
				"convert next_position block number",
				err,
			)
		}
		ts, err := midnightTimestamp(last.timestampMs)
		if err != nil {
			return nil, s.internalError("convert next_position timestamp", err)
		}
		resp.NextPosition = &midnight.CardanoPosition{
			BlockHash:                last.blockHash,
			BlockNumber:              blockNumber,
			TxIndex:                  last.txIndex,
			BlockTimestampUnixMillis: ts,
		}
	}
	return resp, nil
}

// midnightBlockNumber converts a stored block number to the Midnight
// protocol's fixed uint32 wire representation, rejecting a value that would
// silently wrap rather than reporting the corruption. Delegates to
// checkedUint32 (service.go) -- the same uint64->uint32 domain check used
// for Block/BlockByHashResponse fields -- rather than a second copy of it.
func midnightBlockNumber(value uint64) (uint32, error) {
	return checkedUint32(value)
}

// midnightTimestamp converts a stored millisecond timestamp to the Midnight
// protocol's fixed int64 wire representation, rejecting a value that would
// wrap negative rather than reporting the corruption.
func midnightTimestamp(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("timestamp %d exceeds int64 range", value)
	}
	return int64(value), nil
}

func assetCreateToProto(
	row *models.MidnightAssetCreate,
) (*midnight.AssetCreate, error) {
	ts, err := midnightTimestamp(row.BlockTimestampMs)
	if err != nil {
		return nil, err
	}
	return &midnight.AssetCreate{
		Address:                  row.Address,
		Quantity:                 row.Quantity,
		TxHash:                   row.TxHash,
		OutputIndex:              row.OutputIndex,
		BlockNumber:              row.BlockNumber,
		BlockHash:                row.BlockHash,
		TxIndex:                  row.TxIndex,
		BlockTimestampUnixMillis: ts,
	}, nil
}

func assetSpendToProto(
	row *models.MidnightAssetSpend,
) (*midnight.AssetSpend, error) {
	ts, err := midnightTimestamp(row.BlockTimestampMs)
	if err != nil {
		return nil, err
	}
	return &midnight.AssetSpend{
		Address:                  row.Address,
		Quantity:                 row.Quantity,
		SpendingTxHash:           row.SpendingTxHash,
		BlockNumber:              row.BlockNumber,
		BlockHash:                row.BlockHash,
		TxIndex:                  row.TxIndex,
		UtxoTxHash:               row.UtxoTxHash,
		UtxoIndex:                row.UtxoIndex,
		BlockTimestampUnixMillis: ts,
	}, nil
}

func registrationToProto(
	row *models.MidnightRegistration,
) (*midnight.Registration, error) {
	ts, err := midnightTimestamp(row.BlockTimestampMs)
	if err != nil {
		return nil, err
	}
	return &midnight.Registration{
		FullDatum:                row.FullDatum,
		TxHash:                   row.TxHash,
		OutputIndex:              row.OutputIndex,
		BlockNumber:              row.BlockNumber,
		BlockHash:                row.BlockHash,
		TxIndex:                  row.TxIndex,
		BlockTimestampUnixMillis: ts,
	}, nil
}

func deregistrationToProto(
	row *models.MidnightDeregistration,
) (*midnight.Deregistration, error) {
	ts, err := midnightTimestamp(row.BlockTimestampMs)
	if err != nil {
		return nil, err
	}
	return &midnight.Deregistration{
		FullDatum:                row.FullDatum,
		TxHash:                   row.TxHash,
		BlockNumber:              row.BlockNumber,
		BlockHash:                row.BlockHash,
		TxIndex:                  row.TxIndex,
		UtxoTxHash:               row.UtxoTxHash,
		UtxoIndex:                row.UtxoIndex,
		BlockTimestampUnixMillis: ts,
	}, nil
}
