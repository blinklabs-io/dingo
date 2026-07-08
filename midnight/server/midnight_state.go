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
	"bytes"
	"context"
	"sort"

	"github.com/blinklabs-io/dingo/database/models"
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

// GetAssetCreates returns cNIGHT UTxO creations starting strictly after
// (start_block, start_tx_index), ordered by (block_number, tx_index)
// ascending and capped at utxo_capacity rows.
func (s *service) GetAssetCreates(
	_ context.Context,
	req *midnight.AssetCreatesRequest,
) (*midnight.AssetCreatesResponse, error) {
	if s.metadata == nil {
		return nil, status.Error(codes.Unimplemented, "method GetAssetCreates not implemented")
	}
	rows, err := s.metadata.FindMidnightAssetCreatesFrom(
		uint64(req.GetStartBlock()),
		req.GetStartTxIndex(),
		int(req.GetUtxoCapacity()),
		nil,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query asset creates: %v", err)
	}
	creates := make([]*midnight.AssetCreate, len(rows))
	for i := range rows {
		creates[i] = assetCreateToProto(&rows[i])
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
		return nil, status.Error(codes.Unimplemented, "method GetAssetSpends not implemented")
	}
	rows, err := s.metadata.FindMidnightAssetSpendsFrom(
		uint64(req.GetStartBlock()),
		req.GetStartTxIndex(),
		int(req.GetUtxoCapacity()),
		nil,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query asset spends: %v", err)
	}
	spends := make([]*midnight.AssetSpend, len(rows))
	for i := range rows {
		spends[i] = assetSpendToProto(&rows[i])
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
		return nil, status.Error(codes.Unimplemented, "method GetRegistrations not implemented")
	}
	rows, err := s.metadata.FindMidnightRegistrationsFrom(
		uint64(req.GetStartBlock()),
		req.GetStartTxIndex(),
		int(req.GetUtxoCapacity()),
		nil,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query registrations: %v", err)
	}
	registrations := make([]*midnight.Registration, len(rows))
	for i := range rows {
		registrations[i] = registrationToProto(&rows[i])
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
		return nil, status.Error(codes.Unimplemented, "method GetDeregistrations not implemented")
	}
	rows, err := s.metadata.FindMidnightDeregistrationsFrom(
		uint64(req.GetStartBlock()),
		req.GetStartTxIndex(),
		int(req.GetUtxoCapacity()),
		nil,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query deregistrations: %v", err)
	}
	deregistrations := make([]*midnight.Deregistration, len(rows))
	for i := range rows {
		deregistrations[i] = deregistrationToProto(&rows[i])
	}
	return &midnight.DeregistrationsResponse{Deregistrations: deregistrations}, nil
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
	event       *midnight.UtxoEvent
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
	_ context.Context,
	req *midnight.UtxoEventsRequest,
) (*midnight.UtxoEventsResponse, error) {
	if s.metadata == nil {
		return nil, status.Error(codes.Unimplemented, "method GetUtxoEvents not implemented")
	}
	var startBlock uint64
	var startTxIndex uint32
	if pos := req.GetStartPosition(); pos != nil {
		startBlock = uint64(pos.GetBlockNumber())
		startTxIndex = pos.GetTxIndex()
	}
	limit := int(req.GetTxCapacity())

	creates, err := s.metadata.FindMidnightAssetCreatesFrom(startBlock, startTxIndex, limit, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query asset creates: %v", err)
	}
	spends, err := s.metadata.FindMidnightAssetSpendsFrom(startBlock, startTxIndex, limit, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query asset spends: %v", err)
	}
	registrations, err := s.metadata.FindMidnightRegistrationsFrom(startBlock, startTxIndex, limit, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query registrations: %v", err)
	}
	deregistrations, err := s.metadata.FindMidnightDeregistrationsFrom(startBlock, startTxIndex, limit, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query deregistrations: %v", err)
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
			event: &midnight.UtxoEvent{
				Kind: &midnight.UtxoEvent_AssetCreate{AssetCreate: assetCreateToProto(row)},
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
			event: &midnight.UtxoEvent{
				Kind: &midnight.UtxoEvent_AssetSpend{AssetSpend: assetSpendToProto(row)},
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
			event: &midnight.UtxoEvent{
				Kind: &midnight.UtxoEvent_Registration{Registration: registrationToProto(row)},
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
			event: &midnight.UtxoEvent{
				Kind: &midnight.UtxoEvent_Deregistration{Deregistration: deregistrationToProto(row)},
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
		boundary, found := uint64(0), false
		for _, item := range items {
			if bytes.Equal(item.blockHash, endHash) {
				boundary, found = item.blockNumber, true
				break
			}
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
		items = items[:limit]
	}

	events := make([]*midnight.UtxoEvent, len(items))
	for i, item := range items {
		events[i] = item.event
	}
	resp := &midnight.UtxoEventsResponse{Events: events}
	if len(items) > 0 {
		last := items[len(items)-1]
		resp.NextPosition = &midnight.CardanoPosition{
			BlockHash:                last.blockHash,
			BlockNumber:              uint32(last.blockNumber), //nolint:gosec // wire format is uint32
			TxIndex:                  last.txIndex,
			BlockTimestampUnixMillis: int64(last.timestampMs), //nolint:gosec // wire format is int64
		}
	}
	return resp, nil
}

func assetCreateToProto(row *models.MidnightAssetCreate) *midnight.AssetCreate {
	return &midnight.AssetCreate{
		Address:                  row.Address,
		Quantity:                 row.Quantity,
		TxHash:                   row.TxHash,
		OutputIndex:              row.OutputIndex,
		BlockNumber:              row.BlockNumber,
		BlockHash:                row.BlockHash,
		TxIndex:                  row.TxIndex,
		BlockTimestampUnixMillis: int64(row.BlockTimestampMs), //nolint:gosec // wire format is int64
	}
}

func assetSpendToProto(row *models.MidnightAssetSpend) *midnight.AssetSpend {
	return &midnight.AssetSpend{
		Address:                  row.Address,
		Quantity:                 row.Quantity,
		SpendingTxHash:           row.SpendingTxHash,
		BlockNumber:              row.BlockNumber,
		BlockHash:                row.BlockHash,
		TxIndex:                  row.TxIndex,
		UtxoTxHash:               row.UtxoTxHash,
		UtxoIndex:                row.UtxoIndex,
		BlockTimestampUnixMillis: int64(row.BlockTimestampMs), //nolint:gosec // wire format is int64
	}
}

func registrationToProto(row *models.MidnightRegistration) *midnight.Registration {
	return &midnight.Registration{
		FullDatum:                row.FullDatum,
		TxHash:                   row.TxHash,
		OutputIndex:              row.OutputIndex,
		BlockNumber:              row.BlockNumber,
		BlockHash:                row.BlockHash,
		TxIndex:                  row.TxIndex,
		BlockTimestampUnixMillis: int64(row.BlockTimestampMs), //nolint:gosec // wire format is int64
	}
}

func deregistrationToProto(row *models.MidnightDeregistration) *midnight.Deregistration {
	return &midnight.Deregistration{
		FullDatum:                row.FullDatum,
		TxHash:                   row.TxHash,
		BlockNumber:              row.BlockNumber,
		BlockHash:                row.BlockHash,
		TxIndex:                  row.TxIndex,
		UtxoTxHash:               row.UtxoTxHash,
		UtxoIndex:                row.UtxoIndex,
		BlockTimestampUnixMillis: int64(row.BlockTimestampMs), //nolint:gosec // wire format is int64
	}
}
