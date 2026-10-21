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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrInvalidRequest = errors.New("invalid request")
	ErrNotFound       = errors.New("not found")
)

type Point struct {
	SlotNo     uint64 `json:"slot_no"`
	HeaderHash string `json:"header_hash"`
}

type PointSelector struct {
	SlotNo     uint64
	HeaderHash string
}

type MatchStatus uint8

const (
	MatchStatusAny MatchStatus = iota
	MatchStatusSpent
	MatchStatusUnspent
)

type MatchQuery struct {
	Pattern       string
	ResolveHashes bool
	Status        MatchStatus
	OldestFirst   bool
	CreatedAfter  *PointSelector
	CreatedBefore *PointSelector
	SpentAfter    *PointSelector
	SpentBefore   *PointSelector
	PolicyID      []byte
	AssetName     []byte
	TransactionID []byte
	OutputIndex   *uint32
	// Cursor resumes strictly after a position a previous page reported in
	// X-Kupo-Next-Cursor. It is only meaningful with the same order and
	// filters as that page.
	Cursor *MatchCursor
	// MaxScan bounds how many stored rows one iterator examines. Zero means
	// unbounded.
	MaxScan int
	// Limit is the page size a client asked for with the limit parameter.
	// Zero means the client did not opt into pagination.
	Limit int
}

// MatchCursor is a position in a match result, in the stable
// (slot, transaction index, output index, transaction id) ordering shared by
// both result orders.
type MatchCursor struct {
	Slot             uint64
	TransactionIndex uint32
	OutputIndex      uint32
	TransactionID    []byte
}

// String renders the cursor as slot:transaction_index:output_index:tx_id.
func (c MatchCursor) String() string {
	return fmt.Sprintf(
		"%d:%d:%d:%s",
		c.Slot,
		c.TransactionIndex,
		c.OutputIndex,
		hex.EncodeToString(c.TransactionID),
	)
}

// ParseMatchCursor parses the form produced by MatchCursor.String.
func ParseMatchCursor(value string) (*MatchCursor, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 4 {
		return nil, fmt.Errorf("%w: invalid cursor", ErrInvalidRequest)
	}
	slot, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid cursor", ErrInvalidRequest)
	}
	txIndex, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid cursor", ErrInvalidRequest)
	}
	outputIndex, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid cursor", ErrInvalidRequest)
	}
	txID, err := hex.DecodeString(parts[3])
	if err != nil || len(txID) != 32 {
		return nil, fmt.Errorf("%w: invalid cursor", ErrInvalidRequest)
	}
	return &MatchCursor{
		Slot:             slot,
		TransactionIndex: uint32(txIndex),
		OutputIndex:      uint32(outputIndex),
		TransactionID:    txID,
	}, nil
}

type Value struct {
	Coins  uint64
	Assets map[string]uint64
}

type SpentPoint struct {
	SlotNo        uint64  `json:"slot_no"`
	HeaderHash    string  `json:"header_hash"`
	TransactionID *string `json:"transaction_id"`
	InputIndex    *uint32 `json:"input_index"`
	Redeemer      *string `json:"redeemer"`
}

type Match struct {
	TransactionIndex uint32
	TransactionID    string
	OutputIndex      uint32
	Address          string
	Value            Value
	DatumHash        *string
	DatumType        string
	ScriptHash       *string
	CreatedAt        Point
	SpentAt          *SpentPoint
	Datum            *string
	Script           *Script
}

type Datum struct {
	Datum string `json:"datum"`
}

type Script struct {
	Language string `json:"language"`
	Script   string `json:"script"`
}

type Metadata struct {
	Hash   string         `json:"hash"`
	Raw    string         `json:"raw"`
	Schema map[string]any `json:"schema"`
}

type Health struct {
	ConnectionStatus       string   `json:"connection_status"`
	MostRecentCheckpoint   *uint64  `json:"most_recent_checkpoint"`
	MostRecentNodeTip      *uint64  `json:"most_recent_node_tip"`
	SecondsSinceLastBlock  *uint64  `json:"seconds_since_last_block"`
	NetworkSynchronization *float64 `json:"network_synchronization"`
	Configuration          struct {
		Indexes string `json:"indexes"`
	} `json:"configuration"`
	Version string `json:"version"`
}

// MatchIterator streams one snapshot-consistent match query. Close must be
// called even when iteration stops early so the coordinated read transaction
// is released promptly.
type MatchIterator interface {
	Tip() Point
	Next() (Match, bool, error)
	// Resume reports the position after the last row Next examined and
	// whether iteration stopped on the scan budget before exhausting the
	// result. The cursor is nil until a row has been examined.
	Resume() (*MatchCursor, bool)
	Close()
}

type KupoNode interface {
	Tip() (Point, error)
	Matches(context.Context, MatchQuery) (MatchIterator, error)
	Datum(context.Context, []byte) (*Datum, Point, error)
	Script(context.Context, []byte) (*Script, Point, error)
	Checkpoints(context.Context) ([]Point, Point, error)
	Checkpoint(context.Context, uint64, bool) (*Point, Point, error)
	Metadata(
		context.Context,
		uint64,
		[]byte,
	) ([]Metadata, string, Point, error)
	Health() (Health, Point, int, error)
}
