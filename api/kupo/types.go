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
	"errors"
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
