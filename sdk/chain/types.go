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

package chain

// ChainID names a chain. Roles bound to different chains never exchange
// payloads, so a node can tell which implementation set a payload belongs to.
type ChainID string

// Point identifies a position on a chain by slot and block hash. The zero
// Point, with a nil Hash, is the chain origin.
type Point struct {
	Slot uint64
	Hash []byte
}

// Tip is a Point together with the block number found there.
type Tip struct {
	Point       Point
	BlockNumber uint64
}

// Comparison is the outcome of comparing two chain tips. The values match the
// result Cardano chain selection already reports, so an adapter converts by
// value.
type Comparison int

const (
	// ComparisonBBetter reports that the second tip is preferred.
	ComparisonBBetter Comparison = -1
	// ComparisonEqual reports that neither tip is preferred.
	ComparisonEqual Comparison = 0
	// ComparisonABetter reports that the first tip is preferred.
	ComparisonABetter Comparison = 1
	// ComparisonUnknown reports that the tips could not be compared, for
	// example because the data a tiebreak needs is missing.
	ComparisonUnknown Comparison = 2
)
