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

// Package format defines the on-disk JSON test-vector format used by the
// consensus-conformance harness. A vector has one of two categories:
//
//   - "consensus": captured per-peer wire traces plus the canonical
//     downstream chainsync trace and structured chain tip produced by
//     running them through cardano-node as the oracle.
//   - "ledger":    converted from the Amaru ledger corpus, carrying
//     opaque CBOR config + initial/final NewEpochState blobs and an
//     ordered event stream.
//
// Binary fields (header bytes, msg bytes, state blobs, hashes) are
// hex-encoded into JSON strings so the on-disk shape stays diffable.
package format

// CurrentSchemaVersion is the schema version this package emits. Bump on
// breaking changes (new required fields, removed fields, semantics
// changes). Decoder rejects unknown versions explicitly.
const CurrentSchemaVersion = 1

// Category discriminates between the two top-level vector shapes.
type Category string

const (
	CategoryConsensus Category = "consensus"
	CategoryLedger    Category = "ledger"
)

// Protocol names the Ouroboros mini-protocol a captured message belongs
// to. Strings (not numeric IDs) keep the JSON self-documenting.
type Protocol string

const (
	ProtocolChainSync  Protocol = "chainsync"
	ProtocolBlockFetch Protocol = "blockfetch"
)

// ChainSync message-type names. The format package maps these to and
// from gouroboros's numeric message-type IDs at encode/decode time.
const (
	ChainSyncMsgRequestNext       = "request_next"
	ChainSyncMsgAwaitReply        = "await_reply"
	ChainSyncMsgRollForward       = "roll_forward"
	ChainSyncMsgRollBackward      = "roll_backward"
	ChainSyncMsgFindIntersect     = "find_intersect"
	ChainSyncMsgIntersectFound    = "intersect_found"
	ChainSyncMsgIntersectNotFound = "intersect_not_found"
	ChainSyncMsgDone              = "done"
)

// BlockFetch message-type names.
const (
	BlockFetchMsgRequestRange = "request_range"
	BlockFetchMsgClientDone   = "client_done"
	BlockFetchMsgStartBatch   = "start_batch"
	BlockFetchMsgNoBlocks     = "no_blocks"
	BlockFetchMsgBlock        = "block"
	BlockFetchMsgBatchDone    = "batch_done"
)

// LedgerEventType names the variants of LedgerEvent. Mirrors the
// Amaru positional 5-tuple discriminant (0..3) one-to-one.
type LedgerEventType string

const (
	LedgerEventTransaction LedgerEventType = "transaction"
	LedgerEventPassTick    LedgerEventType = "pass_tick"
	LedgerEventPassEpoch   LedgerEventType = "pass_epoch"
	LedgerEventRollback    LedgerEventType = "rollback"
)

// TestVector is the top-level envelope. Exactly one of Capture or
// LedgerPhase is set, matching Category.
type TestVector struct {
	SchemaVersion int      `json:"schema_version"`
	Title         string   `json:"title"`
	Category      Category `json:"category"`

	// Capture is set when Category == CategoryConsensus.
	Capture *ConsensusCapture `json:"capture,omitempty"`
	// LedgerPhase is set when Category == CategoryLedger.
	LedgerPhase *LedgerPhase `json:"ledger_phase,omitempty"`
}

// ConsensusCapture holds the inputs and oracle outputs for a consensus
// scenario. Per-peer served traces are the inputs; ExpectedOutput is
// what the SUT is asserted to produce after stabilizing.
type ConsensusCapture struct {
	Peers          []PeerInput    `json:"peers"`
	ExpectedOutput ExpectedOutput `json:"expected_output"`
}

// PeerInput is the trace cardano-node served on one upstream
// connection. Order matches arrival order.
type PeerInput struct {
	PeerID uint64          `json:"peer_id"`
	Served []ServedMessage `json:"served"`
}

// ServedMessage is one inbound protocol-message body, decomposed into
// named fields based on Protocol + MsgType. The opaque blocks of bytes
// (block headers, block bodies) stay hex-encoded because they carry
// cryptographic content (VRF proofs, KES signatures, transaction
// signatures) that is meaningful only as-is. Everything else (slot,
// hash, era, tip, points) is first-class JSON so vectors stay
// diff-readable and the format does not depend on gouroboros's wire
// re-encoding being byte-for-byte stable.
//
// The full schema below covers chainsync + blockfetch, but the
// current capture-sidecar only populates roll_forward and
// roll_backward via the two callbacks registered in Sidecar.Connect.
// The other msg_types are recognized by the decoder so a future
// scenario that records them (or a hand-crafted fixture) parses
// cleanly without a format bump. A capture's served trace today
// will therefore contain at most {roll_forward, roll_backward} —
// not the full chainsync conversation.
//
// Field population per msg_type:
//
//	chainsync/request_next        — no fields
//	chainsync/await_reply         — no fields
//	chainsync/roll_forward (NtN)  — Era + HeaderCbor + Tip
//	chainsync/roll_backward       — Point + Tip
//	chainsync/find_intersect      — Points
//	chainsync/intersect_found     — Point + Tip
//	chainsync/intersect_not_found — Tip
//	chainsync/done                — no fields
//	blockfetch/request_range      — Start + End
//	blockfetch/client_done        — no fields
//	blockfetch/start_batch        — no fields
//	blockfetch/no_blocks          — no fields
//	blockfetch/block              — BlockCbor
//	blockfetch/batch_done         — no fields
//
// Setting a field outside its msg_type's allowed set is a validation
// error; the decoder rejects vectors that do so.
type ServedMessage struct {
	Protocol Protocol `json:"protocol"`
	MsgType  string   `json:"msg_type"`

	// roll_forward (NtN)
	Era        *uint    `json:"era,omitempty"`
	HeaderCbor HexBytes `json:"header_cbor,omitempty"`

	// roll_forward / roll_backward / intersect_found / intersect_not_found
	Tip *Tip `json:"tip,omitempty"`

	// roll_backward / intersect_found
	Point *Point `json:"point,omitempty"`

	// find_intersect
	Points []Point `json:"points,omitempty"`

	// blockfetch request_range
	Start *Point `json:"start,omitempty"`
	End   *Point `json:"end,omitempty"`

	// blockfetch block
	BlockCbor HexBytes `json:"block_cbor,omitempty"`
}

// Point is a chain location: slot + block hash. Hash hex-encodes
// during JSON marshal so the on-disk shape stays diffable.
type Point struct {
	Slot uint64   `json:"slot"`
	Hash HexBytes `json:"hash"`
}

// ExpectedOutput holds the canonical wire-level chainsync trace a
// downstream client would observe from the SUT after stabilization
// (DownstreamChainSync), plus a coarse structured tip used as a
// fast-fail sanity check.
type ExpectedOutput struct {
	DownstreamChainSync []ServedMessage `json:"downstream_chainsync"`
	FinalTip            Tip             `json:"final_tip"`
}

// Tip is a structured chain tip suitable for fast equality checks. Hash
// is hex-encoded into a JSON string at marshal time. BlockNumber is
// the chain-length count cardano-node reports alongside the tip and
// is what Praos compares on for chain-selection — never derive it
// from the served-trace length when feeding into chain selection,
// because chainsync starting from a non-origin intersect would
// undercount.
type Tip struct {
	Slot        uint64   `json:"slot"`
	Hash        HexBytes `json:"hash"`
	BlockNumber uint64   `json:"block_number"`
}

// LedgerPhase is the payload for ledger-category vectors. Mirrors the
// Amaru-supplied corpus shape so cmd/convert-amaru-vector's rewrap
// of the existing per-vector content stays mechanical.
type LedgerPhase struct {
	Config       HexBytes      `json:"config"`
	InitialState HexBytes      `json:"initial_state"`
	FinalState   HexBytes      `json:"final_state"`
	Events       []LedgerEvent `json:"events"`
}

// LedgerEvent is a discriminated-union event in the converted ledger
// stream. Fields populated depend on Type:
//
//   - transaction: TxCbor, Success, Slot
//   - pass_tick:   Slot
//   - pass_epoch:  Epoch
//   - rollback:    TargetSlot
type LedgerEvent struct {
	Type LedgerEventType `json:"type"`

	TxCbor  HexBytes `json:"tx_cbor,omitempty"`
	Success *bool    `json:"success,omitempty"`
	Slot    *uint64  `json:"slot,omitempty"`

	Epoch *uint64 `json:"epoch,omitempty"`

	TargetSlot *uint64 `json:"target_slot,omitempty"`
}
