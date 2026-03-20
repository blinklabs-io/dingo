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

package analysis

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// EventType classifies what happened in a parsed log line.
type EventType int

const (
	// EventUnknown is returned for log lines that do not match any known
	// pattern and should be ignored.
	EventUnknown EventType = iota

	// EventForgedBlock indicates this node successfully produced a block.
	EventForgedBlock

	// EventChainExtended indicates the node's local chain was extended with
	// a new block (produced by any pool).
	EventChainExtended

	// EventBlockReceived indicates the node received a block from a peer.
	EventBlockReceived

	// EventMempoolAdd indicates a transaction was added to the mempool.
	EventMempoolAdd

	// EventTxSubmitted indicates txpump successfully submitted a transaction.
	// The TxType field on BlockEvent identifies the tx type (payment,
	// delegation, governance, plutus).
	EventTxSubmitted
)

// BlockEvent is the normalised representation of a single log line.
type BlockEvent struct {
	// Type is the classification of the event.
	Type EventType

	// Timestamp is the wall-clock time extracted from the log line, or the
	// zero value if not present.
	Timestamp time.Time

	// Slot is the blockchain slot number associated with this event.
	// Zero means the slot could not be extracted.
	Slot uint64

	// BlockHash is the hex-encoded hash of the block, or empty if not
	// present.
	BlockHash string

	// NodeID is an opaque identifier for the originating node derived from
	// the log file name (set by the caller, not the parser).
	NodeID string

	// TxType is the transaction type for EventTxSubmitted events
	// (e.g. "payment", "delegation", "governance", "plutus").
	TxType string
}

// ParseLogLine attempts to parse a single JSON log line and return a
// BlockEvent. It returns nil for lines that cannot be parsed or that do not
// match any recognised event type.
//
// Two log formats are supported:
//
//   - dingo:         slog JSON with a "msg" key
//   - cardano-node:  trace-dispatcher JSON with a "ns" key
func ParseLogLine(line string) *BlockEvent {
	line = strings.TrimSpace(line)
	if len(line) == 0 || line[0] != '{' {
		return nil
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return nil
	}

	if _, hasMsgKey := raw["msg"]; hasMsgKey {
		return parseDingoLine(raw)
	}
	if _, hasNsKey := raw["ns"]; hasNsKey {
		return parseCardanoNodeLine(raw)
	}
	// txpump log format: {"ts":"...","tx_id":"...","tx_type":"...","status":"..."}
	if _, hasTxType := raw["tx_type"]; hasTxType {
		return parseTxpumpLine(raw)
	}
	return nil
}

// parseDingoLine handles dingo slog JSON format.
func parseDingoLine(raw map[string]interface{}) *BlockEvent {
	msg, _ := raw["msg"].(string)
	msg = strings.ToLower(msg)

	var evType EventType
	switch {
	case strings.HasPrefix(msg, "block produced"):
		evType = EventForgedBlock
	case strings.HasPrefix(msg, "chain extended"):
		evType = EventChainExtended
	case msg == "added transaction" && componentIs(raw, "mempool"):
		evType = EventMempoolAdd
	default:
		return nil
	}

	ev := &BlockEvent{Type: evType}
	ev.Timestamp = extractTimestamp(raw)
	ev.Slot = extractSlot(raw)
	// For "chain extended" messages the slot may be embedded in the message
	// string as "chain extended, new tip: <hash> at slot <N>".
	if ev.Slot == 0 && evType == EventChainExtended {
		ev.Slot = extractSlotFromMsg(msg)
	}
	ev.BlockHash = extractHash(raw)
	if ev.BlockHash == "" && evType == EventChainExtended {
		ev.BlockHash = extractHashFromMsg(msg)
	}
	return ev
}

// parseCardanoNodeLine handles cardano-node trace-dispatcher JSON format.
func parseCardanoNodeLine(raw map[string]interface{}) *BlockEvent {
	ns, _ := raw["ns"].(string)

	var evType EventType
	switch {
	case strings.Contains(ns, "ForgedBlock"):
		evType = EventForgedBlock
	case strings.Contains(ns, "AddedToCurrentChain"):
		evType = EventChainExtended
	case strings.Contains(ns, "CompletedBlockFetch"):
		evType = EventBlockReceived
	case strings.Contains(ns, "Mempool") && strings.Contains(ns, "AddedTx"):
		evType = EventMempoolAdd
	default:
		return nil
	}

	ev := &BlockEvent{Type: evType}
	ev.Timestamp = extractTimestamp(raw)
	ev.Slot = extractSlotCardano(raw)
	ev.BlockHash = extractHash(raw)
	return ev
}

// extractTimestamp tries common timestamp keys in the JSON object.
func extractTimestamp(raw map[string]interface{}) time.Time {
	for _, key := range []string{"time", "ts", "at", "timestamp"} {
		if v, ok := raw[key].(string); ok {
			for _, layout := range []string{
				time.RFC3339Nano,
				time.RFC3339,
				"2006-01-02T15:04:05.999999999Z",
			} {
				if t, err := time.Parse(layout, v); err == nil {
					return t
				}
			}
		}
	}
	return time.Time{}
}

// extractSlot pulls the slot number from dingo-style log fields.
func extractSlot(raw map[string]interface{}) uint64 {
	for _, key := range []string{"slot", "slot_no", "slotNo"} {
		switch v := raw[key].(type) {
		case float64:
			return uint64(v)
		case string:
			if n, err := strconv.ParseUint(v, 10, 64); err == nil {
				return n
			}
		}
	}
	return 0
}

// extractSlotFromMsg parses the slot number embedded in a dingo log message
// of the form "chain extended, new tip: <hash> at slot <N>".
// Returns 0 if the pattern is not found or the number cannot be parsed.
func extractSlotFromMsg(msg string) uint64 {
	const marker = " at slot "
	idx := strings.LastIndex(msg, marker)
	if idx < 0 {
		return 0
	}
	rest := strings.TrimSpace(msg[idx+len(marker):])
	// The slot number may be followed by other text; take only the leading digits.
	end := strings.IndexFunc(rest, func(r rune) bool {
		return r < '0' || r > '9'
	})
	if end >= 0 {
		rest = rest[:end]
	}
	n, err := strconv.ParseUint(rest, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// extractHashFromMsg parses the block hash embedded in a dingo log message
// of the form "chain extended, new tip: <hex> at slot <N>".
// Returns an empty string if the pattern is not found.
func extractHashFromMsg(msg string) string {
	const prefix = "new tip: "
	idx := strings.Index(msg, prefix)
	if idx < 0 {
		return ""
	}
	rest := msg[idx+len(prefix):]
	end := strings.IndexByte(rest, ' ')
	if end < 0 {
		return rest
	}
	return rest[:end]
}

// extractSlotCardano pulls slot from a nested cardano-node data object.
func extractSlotCardano(raw map[string]interface{}) uint64 {
	// Try top-level first
	if s := extractSlot(raw); s != 0 {
		return s
	}
	// cardano-node nests event data under "data" key
	if data, ok := raw["data"].(map[string]interface{}); ok {
		return extractSlot(data)
	}
	return 0
}

// componentIs checks whether the "component" field in the raw JSON object
// matches the expected value (case-insensitive).
func componentIs(raw map[string]interface{}, expected string) bool {
	v, ok := raw["component"].(string)
	return ok && strings.EqualFold(v, expected)
}

// parseTxpumpLine handles txpump JSON log format.
// Format: {"ts":"...","tx_id":"...","tx_type":"payment","status":"submitted",...}
func parseTxpumpLine(raw map[string]interface{}) *BlockEvent {
	status, _ := raw["status"].(string)
	if status != "submitted" {
		return nil // only count successful submissions
	}
	txType, _ := raw["tx_type"].(string)
	if txType == "" {
		return nil
	}

	ev := &BlockEvent{
		Type:   EventTxSubmitted,
		TxType: txType,
	}
	if ts, ok := raw["ts"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			ev.Timestamp = t
		}
	}
	return ev
}

// hashKeys are the JSON field names that may contain a block hash.
var hashKeys = []string{"block_hash", "blockHash", "hash", "headerHash"}

// extractHash pulls the block hash from the log object.
func extractHash(raw map[string]interface{}) string {
	for _, key := range hashKeys {
		if v, ok := raw[key].(string); ok && v != "" {
			return v
		}
	}
	// cardano-node may nest under "data"
	if data, ok := raw["data"].(map[string]interface{}); ok {
		for _, key := range hashKeys {
			if v, ok := data[key].(string); ok && v != "" {
				return v
			}
		}
	}
	return ""
}
