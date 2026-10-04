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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDingo_BlockProduced(t *testing.T) {
	line := `{"time":"2026-01-01T00:00:01Z","msg":"block produced","slot":100,"block_hash":"abc123"}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, EventForgedBlock, ev.Type)
	require.Equal(t, uint64(100), ev.Slot)
	require.Equal(t, "abc123", ev.BlockHash)
	require.False(t, ev.Timestamp.IsZero())
}

func TestParseDingo_ChainExtended(t *testing.T) {
	line := `{"time":"2026-01-01T00:00:02Z","msg":"chain extended","slot":200,"block_hash":"def456"}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, EventChainExtended, ev.Type)
	require.Equal(t, uint64(200), ev.Slot)
	require.Equal(t, "def456", ev.BlockHash)
}

func TestParseDingo_MempoolAdd(t *testing.T) {
	// Matches the actual Dingo mempool log format:
	//   m.logger.Debug("added transaction", "component", "mempool", ...)
	line := `{"time":"2026-01-01T00:00:03Z","msg":"added transaction","component":"mempool","tx_hash":"abc123"}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, EventMempoolAdd, ev.Type)
}

func TestParseDingo_ConfirmedTransaction(t *testing.T) {
	line := `{"msg":"confirmed transaction","component":"mempool","tx_hash":"abc123"}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, EventTxConfirmed, ev.Type)
	require.Equal(t, "abc123", ev.TxID)
}

func TestParseTxpump_SubmissionIncludesID(t *testing.T) {
	line := `{"ts":"2026-01-01T00:00:01Z","tx_id":"abc123","tx_type":"payment","status":"submitted"}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, EventTxSubmitted, ev.Type)
	require.Equal(t, "abc123", ev.TxID)
}

func TestParseDingo_MempoolMissingComponent(t *testing.T) {
	// "added transaction" without "component":"mempool" should NOT match
	line := `{"time":"2026-01-01T00:00:04Z","msg":"added transaction","slot":400}`
	ev := ParseLogLine(line)
	require.Nil(
		t,
		ev,
		"added transaction without component=mempool should not match",
	)
}

func TestParseDingo_Unknown(t *testing.T) {
	line := `{"time":"2026-01-01T00:00:05Z","msg":"some unrecognised message"}`
	ev := ParseLogLine(line)
	require.Nil(t, ev)
}

func TestParseCardanoNode_ForgedBlock(t *testing.T) {
	line := `{"ns":"Cardano.Node.ForgedBlock","at":"2026-01-01T00:00:06Z","data":{"slot":500,"headerHash":"aabbcc"}}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, EventForgedBlock, ev.Type)
	require.Equal(t, "aabbcc", ev.BlockHash)
}

func TestParseCardanoNode_AddedToCurrentChain(t *testing.T) {
	line := `{"ns":"Cardano.ChainSync.AddedToCurrentChain","at":"2026-01-01T00:00:07Z","data":{"slot":600}}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, EventChainExtended, ev.Type)
	require.Equal(t, uint64(600), ev.Slot)
}

func TestParseCardanoNode_CompletedBlockFetch(t *testing.T) {
	line := `{"ns":"Cardano.BlockFetch.CompletedBlockFetch","at":"2026-01-01T00:00:08Z"}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, EventBlockReceived, ev.Type)
}

func TestParseCardanoNode_MempoolAddedTx(t *testing.T) {
	line := `{"ns":"Cardano.Mempool.AddedTx","at":"2026-01-01T00:00:09Z"}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, EventMempoolAdd, ev.Type)
}

func TestParseCardanoNode_Unknown(t *testing.T) {
	line := `{"ns":"Cardano.SomeOtherEvent","at":"2026-01-01T00:00:10Z"}`
	ev := ParseLogLine(line)
	require.Nil(t, ev)
}

func TestParseLine_NonJSON(t *testing.T) {
	ev := ParseLogLine("this is not json at all")
	require.Nil(t, ev)
}

func TestParseLine_EmptyLine(t *testing.T) {
	ev := ParseLogLine("")
	require.Nil(t, ev)
}

func TestParseLine_WhitespaceLine(t *testing.T) {
	ev := ParseLogLine("   \t  ")
	require.Nil(t, ev)
}

func TestParseLine_JSONNoKnownKey(t *testing.T) {
	// Valid JSON but no "msg" or "ns" key — should return nil
	ev := ParseLogLine(`{"level":"info","text":"hello"}`)
	require.Nil(t, ev)
}

func TestParseLine_MalformedJSON(t *testing.T) {
	ev := ParseLogLine(`{"msg": "block produced", "slot": }`)
	require.Nil(t, ev)
}

func TestParseLine_SlotAsFloat(t *testing.T) {
	// JSON numbers without decimals are decoded as float64
	line := `{"msg":"block produced","slot":9999,"block_hash":"ff"}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.Equal(t, uint64(9999), ev.Slot)
}

func TestParseLine_MissingTimestamp(t *testing.T) {
	line := `{"msg":"chain extended","slot":1}`
	ev := ParseLogLine(line)
	require.NotNil(t, ev)
	require.True(t, ev.Timestamp.IsZero())
}

// A JSON number that is not an exactly representable non-negative integer must
// not become a slot: 0 means "no slot" to every consumer, which is the only
// safe reading of a malformed value.
func TestExtractSlot_RejectsMalformedNumbers(t *testing.T) {
	t.Parallel()
	const maxSafe = float64(1<<53 - 1)
	cases := []struct {
		name string
		slot interface{}
		want uint64
	}{
		{"integral", float64(42), 42},
		{"zero", float64(0), 0},
		{"max safe integer", maxSafe, 1<<53 - 1},
		{"above max safe integer", maxSafe + 2, 0},
		{"far out of range", float64(1e30), 0},
		{"fractional", 1.5, 0},
		{"negative", float64(-3), 0},
		{"negative fractional", -0.5, 0},
		{"NaN", math.NaN(), 0},
		{"positive infinity", math.Inf(1), 0},
		{"negative infinity", math.Inf(-1), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t,
				tc.want,
				extractSlot(map[string]interface{}{"slot": tc.slot}),
			)
		})
	}
}

func TestParseLine_MalformedSlotJSON(t *testing.T) {
	t.Parallel()
	for name, slot := range map[string]string{
		"fractional":    `1.5`,
		"negative":      `-7`,
		"exponent past": `1e30`,
		"imprecise":     `9007199254740993`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ev := ParseLogLine(
				`{"msg":"block produced","slot":` + slot + `,"block_hash":"abc"}`,
			)
			require.NotNil(t, ev)
			require.Zero(t, ev.Slot)
		})
	}
}

// Only an explicit node rejection of a delegation is a workload failure: a
// transport error carries status "error" and must not be reported as one.
func TestParseLine_TxpumpRejection(t *testing.T) {
	t.Parallel()
	ev := ParseLogLine(
		`{"tx_id":"ab","tx_type":"delegation","status":"rejected"}`,
	)
	require.NotNil(t, ev)
	require.Equal(t, EventTxRejected, ev.Type)
	require.Equal(t, "delegation", ev.TxType)

	require.Nil(t, ParseLogLine(
		`{"tx_id":"ab","tx_type":"delegation","status":"error"}`,
	))
}
