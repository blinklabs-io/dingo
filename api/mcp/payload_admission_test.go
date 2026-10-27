// Copyright 2025 Blink Labs Software
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

package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// admissionValidTxHex is a well-formed transaction, so a request carrying it
// reaches the evaluator.
const admissionValidTxHex = "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1" +
	"e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ce" +
	"b93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e391" +
	"4f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a" +
	"9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172" +
	"e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe17" +
	"9af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebee" +
	"f022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a40081825820" +
	"25fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409d" +
	"a928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c795" +
	"1c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581" +
	"840000187b820a0a06814746010000222601f4f6"

// admissionValidOutputHex is a well-formed ADA-only Babbage output.
const admissionValidOutputHex = "82581d60" +
	"00000000000000000000000000000000000000000000000000000000" +
	"1a000f4240"

// blockingEvaluator holds every evaluation open until release is closed, and
// counts the evaluations it was asked to run.
type blockingEvaluator struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func newBlockingEvaluator() *blockingEvaluator {
	return &blockingEvaluator{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
}

func (e *blockingEvaluator) EvaluateTxContext(
	ctx context.Context,
	_ lcommon.Transaction,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	e.calls.Add(1)
	e.entered <- struct{}{}
	select {
	case <-e.release:
	case <-ctx.Done():
	}
	return 0, lcommon.ExUnits{}, nil, ctx.Err()
}

func evaluateTxSession(
	t *testing.T,
	evaluator txEvaluator,
) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1"},
		nil,
	)
	registerEvaluateTxTool(server, evaluator, 5*time.Second)
	return reviewSession(t, server)
}

func minUtxoSession(t *testing.T, limits *readLimits) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(
		&mcp.Implementation{Name: "test", Version: "1"},
		nil,
	)
	registerCardanoTools(
		server, nil, nil, nil, "preview", 5*time.Second, limits,
	)
	return reviewSession(t, server)
}

func TestCheckEncodedPayloadSizeBoundary(t *testing.T) {
	t.Parallel()
	limit := hex.EncodedLen(maxCardanoPayloadBytes)
	require.NoError(
		t,
		checkEncodedPayloadSize("payload", strings.Repeat("0", limit)),
	)
	require.ErrorContains(
		t,
		checkEncodedPayloadSize("payload", strings.Repeat("0", limit+1)),
		"too large",
	)
	require.NoError(
		t,
		checkDecodedPayloadSize("payload", maxCardanoPayloadBytes),
	)
	require.ErrorContains(
		t,
		checkDecodedPayloadSize("payload", maxCardanoPayloadBytes+1),
		"too large",
	)
}

// An oversized transaction is refused from its size alone: it never reaches
// the parser or the evaluator, whatever its encoding.
func TestEvaluateTxRejectsOversizedPayloadWithoutParsing(t *testing.T) {
	t.Parallel()
	evaluator := newBlockingEvaluator()
	cs := evaluateTxSession(t, evaluator)

	for name, payload := range map[string]string{
		// More text than any encoding of a payload at the cap.
		"hex text over the cap": strings.Repeat("00", maxCardanoPayloadBytes+1),
		// Short enough as text, but it decodes to more than the cap. The
		// bytes are chosen so the text contains '/', which keeps it from
		// also reading as hex: the handler tries hex first.
		"base64 that decodes over the cap": base64.StdEncoding.EncodeToString(
			bytes.Repeat([]byte{0xff}, maxCardanoPayloadBytes+1),
		),
	} {
		t.Run(name, func(t *testing.T) {
			text := callTool(
				t,
				cs,
				"evaluate_tx",
				map[string]any{"cbor": payload},
				true,
			)
			require.Contains(t, text, "too large")
			require.NotContains(t, text, "Invalid transaction CBOR")
		})
	}
	require.Zero(t, evaluator.calls.Load())
}

// A payload exactly at the cap is not refused for its size; it is parsed, and
// fails there only because it is not a transaction.
func TestEvaluateTxAcceptsPayloadAtTheCap(t *testing.T) {
	t.Parallel()
	cs := evaluateTxSession(t, newBlockingEvaluator())
	text := callTool(
		t,
		cs,
		"evaluate_tx",
		map[string]any{"cbor": strings.Repeat("00", maxCardanoPayloadBytes)},
		true,
	)
	require.NotContains(t, text, "too large")
	require.Contains(t, text, "Invalid transaction CBOR")
}

// While one evaluation holds the only slot, a second request is refused as
// busy before its payload is parsed: a malformed body gets the busy error, not
// the parse error it would get if it were decoded outside the slot.
func TestEvaluateTxAdmitsBeforeParsing(t *testing.T) {
	t.Parallel()
	evaluator := newBlockingEvaluator()
	cs := evaluateTxSession(t, evaluator)

	first := make(chan string, 1)
	go func() {
		first <- callTool(
			t,
			cs,
			"evaluate_tx",
			map[string]any{"cbor": admissionValidTxHex},
			false,
		)
	}()
	testutil.RequireReceive(
		t,
		evaluator.entered,
		5*time.Second,
		"first evaluation",
	)

	text := callTool(t, cs, "evaluate_tx", map[string]any{"cbor": "80ff"}, true)
	require.Contains(t, text, "busy")
	require.NotContains(t, text, "trailing data")
	require.NotContains(t, text, "Invalid transaction CBOR")
	require.Equal(t, int32(1), evaluator.calls.Load())

	close(evaluator.release)
	testutil.RequireReceive(t, first, 5*time.Second, "first evaluation result")

	// With the slot free again the malformed body is parsed and rejected.
	text = callTool(t, cs, "evaluate_tx", map[string]any{"cbor": "80ff"}, true)
	require.Contains(t, text, "trailing data")
}

func TestCalculateMinUtxoRejectsOversizedOutputWithoutParsing(t *testing.T) {
	t.Parallel()
	limits := newReadLimits()
	cs := minUtxoSession(t, limits)

	for name, payload := range map[string]string{
		"text over the cap": strings.Repeat("00", maxCardanoPayloadBytes+1),
		"0x prefix over the cap": "0x" + strings.Repeat(
			"00", maxCardanoPayloadBytes+1,
		),
	} {
		t.Run(name, func(t *testing.T) {
			text := callTool(
				t,
				cs,
				"calculate_min_utxo",
				map[string]any{"output_cbor_hex": payload},
				true,
			)
			require.Contains(t, text, "too large")
			require.NotContains(t, text, "decode output CBOR")
		})
	}
	// Nothing was admitted to the parser.
	require.Zero(t, len(limits.parseSlots))
}

func TestCalculateMinUtxoAcceptsOutputAtTheCap(t *testing.T) {
	t.Parallel()
	cs := minUtxoSession(t, newReadLimits())
	text := callTool(
		t,
		cs,
		"calculate_min_utxo",
		map[string]any{
			"output_cbor_hex": strings.Repeat("00", maxCardanoPayloadBytes),
		},
		true,
	)
	require.NotContains(t, text, "too large")
	require.Contains(t, text, "decode output CBOR")
}

// Output parsing takes a slot from the shared pool and gives it back, and a
// request that finds the pool full is refused instead of parsed.
func TestCalculateMinUtxoAdmitsParsesThroughSlots(t *testing.T) {
	t.Parallel()
	limits := newReadLimits()
	cs := minUtxoSession(t, limits)
	args := map[string]any{"output_cbor_hex": admissionValidOutputHex}

	for range cap(limits.parseSlots) {
		limits.parseSlots <- struct{}{}
	}
	text := callTool(t, cs, "calculate_min_utxo", args, true)
	require.Contains(t, text, "too many concurrent payload parses")
	// A malformed body is refused the same way, not parsed.
	text = callTool(
		t,
		cs,
		"calculate_min_utxo",
		map[string]any{"output_cbor_hex": "ff"},
		true,
	)
	require.Contains(t, text, "too many concurrent payload parses")

	for range cap(limits.parseSlots) {
		<-limits.parseSlots
	}
	text = callTool(t, cs, "calculate_min_utxo", args, false)
	require.Contains(t, text, "Required Minimum Deposit")
	require.Zero(t, len(limits.parseSlots), "parse slot not released")
}

func TestRunAdmittedHoldsSlotUntilWorkReturns(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{}, 1)
	entered := make(chan struct{})
	finish := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := runAdmitted(
			ctx,
			gate,
			errParseBusy,
			"test work",
			func(context.Context) (int, error) {
				close(entered)
				<-finish
				return 1, nil
			},
		)
		done <- err
	}()
	testutil.RequireReceive(t, entered, time.Second, "work start")
	cancel()
	require.ErrorIs(
		t,
		testutil.RequireReceive(t, done, time.Second, "canceled call"),
		context.Canceled,
	)

	// The caller gave up, but the work still holds the slot.
	_, err := runAdmitted(
		t.Context(),
		gate,
		errParseBusy,
		"test work",
		func(context.Context) (int, error) {
			t.Error("second call ran while the slot was held")
			return 0, nil
		},
	)
	require.ErrorIs(t, err, errParseBusy)

	close(finish)
	testutil.WaitForCondition(
		t,
		func() bool { return len(gate) == 0 },
		time.Second,
		"slot release",
	)
}

func TestRunAdmittedReportsPanic(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{}, 1)
	_, err := runAdmitted(
		t.Context(),
		gate,
		errParseBusy,
		"test work",
		func(context.Context) (int, error) { panic("boom") },
	)
	require.ErrorContains(t, err, "test work panicked: boom")
	require.Zero(t, len(gate))
}
