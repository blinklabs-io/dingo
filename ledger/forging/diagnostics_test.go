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

package forging

import (
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// corruptBlock mirrors the Conway block envelope but encodes
// transaction_bodies as a CBOR map instead of an array — the structural
// defect behind issue #2063. Used only to exercise forgedBlockDiagnostics.
type corruptBlock struct {
	cbor.StructAsArray
	Header                 cbor.RawMessage
	TransactionBodies      map[uint]cbor.RawMessage
	TransactionWitnessSets []cbor.RawMessage
	TransactionMetadataSet cbor.RawMessage
	InvalidTransactions    []uint
}

// TestForgedBlockDiagnosticsPinpointsBadBodiesField verifies that the
// diagnostic dump labels the offending block field. When the bodies slot
// holds a map instead of an array (issue #2063), the notation must show
// "transaction_bodies" rendered as a map so the mismatch is obvious.
func TestForgedBlockDiagnosticsPinpointsBadBodiesField(t *testing.T) {
	// A minimal but structurally valid Conway header: [header_body, sig].
	header, err := cbor.Encode([]any{[]any{uint64(0)}, []byte{0x00}})
	require.NoError(t, err)

	bad, err := cbor.Encode(corruptBlock{
		Header:                 cbor.RawMessage(header),
		TransactionBodies:      map[uint]cbor.RawMessage{0: {0xa0}},
		TransactionWitnessSets: []cbor.RawMessage{{0xa0}},
		TransactionMetadataSet: cbor.RawMessage{0xa0},
		InvalidTransactions:    []uint{},
	})
	require.NoError(t, err)

	diag := forgedBlockDiagnostics(bad)
	t.Logf("diagnostics:\n%s", diag)

	assert.Contains(
		t,
		diag,
		"transaction_bodies",
		"diagnostics must label the bodies field",
	)
	// The bodies field should render as a map ("{ ... }"), which is the
	// shape mismatch the decoder rejects.
	idx := strings.Index(diag, "transaction_bodies")
	require.GreaterOrEqual(t, idx, 0)
	rest := diag[idx:]
	assert.True(
		t,
		strings.HasPrefix(
			strings.TrimSpace(rest[len("transaction_bodies:"):]),
			"{",
		),
		"transaction_bodies should render as a map in the diagnostics",
	)
}

// TestForgedBlockDiagnosticsNeverPanics ensures the helper degrades
// gracefully (returns a marker, never panics or errors) on garbage input.
func TestForgedBlockDiagnosticsNeverPanics(t *testing.T) {
	for _, in := range [][]byte{
		nil,
		{},
		{0xff, 0xff, 0xff}, // malformed
		{0x01},             // a bare uint, not a block
		[]byte("not cbor at all..."),
	} {
		out := forgedBlockDiagnostics(in)
		assert.NotEmpty(t, out, "should always return some diagnostic text")
	}
}

// TestForgedBlockDiagnosticsLabelsValidBlock confirms a well-formed forged
// block dumps with the bodies field as an array (the correct shape).
func TestForgedBlockDiagnosticsLabelsValidBlock(t *testing.T) {
	creds := setupTestCredentials(t)
	pp := &mockPParamsProvider{pparams: &conway.ConwayProtocolParameters{
		MaxTxSize:        1 << 20,
		MaxBlockBodySize: 1 << 22,
		MaxBlockExUnits:  lcommon.ExUnits{Memory: 1 << 40, Steps: 1 << 40},
	}}
	ct := &mockChainTip{tip: ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1000, Hash: make([]byte, 32)},
		BlockNumber: 100,
	}}
	en := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{},
		PParamsProvider: pp,
		ChainTip:        ct,
		EpochNonce:      en,
		Credentials:     creds,
	})
	require.NoError(t, err)
	_, blockCbor, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)

	diag := forgedBlockDiagnostics(blockCbor)
	assert.Contains(t, diag, "header")
	assert.Contains(t, diag, "transaction_bodies")
	idx := strings.Index(diag, "transaction_bodies")
	require.GreaterOrEqual(t, idx, 0)
	rest := strings.TrimSpace(diag[idx+len("transaction_bodies:"):])
	assert.True(
		t,
		strings.HasPrefix(rest, "["),
		"a valid block must render transaction_bodies as an array",
	)
}
