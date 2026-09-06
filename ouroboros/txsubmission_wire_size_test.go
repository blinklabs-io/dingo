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

package ouroboros

import (
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/protocol/txsubmission"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// txsubmissionWireEncodedItem returns the bytes gouroboros puts on the wire
// for a single MsgReplyTxs item, [eraId, #6.24(txBody)]. The decode side
// keeps only the tag-24 payload, so this encoding is the only place the
// wrapper length is observable from Go.
func txsubmissionWireEncodedItem(
	t *testing.T,
	eraId uint16,
	body []byte,
) []byte {
	t.Helper()
	item := txsubmission.TxBody{EraId: eraId, TxBody: body}
	encoded, err := item.MarshalCBOR()
	require.NoError(t, err)
	return encoded
}

// TestTxsubmissionWireSizeMatchesWireEncoding checks the derived wire size
// against the real encoder across all four CBOR byte-string length header
// bands, which is what makes the observed delta 6 bytes for a 24..255 byte
// body and 7 bytes for a 256..65535 byte body.
func TestTxsubmissionWireSizeMatchesWireEncoding(t *testing.T) {
	for _, bodyLen := range []int{
		0,     // empty
		1,     // length header 1 byte
		23,    // largest body with a 1-byte length header
		24,    // smallest body with a 2-byte length header
		255,   // largest body with a 2-byte length header
		256,   // smallest body with a 3-byte length header
		65535, // largest body with a 3-byte length header
		65536, // smallest body with a 5-byte length header
	} {
		for _, eraId := range []uint16{0, 6, 23, 24, 255} {
			t.Run(
				fmt.Sprintf("era%d/len%d", eraId, bodyLen),
				func(t *testing.T) {
					body := make([]byte, bodyLen)
					encoded := txsubmissionWireEncodedItem(t, eraId, body)
					require.Equal(
						t,
						uint64(len(encoded)),
						txsubmissionWireSize(eraId, bodyLen),
					)
				},
			)
		}
	}
}

// TestTxsubmissionWireSizeOverheadBands documents the exact per-band
// overhead observed against cardano-node peers.
func TestTxsubmissionWireSizeOverheadBands(t *testing.T) {
	const conwayEraId = txsubmissionRelayTestEraId
	for _, tc := range []struct {
		bodyLen  int
		overhead uint64
	}{
		{bodyLen: 23, overhead: 5},
		{bodyLen: 24, overhead: 6},
		{bodyLen: 238, overhead: 6},
		{bodyLen: 255, overhead: 6},
		{bodyLen: 256, overhead: 7},
		{bodyLen: 2331, overhead: 7},
		{bodyLen: 65535, overhead: 7},
		{bodyLen: 65536, overhead: 9},
	} {
		t.Run(fmt.Sprintf("len%d", tc.bodyLen), func(t *testing.T) {
			require.Equal(
				t,
				uint64(tc.bodyLen)+tc.overhead,
				txsubmissionWireSize(conwayEraId, tc.bodyLen),
			)
		})
	}
}

// TestValidateTxsubmissionReplyAcceptsWireSizeAdvertisement is the
// regression test for the size-validation regression from #3883: a
// cardano-node peer advertises the wrapped wire size in MsgReplyTxIds while
// gouroboros hands Dingo only the unwrapped body, so an equality check
// against len(TxBody) rejects every batch such a peer offers.
func TestValidateTxsubmissionReplyAcceptsWireSizeAdvertisement(t *testing.T) {
	fixtures := txsubmissionTestFixtures(t)
	requested := make([]txsubmission.TxIdAndSize, 0, len(fixtures))
	returned := make([]txsubmission.TxBody, 0, len(fixtures))
	for _, fixture := range fixtures {
		wireSize := len(
			txsubmissionWireEncodedItem(
				t,
				fixture.txId.EraId,
				fixture.body,
			),
		)
		require.Greater(t, wireSize, len(fixture.body))
		requested = append(requested, txsubmission.TxIdAndSize{
			TxId: fixture.txId,
			Size: uint32(wireSize), // #nosec G115 -- test fixture
		})
		returned = append(returned, txsubmission.TxBody{
			EraId:  fixture.txId.EraId,
			TxBody: fixture.body,
		})
	}

	validated, err := validateTxsubmissionReply(requested, returned)
	require.NoError(t, err)
	require.Len(t, validated, len(returned))
}

// TestValidateTxsubmissionReplyRejectsGenuineSizeMismatch verifies the
// wire-size allowance does not turn the size check into a range check: only
// the unwrapped body size and the exact derived wire size are accepted.
func TestValidateTxsubmissionReplyRejectsGenuineSizeMismatch(t *testing.T) {
	fixture := txsubmissionTestFixtures(t)[0]
	returned := []txsubmission.TxBody{
		{EraId: fixture.txId.EraId, TxBody: fixture.body},
	}
	wireSize := uint32( // #nosec G115 -- test fixture
		len(
			txsubmissionWireEncodedItem(t, fixture.txId.EraId, fixture.body),
		),
	)
	bodySize := uint32(len(fixture.body)) // #nosec G115 -- test fixture
	for _, tc := range []struct {
		name  string
		size  uint32
		match string
	}{
		// An advertisement below the body size is a size mismatch like
		// any other, and must be classified as one rather than falling
		// through to the aggregate byte-budget error.
		{name: "one below body", size: bodySize - 1, match: "size mismatch"},
		{name: "zero", size: 0, match: "size mismatch"},
		{name: "one above body", size: bodySize + 1, match: "size mismatch"},
		{name: "one below wire", size: wireSize - 1, match: "size mismatch"},
		{name: "one above wire", size: wireSize + 1, match: "size mismatch"},
		{
			name:  "beyond wrapper overhead",
			size:  bodySize + 8,
			match: "size mismatch",
		},
		{name: "double", size: bodySize * 2, match: "size mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requested := []txsubmission.TxIdAndSize{
				{TxId: fixture.txId, Size: tc.size},
			}
			validated, err := validateTxsubmissionReply(requested, returned)
			require.ErrorContains(t, err, tc.match)
			require.Nil(t, validated)
		})
	}
}

// TestTxSubmissionRelayAdmitsWireSizeAdvertisedTransaction drives the real
// pull loop end to end. Since Dingo's client now advertises the wrapped
// wire size, node B stands in for a cardano-node peer: node A must accept
// and admit the body and count the acceptance under the wire-size outcome.
func TestTxSubmissionRelayAdmitsWireSizeAdvertisedTransaction(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := newTxSubmissionRelayHarnessWithOpts(t, txSubmissionRelayHarnessOpts{
		promRegistryA: reg,
	})
	defer h.close(t)

	fixture := txsubmissionTestFixtures(t)[0]
	addTxSubmissionTestFixtures(t, h.mB, fixture)
	require.NoError(t, h.nodeB.txsubmissionClientStart(h.connB.Id()))

	require.Eventually(
		t,
		func() bool {
			_, ok := h.mA.GetTransaction(fixture.hash)
			return ok
		},
		5*time.Second,
		10*time.Millisecond,
		"expected a wire-size-advertised transaction to be admitted",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			h.nodeA.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeAcceptedWire),
		),
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			h.nodeA.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeRejected),
		),
	)
}

// TestTxsubmissionReplySizeMetricPreMaterialized verifies both outcomes are
// exported as zero before the first mismatch, so an alert on the counter
// does not have to tolerate a missing series.
func TestTxsubmissionReplySizeMetricPreMaterialized(t *testing.T) {
	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	families, err := reg.Gather()
	require.NoError(t, err)
	outcomes := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "dingo_txsubmission_reply_size_mismatch_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "outcome" {
					outcomes[label.GetValue()] = metric.GetCounter().
						GetValue()
				}
			}
		}
	}
	require.Equal(
		t,
		map[string]float64{
			txsubmissionReplySizeAcceptedWire: 0,
			txsubmissionReplySizeRejected:     0,
		},
		outcomes,
	)

	o.recordTxsubmissionReplySize(txsubmissionReplySizeRejected, 1)
	o.recordTxsubmissionReplySize(txsubmissionReplySizeAcceptedWire, 3)
	// A zero or negative count must not create spurious observations.
	o.recordTxsubmissionReplySize(txsubmissionReplySizeRejected, 0)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			o.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeRejected),
		),
	)
	require.Equal(
		t,
		float64(3),
		testutil.ToFloat64(
			o.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeAcceptedWire),
		),
	)
}

// TestRecordTxsubmissionReplySizeWithoutMetrics verifies the recorder is a
// no-op when metrics were never initialized.
func TestRecordTxsubmissionReplySizeWithoutMetrics(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})
	require.Nil(t, o.protocolMetrics)
	require.NotPanics(t, func() {
		o.recordTxsubmissionReplySize(txsubmissionReplySizeRejected, 1)
	})
}

// TestValidateTxsubmissionReplyUndersizedAdvertisementIsCounted covers a
// peer advertising a size SMALLER than the body it returns. That case used
// to trip the aggregate byte-budget check before the per-body size check
// ran, so the reply was dropped without being classified or counted as a
// size mismatch.
func TestValidateTxsubmissionReplyUndersizedAdvertisementIsCounted(
	t *testing.T,
) {
	fixture := txsubmissionTestFixtures(t)[0]
	returned := []txsubmission.TxBody{
		{EraId: fixture.txId.EraId, TxBody: fixture.body},
	}
	requested := []txsubmission.TxIdAndSize{
		{
			TxId: fixture.txId,
			Size: uint32(len(fixture.body)) - 1, // #nosec G115 -- fixture
		},
	}

	validated, err := validateTxsubmissionReply(requested, returned)
	require.Nil(t, validated)
	require.ErrorIs(t, err, errTxsubmissionReplySizeMismatch)
	// The operator needs both numbers and the era to tell an undersized
	// advertisement apart from a wrapper-size disagreement.
	require.ErrorContains(t, err, "advertised")
	require.ErrorContains(t, err, "body")
	require.ErrorContains(t, err, "wire")
	require.ErrorContains(t, err, "era")

	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{PromRegistry: reg})
	o.recordTxsubmissionReplyOutcome(validated, len(returned), err)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			o.protocolMetrics.txsubmissionReplySizeMismatch.
				WithLabelValues(txsubmissionReplySizeRejected),
		),
	)
}

// TestRecordTxsubmissionReplyOutcomeCountsBodies pins the unit of both
// outcomes: each counts reply BODIES, never replies. A three-body reply
// that is accepted adds three to accepted_wire_size, and a three-body
// reply dropped for a size mismatch adds three to rejected, because the
// whole reply is dropped.
func TestRecordTxsubmissionReplyOutcomeCountsBodies(t *testing.T) {
	fixtures := txsubmissionTestFixtures(t)
	require.Len(t, fixtures, 3)
	requested := make([]txsubmission.TxIdAndSize, 0, len(fixtures))
	returned := make([]txsubmission.TxBody, 0, len(fixtures))
	for _, fixture := range fixtures {
		requested = append(requested, txsubmission.TxIdAndSize{
			TxId: fixture.txId,
			Size: uint32( // #nosec G115 -- test fixture
				len(
					txsubmissionWireEncodedItem(
						t,
						fixture.txId.EraId,
						fixture.body,
					),
				),
			),
		})
		returned = append(returned, txsubmission.TxBody{
			EraId:  fixture.txId.EraId,
			TxBody: fixture.body,
		})
	}

	t.Run("accepted counts three bodies", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		o := newOuroboros(OuroborosConfig{PromRegistry: reg})
		validated, err := validateTxsubmissionReply(requested, returned)
		require.NoError(t, err)
		require.Len(t, validated, 3)
		o.recordTxsubmissionReplyOutcome(validated, len(returned), err)
		require.Equal(
			t,
			float64(3),
			testutil.ToFloat64(
				o.protocolMetrics.txsubmissionReplySizeMismatch.
					WithLabelValues(txsubmissionReplySizeAcceptedWire),
			),
		)
		require.Equal(
			t,
			float64(0),
			testutil.ToFloat64(
				o.protocolMetrics.txsubmissionReplySizeMismatch.
					WithLabelValues(txsubmissionReplySizeRejected),
			),
		)
	})

	// One bad advertisement drops the whole three-body reply, so all three
	// bodies are counted as rejected regardless of which one was bad.
	for _, badIdx := range []int{0, 1, 2} {
		t.Run(
			fmt.Sprintf("rejected counts three bodies bad%d", badIdx),
			func(t *testing.T) {
				bad := make([]txsubmission.TxIdAndSize, len(requested))
				copy(bad, requested)
				bad[badIdx].Size += 8
				reg := prometheus.NewRegistry()
				o := newOuroboros(OuroborosConfig{PromRegistry: reg})
				validated, err := validateTxsubmissionReply(bad, returned)
				require.ErrorIs(t, err, errTxsubmissionReplySizeMismatch)
				o.recordTxsubmissionReplyOutcome(
					validated,
					len(returned),
					err,
				)
				require.Equal(
					t,
					float64(3),
					testutil.ToFloat64(
						o.protocolMetrics.txsubmissionReplySizeMismatch.
							WithLabelValues(
								txsubmissionReplySizeRejected,
							),
					),
				)
				// A dropped reply contributes nothing to the accepted
				// outcome, even when earlier bodies validated.
				require.Equal(
					t,
					float64(0),
					testutil.ToFloat64(
						o.protocolMetrics.txsubmissionReplySizeMismatch.
							WithLabelValues(
								txsubmissionReplySizeAcceptedWire,
							),
					),
				)
			},
		)
	}
}
