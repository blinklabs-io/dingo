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

package ledgerstate

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// encodeTestNonce builds a CBOR Nonce value: [0] for NeutralNonce or
// [1, hash] for Nonce(hash). The shape matches `decodeNonce`.
func encodeTestNonce(t *testing.T, hash []byte) []byte {
	t.Helper()
	if hash == nil {
		out, err := cbor.Encode([]any{uint64(0)})
		require.NoError(t, err)
		return out
	}
	out, err := cbor.Encode([]any{uint64(1), hash})
	require.NoError(t, err)
	return out
}

// TestExtractPraosNonces_LastEpochBlockNonceIs8FieldShape locks in the
// fix for the Mithril-bootstrap VRF wedge. The cardano-ledger PraosState
// has 8 fields with `previousEpochNonce` at index 5, shifting `labNonce`
// to 6 and `lastEpochBlockNonce` to 7. The epoch-rollover formula uses
// `lastEpochBlockNonce` (index 7), so picking up `labNonce` (index 6)
// yields the wrong eta0 and every header in the next epoch fails VRF.
func TestExtractPraosNonces_LastEpochBlockNonceIs8FieldShape(t *testing.T) {
	evolving := bytes.Repeat([]byte{0xee}, 32)
	candidate := bytes.Repeat([]byte{0xcc}, 32)
	epoch := bytes.Repeat([]byte{0xee, 0x09}, 16)
	previous := bytes.Repeat([]byte{0xa0}, 32)
	lab := bytes.Repeat([]byte{0xab}, 32)
	lastEpochBlock := bytes.Repeat([]byte{0xfe}, 32)

	lastSlot, err := cbor.Encode(uint64(123456))
	require.NoError(t, err)
	ocertCounters, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)

	praosState := [][]byte{
		lastSlot,
		ocertCounters,
		encodeTestNonce(t, evolving),
		encodeTestNonce(t, candidate),
		encodeTestNonce(t, epoch),
		encodeTestNonce(t, previous),
		encodeTestNonce(t, lab),
		encodeTestNonce(t, lastEpochBlock),
	}

	got, err := extractPraosNonces(praosState)
	require.NoError(t, err)

	require.Equal(t, evolving, got.EvolvingNonce, "evolving nonce")
	require.Equal(t, candidate, got.CandidateNonce, "candidate nonce")
	require.Equal(t, epoch, got.EpochNonce, "epoch nonce")
	require.Equal(
		t, lastEpochBlock, got.LastEpochBlockNonce,
		"lastEpochBlockNonce must come from the LAST element "+
			"(index 7); reading index 6 picks up labNonce and "+
			"breaks VRF on every header in the next epoch",
	)
	require.NotEqual(
		t, lab, got.LastEpochBlockNonce,
		"sanity: parser must not return labNonce as "+
			"LastEpochBlockNonce",
	)
}

// TestExtractPraosNonces_LastEpochBlockNonceIs7FieldShape covers the
// older PraosState shape (no `previousEpochNonce`), where
// `lastEpochBlockNonce` is at index 6.
func TestExtractPraosNonces_LastEpochBlockNonceIs7FieldShape(t *testing.T) {
	evolving := bytes.Repeat([]byte{0xee}, 32)
	candidate := bytes.Repeat([]byte{0xcc}, 32)
	epoch := bytes.Repeat([]byte{0xee, 0x09}, 16)
	lab := bytes.Repeat([]byte{0xab}, 32)
	lastEpochBlock := bytes.Repeat([]byte{0xfe}, 32)

	lastSlot, err := cbor.Encode(uint64(123456))
	require.NoError(t, err)
	ocertCounters, err := cbor.Encode(map[uint64]uint64{})
	require.NoError(t, err)

	praosState := [][]byte{
		lastSlot,
		ocertCounters,
		encodeTestNonce(t, evolving),
		encodeTestNonce(t, candidate),
		encodeTestNonce(t, epoch),
		encodeTestNonce(t, lab),
		encodeTestNonce(t, lastEpochBlock),
	}

	got, err := extractPraosNonces(praosState)
	require.NoError(t, err)
	require.Equal(t, evolving, got.EvolvingNonce)
	require.Equal(t, candidate, got.CandidateNonce)
	require.Equal(t, epoch, got.EpochNonce)
	require.Equal(t, lastEpochBlock, got.LastEpochBlockNonce)
}
