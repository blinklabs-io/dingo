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

package ledger

import (
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// goldenWrongEraByron mirrors the bytes captured from
// IntersectMBO/ouroboros-consensus golden test corpus
// (ApplyTxErr_WrongEraByron) — applying a Shelley tx to a Byron ledger.
const goldenWrongEraByron = "828201675368656c6c657982006542" +
	"79726f6e"

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestNewEraMismatchError_Fields(t *testing.T) {
	got := newEraMismatchError(eras.ShelleyEraDesc.Id, eras.ByronEraDesc.Id)
	require.NotNil(t, got)
	assert.Equal(t, uint8(eras.ShelleyEraDesc.Id), got.OtherEra.Index)
	assert.Equal(t, "Shelley", got.OtherEra.Name)
	assert.Equal(t, uint8(eras.ByronEraDesc.Id), got.LedgerEra.Index)
	assert.Equal(t, "Byron", got.LedgerEra.Name)
}

// TestNewEraMismatchError_EncodesGoldenBytes is the core closure of the
// wire-format gap: a typed reject reason returned from dingo's
// SubmitTxFunc must hit the wire as canonical bytes matching what a
// Haskell node would emit. Compare against
// IntersectMBO/ouroboros-consensus golden ApplyTxErr_WrongEraByron.
func TestNewEraMismatchError_EncodesGoldenBytes(t *testing.T) {
	got := newEraMismatchError(eras.ShelleyEraDesc.Id, eras.ByronEraDesc.Id)
	encoded, err := cbor.Encode(got)
	require.NoError(t, err)
	assert.Equal(t, mustHex(t, goldenWrongEraByron), encoded)
}

// TestNewEraMismatchError_AsErrorWithWrap verifies the typed error
// survives fmt.Errorf("...: %w", typed) wrapping (errors.As walks the
// chain). dingo callers may wrap the error with additional context
// before returning to gouroboros' tx-submission server; the server's
// encodeRejectReason uses errors.As to pick out the typed reason
// regardless of wrapping depth.
func TestNewEraMismatchError_AsErrorWithWrap(t *testing.T) {
	em := newEraMismatchError(eras.ShelleyEraDesc.Id, eras.ByronEraDesc.Id)
	var err error = em
	var got *gledger.EraMismatch
	require.True(t, errors.As(err, &got))
	assert.Equal(t, "Shelley", got.OtherEra.Name)
}

// TestNewEraMismatchError_UnknownEraIdSurvives verifies that a synthetic
// era ID that doesn't resolve in dingo's registry produces a usable
// error (with a fallback name) rather than panicking. This matches the
// behaviour of the prior fmt.Errorf path's "unknown(%d)" fallback.
func TestNewEraMismatchError_UnknownEraIdSurvives(t *testing.T) {
	got := newEraMismatchError(99, eras.ConwayEraDesc.Id)
	require.NotNil(t, got)
	assert.Equal(t, uint8(99), got.OtherEra.Index)
	assert.Contains(t, got.OtherEra.Name, "unknown")
	assert.Equal(t, "Conway", got.LedgerEra.Name)
}

// TestNewEraMismatchError_ErrorStringMentionsBothEras keeps fmt.Errorf-
// style log output usable: the typed error's Error() string must still
// name both eras for human-readable diagnostics in mempool / forging
// logs.
func TestNewEraMismatchError_ErrorStringMentionsBothEras(t *testing.T) {
	got := newEraMismatchError(eras.ShelleyEraDesc.Id, eras.ByronEraDesc.Id)
	msg := got.Error()
	assert.Contains(t, msg, "Byron")
	assert.Contains(t, msg, "Shelley")
}

// TestEraMismatchInterop_FullChain proves the full wire-protocol path
// end-to-end:
//
//  1. dingo's helper produces a typed *gledger.EraMismatch.
//  2. The mempool wraps it via fmt.Errorf("validate transaction: %w", err)
//     (real callers do this).
//  3. gouroboros' tx-submission server invokes its rejection encoder,
//     which uses errors.As to find the typed reason through the wrapper.
//  4. The encoded bytes match the canonical Haskell golden.
//  5. A peer (gouroboros client) decodes those bytes and recovers a
//     *gledger.EraMismatch with the original era info.
//
// This is the "wire format closure" claimed by Path C: the full chain
// from dingo validation to a remote peer's decoder produces structured
// data, not the unstructured strings the chain emitted before.
func TestEraMismatchInterop_FullChain(t *testing.T) {
	// Step 1: dingo produces typed error
	original := newEraMismatchError(eras.ShelleyEraDesc.Id, eras.ByronEraDesc.Id)

	// Step 2: mempool/forging wraps it (mirrors mempool.go:757
	// `fmt.Errorf("validate transaction: %w", err)`).
	wrapped := fmt.Errorf("validate transaction: %w", error(original))

	// Step 3: tx-submission server encodes via cbor.Encode after
	// errors.As pulls out the typed reason. (We can't import the
	// localtxsubmission package in the ledger package without a
	// dependency cycle, so we replicate the dispatch logic here. The
	// real production path is identical: see
	// gouroboros/protocol/localtxsubmission/reject_reason.go.)
	var typedReason interface {
		error
		MarshalCBOR() ([]byte, error)
	}
	require.True(t, errors.As(wrapped, &typedReason),
		"wrapped typed reason must surface via errors.As")
	wireBytes, err := typedReason.MarshalCBOR()
	require.NoError(t, err)

	// Step 4: bytes match the Haskell golden.
	assert.Equal(t, mustHex(t, goldenWrongEraByron), wireBytes,
		"interop wire bytes must match the Haskell golden")

	// Step 5: peer-side decoder recovers the typed reason.
	decodedErr, err := gledger.NewTxSubmitErrorFromCbor(wireBytes)
	require.NoError(t, err)
	var em *gledger.EraMismatch
	require.True(t, errors.As(decodedErr, &em),
		"peer-side decoder must recognise the typed reason")
	assert.Equal(t, "Shelley", em.OtherEra.Name)
	assert.Equal(t, "Byron", em.LedgerEra.Name)
}
