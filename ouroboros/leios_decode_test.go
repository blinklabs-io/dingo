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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/safedecode"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

var errLeiosDecodePanic = errors.New(
	"runtime error: index out of range [4] with length 2",
)

// TestGuardedDecodeContainsDecoderPanic covers every Leios decode routed
// through guardedDecode -- the five endorser-block manifest sites and the
// leios-notify announcement header. Peer bytes that panic the decoder must be
// reported as a decode failure, not unwound into the Leios protocol worker,
// which has no recover above it and would take the node process down. Drop
// the Guard in guardedDecode and each subtest crashes the test binary rather
// than failing.
func TestGuardedDecodeContainsDecoderPanic(t *testing.T) {
	t.Parallel()

	raw := []byte{0x81, 0xa0}
	for _, testCase := range []struct {
		name    string
		panic   func()
		wrapped error
	}{
		{name: "string value", panic: func() { panic("cbor: bad header") }},
		{
			name:    "error value",
			panic:   func() { panic(errLeiosDecodePanic) },
			wrapped: errLeiosDecodePanic,
		},
		{name: "nil value", panic: func() { panic(nil) }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			value, err := guardedDecode(
				raw,
				func([]byte) (*lcommon.LeiosEndorserBlock, error) {
					testCase.panic()
					return nil, nil
				},
			)
			require.ErrorIs(t, err, safedecode.ErrDecodePanic)
			// The zero value is returned, so a panicking decode can never
			// hand a caller a partially built manifest.
			require.Nil(t, value)
			if testCase.wrapped != nil {
				require.ErrorIs(t, err, testCase.wrapped)
			}
		})
	}
}

// TestDecodeLeiosEndorserBlockNonPanickingOutcomesUnchanged keeps the two
// outcomes that must not move at the manifest sites.
func TestDecodeLeiosEndorserBlockNonPanickingOutcomesUnchanged(t *testing.T) {
	t.Parallel()

	_, ref := testLeiosManifestTx(t, 1)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)

	t.Run("valid manifest decodes", func(t *testing.T) {
		t.Parallel()
		block, err := decodeLeiosEndorserBlock(manifestRaw)
		require.NoError(t, err)
		require.Equal(t, []lcommon.LeiosTransactionReference{ref},
			block.TransactionReferences)
	})

	t.Run("malformed manifest reports an ordinary error", func(t *testing.T) {
		t.Parallel()
		block, err := decodeLeiosEndorserBlock([]byte{0xff, 0xff, 0xff})
		require.Error(t, err)
		require.NotErrorIs(t, err, safedecode.ErrDecodePanic)
		require.Nil(t, block)
	})
}

// TestDecodeLeiosAnnouncementHeaderNonPanickingOutcomesUnchanged does the same
// for the leios-notify announcement header, using the real Musashi Dijkstra
// header bytes rather than a synthetic value.
func TestDecodeLeiosAnnouncementHeaderNonPanickingOutcomesUnchanged(
	t *testing.T,
) {
	t.Parallel()

	t.Run("valid header decodes", func(t *testing.T) {
		t.Parallel()
		raw := readHexFixture(t, musashiType8HeaderFixture)
		header, err := decodeLeiosAnnouncementHeader(raw)
		require.NoError(t, err)
		require.NotNil(t, header)
	})

	t.Run("malformed header reports an ordinary error", func(t *testing.T) {
		t.Parallel()
		header, err := decodeLeiosAnnouncementHeader([]byte{0xff, 0xff, 0xff})
		require.Error(t, err)
		require.NotErrorIs(t, err, safedecode.ErrDecodePanic)
		require.Nil(t, header)
	})
}

// TestStoreLeiosEndorserBlockDecodeFailureReleasesAnnouncementLock pins the
// property that makes containing a panic at that site safe rather than worse.
// The manifest decode there runs under leiosAnnouncementsMu, which is released
// by explicit Unlock calls and not by a defer, so a decode failure must leave
// the lock free. Before containment the only thing that released it on a
// panicking manifest was the process dying.
func TestStoreLeiosEndorserBlockDecodeFailureReleasesAnnouncementLock(
	t *testing.T,
) {
	t.Parallel()

	// Malformed manifest bytes, presented under their own hash so the
	// content-address check ahead of the decode passes and the decode is
	// actually reached.
	blockRaw := []byte{0x81, 0x81, 0x00}
	point := ocommon.Point{
		Slot: 10,
		Hash: lcommon.Blake2b256Hash(blockRaw).Bytes(),
	}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	err := o.storeLeiosEndorserBlock(point, blockRaw, nil, leiosStorePeerOffered)
	require.ErrorContains(t, err, "decode leios endorser block")
	require.NotErrorIs(t, err, safedecode.ErrDecodePanic)

	locked := make(chan struct{})
	go func() {
		o.leiosAnnouncementsMu.Lock()
		o.leiosAnnouncementsMu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("leiosAnnouncementsMu still held after a failed manifest decode")
	}

	_, cached := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.False(t, cached, "a failed decode must not cache an entry")
}
