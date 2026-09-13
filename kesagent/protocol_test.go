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

package kesagent

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteReadFrameRoundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	want := Hello{Protocol: ProtocolID, Mode: ModeServeKey}
	require.NoError(t, writeFrame(&buf, MaxHelloFrameLen, want))

	var got Hello
	require.NoError(t, readFrame(&buf, MaxHelloFrameLen, &got))
	require.Equal(t, want, got)
}

func TestWriteFrameRejectsOversizedPayload(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	// A message string this long forces the marshaled frame over a tiny
	// maxSize, proving writeFrame checks payload size before ever writing to
	// the connection.
	huge := SignRequest{
		Type:    "sign_request",
		Message: bytes.Repeat([]byte{0x01}, 128),
	}
	err := writeFrame(&buf, 16, huge)
	require.ErrorIs(t, err, errFrameTooLarge)
	require.Zero(t, buf.Len(), "no bytes should reach the writer for a rejected frame")
}

// TestReadFrameRejectsOversizedDeclaredLength proves a client never attempts
// to read more than maxSize bytes of payload, regardless of what the length
// prefix on the wire declares -- the P1 "unbounded Hello handshake" finding
// generalized to every frame kind. The declared length here (1 GiB) is never
// backed by that much data on the reader, so a version that tried to read it
// would hang or fail differently; readFrame must reject before attempting to
// read it at all.
func TestReadFrameRejectsOversizedDeclaredLength(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 1<<30) // declares 1 GiB, sends none of it
	buf.Write(hdr[:])

	var hello Hello
	err := readFrame(&buf, MaxHelloFrameLen, &hello)
	require.ErrorIs(t, err, errFrameTooLarge)
}

func TestReadFrameRejectsZeroLengthFrame(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	var hdr [4]byte
	buf.Write(hdr[:])

	var hello Hello
	err := readFrame(&buf, MaxHelloFrameLen, &hello)
	require.Error(t, err)
	require.False(t, errors.Is(err, errFrameTooLarge))
}
