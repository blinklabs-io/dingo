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

package recovery

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordRoundTrip(t *testing.T) {
	t.Parallel()
	hash := bytes.Repeat([]byte{0xab}, 32)
	records := []Record{
		{
			Type:            RecordTypeBegin,
			Seq:             42,
			CommitTimestamp: 1700000000123,
			Intent: Intent{
				Kind:        IntentBlockAdd,
				Slot:        98765,
				BlockNumber: 4321,
				Hash:        hash,
			},
		},
		{
			Type:            RecordTypeBegin,
			Seq:             43,
			CommitTimestamp: 1700000000456,
			Intent: Intent{
				Kind: IntentRollback,
				Slot: 98700,
			},
		},
		{Type: RecordTypeCommit, Seq: 42},
		{Type: RecordTypeAbort, Seq: 43},
	}
	var buf bytes.Buffer
	for _, record := range records {
		frame, err := appendFrame(nil, record)
		require.NoError(t, err)
		buf.Write(frame)
	}
	for _, want := range records {
		got, err := readFrame(&buf)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	_, err := readFrame(&buf)
	assert.ErrorIs(t, err, io.EOF)
}

func TestRecordCheckpointRoundTrip(t *testing.T) {
	t.Parallel()
	cp := Checkpoint{
		Seq:              7,
		CreatedUnixMilli: 1700000000000,
		CommitTimestamp:  1699999999999,
		TipSlot:          500,
		TipHash:          bytes.Repeat([]byte{0x01}, 32),
		TipBlockNumber:   499,
		BlobTipSlot:      501,
		BlobTipHash:      bytes.Repeat([]byte{0x02}, 32),
	}
	cp.Seal()
	frame, err := appendFrame(nil, Record{
		Type:       RecordTypeCheckpoint,
		Seq:        cp.Seq,
		Checkpoint: &cp,
	})
	require.NoError(t, err)
	got, err := readFrame(bytes.NewReader(frame))
	require.NoError(t, err)
	require.NotNil(t, got.Checkpoint)
	assert.Equal(t, cp, *got.Checkpoint)
	assert.Equal(t, cp.Seq, got.Seq)
	assert.NoError(t, got.Checkpoint.Verify())
}

func TestDecodePayloadRejectsUnverifiedCheckpoint(t *testing.T) {
	cp := testCheckpoint(3)
	cp.Seal()
	cp.Seq = 4
	payload, err := encodeCheckpoint(cp)
	require.NoError(t, err)
	_, err = decodePayload(RecordTypeCheckpoint, payload)
	assert.Error(t, err)
}

func TestReadFrameRejectsCorruptedPayload(t *testing.T) {
	t.Parallel()
	frame, err := appendFrame(nil, Record{
		Type:            RecordTypeBegin,
		Seq:             1,
		CommitTimestamp: 5,
		Intent:          Intent{Kind: IntentBlockAdd, Slot: 9},
	})
	require.NoError(t, err)
	// Flip a bit in the payload, past the 10-byte header.
	frame[12] ^= 0xff
	_, err = readFrame(bytes.NewReader(frame))
	assert.ErrorIs(t, err, ErrCorruptRecord)
}

func TestReadFrameRejectsTruncatedTail(t *testing.T) {
	t.Parallel()
	frame, err := appendFrame(nil, Record{
		Type:            RecordTypeBegin,
		Seq:             1,
		CommitTimestamp: 5,
		Intent:          Intent{Kind: IntentBlockAdd, Slot: 9},
	})
	require.NoError(t, err)
	for _, cut := range []int{3, 9, len(frame) - 5, len(frame) - 1} {
		_, err := readFrame(bytes.NewReader(frame[:cut]))
		assert.Error(t, err, "cut at %d should not decode", cut)
		assert.False(
			t,
			errors.Is(err, io.EOF),
			"a partial frame is not a clean end of stream",
		)
	}
}

func TestReadFrameRejectsBadMagic(t *testing.T) {
	t.Parallel()
	frame, err := appendFrame(nil, Record{Type: RecordTypeCommit, Seq: 1})
	require.NoError(t, err)
	frame[0] = 'X'
	_, err = readFrame(bytes.NewReader(frame))
	assert.ErrorIs(t, err, ErrCorruptRecord)
}

func TestReadFrameRejectsUnknownVersion(t *testing.T) {
	t.Parallel()
	frame, err := appendFrame(nil, Record{Type: RecordTypeCommit, Seq: 1})
	require.NoError(t, err)
	frame[4] = recordVersion + 1
	_, err = readFrame(bytes.NewReader(frame))
	assert.ErrorIs(t, err, ErrUnknownVersion)
}

func TestEncodePayloadRejectsOversizeHash(t *testing.T) {
	t.Parallel()
	_, err := appendFrame(nil, Record{
		Type: RecordTypeBegin,
		Seq:  1,
		Intent: Intent{
			Kind: IntentBlockAdd,
			Hash: bytes.Repeat([]byte{0x01}, maxHashBytes+1),
		},
	})
	assert.Error(t, err)
}

func TestEncodeCheckpointRecordRequiresCheckpoint(t *testing.T) {
	t.Parallel()
	_, err := appendFrame(nil, Record{Type: RecordTypeCheckpoint, Seq: 1})
	assert.Error(t, err)
}

func TestDecodePayloadRejectsTrailingBytes(t *testing.T) {
	t.Parallel()
	payload, err := encodePayload(Record{Type: RecordTypeCommit, Seq: 3})
	require.NoError(t, err)
	_, err = decodePayload(RecordTypeCommit, append(payload, 0x00))
	assert.ErrorIs(t, err, ErrCorruptRecord)
}

func TestPointHelpers(t *testing.T) {
	t.Parallel()
	hash := bytes.Repeat([]byte{0x03}, 32)
	a := Point{Slot: 10, Hash: hash}
	assert.True(t, a.Equal(Point{Slot: 10, Hash: bytes.Clone(hash)}))
	assert.False(t, a.Equal(Point{Slot: 11, Hash: hash}))
	assert.False(t, a.IsZero())
	assert.True(t, Point{}.IsZero())
}
