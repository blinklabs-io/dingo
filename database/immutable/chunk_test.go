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

package immutable

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func TestChunkNextReadsDistinctCBORAtNonzeroOffset(t *testing.T) {
	t.Parallel()

	payloads := [][]byte{{0x80}, {0x81, 0x01}}
	var chunkData []byte
	blockOffsets := make([]uint64, len(payloads))
	for i, payload := range payloads {
		blockOffsets[i] = uint64(len(chunkData))
		wrapped, err := cbor.Encode([]any{uint64(1), cbor.RawMessage(payload)})
		require.NoError(t, err)
		chunkData = append(chunkData, wrapped...)
	}
	primaryData := make([]byte, 1+4*len(payloads))
	primaryData[0] = primaryIndexVersion
	for i := range payloads {
		binary.BigEndian.PutUint32(
			primaryData[1+i*4:],
			uint32(i*secondaryIndexEntrySize),
		)
	}
	secondaryData := make([]byte, secondaryIndexEntrySize*len(payloads))
	for i, offset := range blockOffsets {
		base := i * secondaryIndexEntrySize
		binary.BigEndian.PutUint64(secondaryData[base:], offset)
		copy(secondaryData[base+16:base+48], bytes.Repeat([]byte{byte(i + 1)}, 32))
		binary.BigEndian.PutUint64(secondaryData[base+48:], uint64(100+i))
	}

	tests := []struct {
		name   string
		reader func(t *testing.T) entryReader
	}{
		{name: "fileEntry", reader: func(t *testing.T) entryReader {
			path := filepath.Join(t.TempDir(), "chunk")
			require.NoError(t, os.WriteFile(path, chunkData, 0o600))
			file, err := os.Open(path)
			require.NoError(t, err)
			return fileEntry{file: file}
		}},
		{name: "bytesEntry", reader: func(t *testing.T) entryReader {
			return newBytesEntry(chunkData)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			primary := newPrimaryIndex()
			require.NoError(t, primary.Open(newBytesEntry(primaryData)))
			secondary := newSecondaryIndex()
			require.NoError(t, secondary.Open(newBytesEntry(secondaryData), primary))
			chunk := newChunk()
			require.NoError(t, chunk.Open(test.reader(t), secondary))
			t.Cleanup(func() { require.NoError(t, chunk.Close()) })

			for i, want := range payloads {
				block, err := chunk.Next()
				require.NoError(t, err)
				require.Equal(t, uint64(100+i), block.Slot)
				require.Equal(t, bytes.Repeat([]byte{byte(i + 1)}, 32), block.Hash)
				require.Equal(t, want, block.Cbor)
			}
			block, err := chunk.Next()
			require.NoError(t, err)
			require.Nil(t, block)
		})
	}
}
