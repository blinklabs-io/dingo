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

package database

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPositionReaderBasic(t *testing.T) {
	data := []byte("hello world")
	r := NewPositionReader(bytes.NewReader(data))

	// Initial position should be 0
	assert.Equal(t, int64(0), r.Position())

	// Read all data
	buf := make([]byte, len(data))
	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, buf)

	// Position should now be at the end
	assert.Equal(t, int64(len(data)), r.Position())
}

func TestPositionReaderMultipleReads(t *testing.T) {
	data := []byte("hello world, this is a test")
	r := NewPositionReader(bytes.NewReader(data))

	// Read in chunks
	buf := make([]byte, 5)

	// First read
	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(5), r.Position())
	assert.Equal(t, []byte("hello"), buf)

	// Second read
	n, err = r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(10), r.Position())
	assert.Equal(t, []byte(" worl"), buf)

	// Third read
	n, err = r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, int64(15), r.Position())
	assert.Equal(t, []byte("d, th"), buf)

	// Continue reading rest
	remaining := make([]byte, 100)
	totalRead := 0
	for {
		n, err = r.Read(remaining[totalRead:])
		totalRead += n
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// Final position should be total length
	assert.Equal(t, int64(len(data)), r.Position())
}

func TestPositionReaderEOF(t *testing.T) {
	data := []byte("short")
	r := NewPositionReader(bytes.NewReader(data))

	// Read more than available
	buf := make([]byte, 100)
	n, err := r.Read(buf)
	assert.Equal(t, len(data), n)
	assert.Equal(t, int64(len(data)), r.Position())
	// First read may or may not return EOF depending on implementation
	// The important thing is that subsequent read returns EOF
	if err == nil {
		n, err = r.Read(buf)
		assert.Equal(t, 0, n)
		assert.Equal(t, io.EOF, err)
	} else {
		assert.Equal(t, io.EOF, err)
	}

	// Position should remain unchanged after EOF
	assert.Equal(t, int64(len(data)), r.Position())
}

func TestPositionReaderPartialRead(t *testing.T) {
	// Use a reader that returns partial reads
	// strings.Reader doesn't do partial reads, but we can simulate
	// by using a custom reader or small buffer reads

	data := "this is test data for partial read testing"
	r := NewPositionReader(strings.NewReader(data))

	// Read with very small buffer to force multiple reads
	buf := make([]byte, 3)
	var totalRead int64

	for {
		n, err := r.Read(buf)
		totalRead += int64(n)
		assert.Equal(t, totalRead, r.Position())

		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, int64(len(data)), totalRead)
	assert.Equal(t, int64(len(data)), r.Position())
}

// partialReader is a reader that returns at most maxBytes per read
type partialReader struct {
	r        io.Reader
	maxBytes int
}

func (p *partialReader) Read(buf []byte) (int, error) {
	if len(buf) > p.maxBytes {
		buf = buf[:p.maxBytes]
	}
	return p.r.Read(buf)
}

func TestPositionReaderWithPartialReader(t *testing.T) {
	data := []byte("0123456789abcdef")
	// Create a reader that only returns 3 bytes at a time
	partial := &partialReader{
		r:        bytes.NewReader(data),
		maxBytes: 3,
	}
	r := NewPositionReader(partial)

	// Try to read 10 bytes, but should only get 3
	buf := make([]byte, 10)
	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, int64(3), r.Position())
	assert.Equal(t, []byte("012"), buf[:n])

	// Read again
	n, err = r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, int64(6), r.Position())
	assert.Equal(t, []byte("345"), buf[:n])
}
