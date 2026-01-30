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

import "io"

// PositionReader wraps an io.Reader and tracks the current byte position.
// This is useful for tracking offsets during CBOR parsing to compute
// positions of transactions and UTxOs within block data.
type PositionReader struct {
	reader   io.Reader
	position int64
}

// NewPositionReader creates a new PositionReader wrapping the given reader.
// The initial position is 0.
func NewPositionReader(r io.Reader) *PositionReader {
	return &PositionReader{
		reader:   r,
		position: 0,
	}
}

// Read reads data from the underlying reader and updates the position.
// It implements the io.Reader interface.
func (pr *PositionReader) Read(buf []byte) (int, error) {
	n, err := pr.reader.Read(buf)
	pr.position += int64(n)
	return n, err
}

// Position returns the current byte position in the reader.
func (pr *PositionReader) Position() int64 {
	return pr.position
}
