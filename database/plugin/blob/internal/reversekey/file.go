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

package reversekey

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
)

// File reads a temporary key spool in reverse order.
type File struct {
	file        *os.File
	pos         int64
	initialized bool
}

// Write appends a length-framed key to a spool file.
func Write(file *os.File, key string) error {
	// len(key) is compared as int64 rather than directly against the
	// untyped constant math.MaxUint32: on a 32-bit platform int is 32
	// bits wide and that constant does not fit in it, so the naive
	// comparison fails to compile.
	if int64(len(key)) > math.MaxUint32 {
		return fmt.Errorf("key length %d exceeds uint32 maximum", len(key))
	}
	length := make([]byte, 4)
	// #nosec G115 -- length is bounds-checked against math.MaxUint32 above.
	binary.BigEndian.PutUint32(length, uint32(len(key)))
	if _, err := file.Write(length); err != nil {
		return err
	}
	if _, err := file.WriteString(key); err != nil {
		return err
	}
	_, err := file.Write(length)
	return err
}

// NextReverse returns the preceding key, or false at the start of the spool.
func (f *File) NextReverse() (string, bool, error) {
	if !f.initialized {
		info, err := f.file.Stat()
		if err != nil {
			return "", false, err
		}
		f.pos = info.Size()
		f.initialized = true
	}
	if f.pos == 0 {
		return "", false, nil
	}
	// Every record has a four-byte prefix and matching trailer. Validate
	// the frame before trusting its declared allocation size.
	if f.pos < 8 {
		return "", false, errors.New("truncated reverse key record")
	}
	trailer := make([]byte, 4)
	if _, err := f.file.ReadAt(trailer, f.pos-4); err != nil {
		return "", false, err
	}
	length := int64(binary.BigEndian.Uint32(trailer))
	if length > f.pos-8 || length > math.MaxInt {
		return "", false, errors.New("invalid reverse key record length")
	}
	start := f.pos - 8 - length
	prefix := make([]byte, 4)
	if _, err := f.file.ReadAt(prefix, start); err != nil {
		return "", false, err
	}
	if int64(binary.BigEndian.Uint32(prefix)) != length {
		return "", false, errors.New("reverse key record lengths do not match")
	}
	key := make([]byte, length)
	if _, err := f.file.ReadAt(key, start+4); err != nil {
		return "", false, err
	}
	f.pos = start
	return string(key), true, nil
}

// New creates a reverse cursor over file. Close removes the backing spool.
func New(file *os.File) *File { return &File{file: file} }

// Reset positions the next read at the end of the spool.
func (f *File) Reset() {
	f.pos = 0
	f.initialized = false
}

// Close closes and removes the temporary spool. Repeated calls are safe.
func (f *File) Close() {
	if f.file != nil {
		name := f.file.Name()
		_ = f.file.Close()
		_ = os.Remove(name)
		f.file = nil
	}
}
