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

// Package committimestamp decodes a blob-stored commit timestamp shared by
// every blob backend (badger, S3, GCS), rejecting a value that does not
// actually fit the int64 the rest of the codebase carries it as, rather
// than each backend separately risking silent truncation or wraparound.
package committimestamp

import (
	"errors"
	"fmt"
	"math"
	"math/big"
)

// ErrOutOfRange means a stored timestamp value does not fit in an int64.
var ErrOutOfRange = errors.New("commit timestamp value out of int64 range")

// DecodeLegacy decodes a variable-length, big-endian byte-encoded
// timestamp (the pre-fixed-width encoding some backends still read for
// backward compatibility). big.Int.Int64() is undefined for a value that
// does not fit in an int64, so this rejects rather than silently
// truncates an oversized or corrupted stored value.
//
// The error reports only the encoded byte length, not the value's decimal
// expansion: a corrupted object can be large (backends cap reads at
// hundreds of MiB), and formatting that many digits to decimal just to
// report an error would itself be an expensive, avoidable allocation.
func DecodeLegacy(data []byte) (int64, error) {
	ts := new(big.Int).SetBytes(data)
	if !ts.IsInt64() {
		return 0, fmt.Errorf(
			"%w: %d-byte encoded value",
			ErrOutOfRange,
			len(data),
		)
	}
	return ts.Int64(), nil
}

// FromFixedWidth validates a fixed-width 8-byte big-endian uint64
// timestamp before narrowing it to int64. A raw int64(v) cast is
// undefined once v's high bit is set, silently wrapping into a negative
// timestamp instead of failing.
func FromFixedWidth(v uint64) (int64, error) {
	if v > math.MaxInt64 {
		return 0, fmt.Errorf(
			"%w: 8-byte encoded value %d",
			ErrOutOfRange,
			v,
		)
	}
	return int64(v), nil // #nosec G115: checked above
}
