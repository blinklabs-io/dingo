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

package safedecode

import (
	"github.com/blinklabs-io/gouroboros/cbor"
)

// Cbor decodes data into a fresh value of type T, containing a decoder panic
// as Guard describes. The second return value is the byte count cbor.Decode
// reported, so a caller keeps its own trailing-bytes check; it is zero on the
// panic path, because the count is recorded only after cbor.Decode returns
// normally.
//
// T must be a type the caller owns end to end: the decoded value is built
// here and returned, so it aliases nothing the caller shares, which is what
// makes recovering around it sound rather than a way to continue from
// half-mutated state.
func Cbor[T any](data []byte) (T, int, error) {
	bytesRead := 0
	value, err := Guard(func() (T, error) {
		var out T
		read, err := cbor.Decode(data, &out)
		bytesRead = read
		return out, err
	})
	return value, bytesRead, err
}
