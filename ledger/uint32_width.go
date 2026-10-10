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

import "math"

// fitsUint32 reports whether n survives a conversion to uint32 unchanged,
// for the offsets, lengths and transaction indexes this package stores in
// uint32 database fields. Widening to int64 before the comparison keeps it
// well-typed on a 32-bit target, where math.MaxUint32 is not representable
// as an int and the comparison would not compile at all.
func fitsUint32(n int) bool {
	return n >= 0 && int64(n) <= math.MaxUint32
}
