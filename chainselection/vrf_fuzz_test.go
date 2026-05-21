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

package chainselection

import (
	"bytes"
	"testing"
)

func FuzzCompareVRFOutputs(f *testing.F) {
	f.Add([]byte(nil), []byte(nil))
	f.Add(make([]byte, VRFOutputSize), make([]byte, VRFOutputSize))
	f.Add(append([]byte{0x00}, bytes.Repeat([]byte{0xff}, VRFOutputSize-1)...), bytes.Repeat([]byte{0xff}, VRFOutputSize))
	f.Add(bytes.Repeat([]byte{0xff}, VRFOutputSize), append([]byte{0x00}, bytes.Repeat([]byte{0xff}, VRFOutputSize-1)...))

	f.Fuzz(func(t *testing.T, vrfA, vrfB []byte) {
		result := CompareVRFOutputs(vrfA, vrfB)
		reversed := CompareVRFOutputs(vrfB, vrfA)

		if len(vrfA) != VRFOutputSize || len(vrfB) != VRFOutputSize {
			if result != ChainEqual || reversed != ChainEqual {
				t.Fatalf(
					"invalid VRF lengths returned (%v,%v), want (ChainEqual,ChainEqual)",
					result,
					reversed,
				)
			}
			return
		}

		switch cmp := bytes.Compare(vrfA, vrfB); {
		case cmp < 0:
			if result != ChainABetter || reversed != ChainBBetter {
				t.Fatalf("lower A comparison = (%v,%v), want (A better,B better)", result, reversed)
			}
		case cmp > 0:
			if result != ChainBBetter || reversed != ChainABetter {
				t.Fatalf("lower B comparison = (%v,%v), want (B better,A better)", result, reversed)
			}
		default:
			if result != ChainEqual || reversed != ChainEqual {
				t.Fatalf("equal VRFs comparison = (%v,%v), want equal", result, reversed)
			}
		}
	})
}
