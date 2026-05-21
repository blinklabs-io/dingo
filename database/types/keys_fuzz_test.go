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

package types_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
)

func FuzzParseBlockBlobKey(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("bp"))
	f.Add(types.BlockBlobKey(0, make([]byte, 32)))
	f.Add(types.BlockBlobKey(42, bytes.Repeat([]byte{0xab}, 32)))

	f.Fuzz(func(t *testing.T, key []byte) {
		slot, hash, err := types.ParseBlockBlobKey(key)
		if err != nil {
			if len(key) == types.BlockBlobKeySize &&
				bytes.HasPrefix(key, []byte(types.BlockBlobKeyPrefix)) {
				t.Fatalf("ParseBlockBlobKey rejected a well-shaped key: %v", err)
			}
			return
		}

		if len(hash) != 32 {
			t.Fatalf("parsed hash length = %d, want 32", len(hash))
		}

		encoded := types.BlockBlobKey(slot, hash)
		if !bytes.Equal(encoded, key) {
			t.Fatalf("BlockBlobKey(ParseBlockBlobKey(key)) = %x, want %x", encoded, key)
		}
	})
}
