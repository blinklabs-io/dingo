// Copyright 2024 Blink Labs Software
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

package immutable_test

import (
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/node/database/immutable"
)

const (
	testDataDir = "./testdata"
)

func TestGetTip(t *testing.T) {
	// These expected values correspond to the last block in our test data
	var expectedSlot uint64 = 38426380
	expectedHash := "7ada6ed78f6caa499370da6548b143c59320f5e5283e5e80e202a994ba7bfebf"
	imm, err := immutable.New(testDataDir)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tip, err := imm.GetTip()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if tip == nil {
		t.Fatalf("did not get expected tip value, got nil instead")
	}
	if tip.Slot != expectedSlot {
		t.Fatalf("did not get expected slot value: expected %d, got %d", expectedSlot, tip.Slot)
	}
	if hex.EncodeToString(tip.Hash) != expectedHash {
		t.Fatalf("did not get expected hash value: expected %s, got %x", expectedHash, tip.Hash)
	}
}

// TODO: add tests for getting a specific block and getting a range of blocks that traverses multiple chunks
