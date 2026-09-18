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

import (
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// nestedListMetadatum builds depth nested single-element lists around a
// zero, matching the exact issue#4351 minimal vector's shape.
func nestedListMetadatum(depth int) []byte {
	out := make([]byte, 0, depth+1)
	for range depth {
		out = append(out, 0x81)
	}
	return append(out, 0x00)
}

// TestDecodeMetadatumAcceptsIssue4351Vector confirms dingo's pinned
// gouroboros dependency accepts blinklabs-io/dingo#4351's exact minimal
// vector -- a Shelley-era auxiliary metadata value of 1025 nested
// one-element lists around integer 0 -- once bumped past the version whose
// custom metadata decoder capped nesting at 1024, an arbitrary bound with no
// relation to anything the ledger enforces (fixed upstream in
// blinklabs-io/gouroboros#2389). The fix lives entirely in gouroboros, which
// has its own thorough boundary and Shelley-block-level coverage; this is a
// narrow confirmation that dingo's own dependency actually carries it,
// exercised the same way dingo's era validation reaches metadata via
// lcommon.AuxiliaryData/TransactionMetadatum (see
// ledger/eras/validation.go's preAlonzoRebuiltWireSize), not a re-test of
// gouroboros's own decoder internals.
func TestDecodeMetadatumAcceptsIssue4351Vector(t *testing.T) {
	t.Parallel()
	const issue4351Depth = 1025
	md, err := lcommon.DecodeMetadatumRaw(nestedListMetadatum(issue4351Depth))
	require.NoError(t, err, "depth %d should decode", issue4351Depth)
	depth := 0
	for {
		list, ok := md.(lcommon.MetaList)
		if !ok || len(list.Items) != 1 {
			break
		}
		depth++
		md = list.Items[0]
	}
	require.Equal(t, issue4351Depth, depth)
	_, ok := md.(lcommon.MetaInt)
	require.True(t, ok, "innermost value: unexpected %T", md)
}
