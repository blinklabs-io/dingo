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

package utxoref

import (
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateOverlayEncodedEntryKeepsOnlyCallerBytes(t *testing.T) {
	t.Parallel()
	overlay := NewStateOverlay()
	encoded := []byte{0xff}
	overlay.ApplyEncoded(0, encoded)
	require.Same(t, &encoded[0], &overlay.entries[0].cbor[0])
	require.Nil(t, overlay.entries[0].tx)

	_, err := overlay.View(nil, nil, ocommon.Point{})
	require.Error(t, err)
	require.Nil(t, overlay.entries[0].tx, "folding must not cache the decoded form")
}
