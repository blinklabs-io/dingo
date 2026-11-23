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
	"context"
	"errors"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// tipFailsSet is a live UTxO set whose tip cannot be read.
type tipFailsSet struct{ UtxoSetWriter }

var errTipUnreadable = errors.New("tip unreadable")

func (tipFailsSet) Tip() (ocommon.Point, bool, error) {
	return ocommon.Point{}, false, errTipUnreadable
}

// A digest of the UTxO set at the anchor needs the set's tip. When the tip
// cannot be read the answer is that error, not a digest of a set it never
// saw.
func TestAnchorUtxoDigestFailsWhenTheSetTipIsUnreadable(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{config: LedgerStateConfig{
		UtxoSet: tipFailsSet{},
	}}
	_, err := ls.AnchorUtxoDigest(context.Background(), 10)
	require.ErrorIs(t, err, errTipUnreadable)
}
