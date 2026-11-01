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
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// An endorser block validates against the announcing ranking block as ledger
// tip, and a voter has only the vote window to act on it. The ledger must say
// so as soon as the chunk ending with that block commits; waiting for the next
// block to be applied delays the vote by a full block interval.
func TestLeiosAnnouncingBlockAtTipIsNotifiedWithoutNextBlock(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	announcing, _ := f.block(
		1,
		leiosCertApplyAnnouncingSlot,
		f.originHash,
		false,
		true,
	)
	f.storeOriginAndBlock(false)
	f.ls.chainsyncState = SyncingChainsyncState
	f.storeBlock(announcing)

	type atTip struct {
		header lcommon.BlockHeader
		tip    []byte
	}
	notified := make(chan atTip, 1)
	f.ls.config.ValidateLeiosAnnouncement = func(
		context.Context,
		lcommon.BlockHeader,
	) error {
		return nil
	}
	f.ls.config.LeiosAnnouncementAtTip = func(header lcommon.BlockHeader) {
		f.ls.RLock()
		tip := bytes.Clone(f.ls.currentTip.Point.Hash)
		f.ls.RUnlock()
		notified <- atTip{header: header, tip: tip}
	}

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{announcing}}
	close(results)
	require.NoError(t, f.ls.ledgerProcessBlocksFromSource(
		t.Context(),
		results,
	))

	got := testutil.RequireReceive(
		t,
		notified,
		5*time.Second,
		"announcing block committed as ledger tip",
	)
	require.Equal(t, announcing.Hash().Bytes(), got.header.Hash().Bytes())
	require.Equal(t, announcing.Hash().Bytes(), got.tip,
		"the announcing block must already be the ledger tip")
}
