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

package dingo

import (
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

func TestBlockBroadcasterAddsWithoutEventSubscriber(t *testing.T) {
	blocks, err := fixtures.GenerateConwayChain(
		0,
		lcommon.Blake2b256{},
		1,
		1,
		1,
	)
	require.NoError(t, err)
	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	broadcaster := &blockBroadcaster{
		chain:  cm.PrimaryChain(),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	require.NoError(t, broadcaster.AddBlock(blocks[0], blocks[0].Cbor()))
	require.Equal(t, blocks[0].Hash().Bytes(), cm.PrimaryChain().Tip().Point.Hash)
}

func TestBlockBroadcasterRejectsUnavailableChain(t *testing.T) {
	blocks, err := fixtures.GenerateConwayChain(
		0,
		lcommon.Blake2b256{},
		1,
		1,
		1,
	)
	require.NoError(t, err)
	broadcaster := &blockBroadcaster{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	err = broadcaster.AddBlock(blocks[0], blocks[0].Cbor())
	require.EqualError(t, err, "chain unavailable")
}
