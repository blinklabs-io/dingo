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

package chain_test

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestAddBlocksRecordsSpan(t *testing.T) {
	spans := testutil.RecordSpans(t)
	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	var origin common.Blake2b256
	blocks := generateTestChain(t, 1, origin, 20, 20, 4)

	require.NoError(t, c.AddBlocks(context.Background(), blocks[:2]))

	ended := spans.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, "chain.add_blocks", ended[0].Name())
	require.Contains(
		t,
		ended[0].Attributes(),
		attribute.Int("blocks.count", 2),
	)
	require.Equal(t, codes.Unset, ended[0].Status().Code)

	// blocks[3]'s parent was never added, so the batch fails.
	require.Error(t, c.AddBlocks(context.Background(), blocks[3:]))
	ended = spans.Ended()
	require.Len(t, ended, 2)
	require.Equal(t, codes.Error, ended[1].Status().Code)
}
