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
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

// TestChainExtendedSourceNamesOurBlockOnly applies one real block while the
// forged-block checker claims a block at the same slot, and reads the
// "chain extended" line's source field. Only a checker hash equal to the new
// tip's hash makes the tip our block; a different hash at the slot is a
// remote block that won the slot battle.
func TestChainExtendedSourceNamesOurBlockOnly(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		ourHash    func(tip gledger.Block) []byte
		wantSource string
	}{
		{
			name:       "tip is the block we forged",
			ourHash:    func(tip gledger.Block) []byte { return tip.Hash().Bytes() },
			wantSource: "forged",
		},
		{
			name:       "tip won the slot against the block we forged",
			ourHash:    func(gledger.Block) []byte { return []byte{0xDE, 0xAD} },
			wantSource: "chainsync",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls, _, firstShelley := newByronShelleyBoundaryLedger(t)
			var logs syncSafeBuffer
			ls.config.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
			ls.config.ForgedBlockChecker = &mockForgedBlockChecker{
				forgedSlots: map[uint64][]byte{
					firstShelley.SlotNumber(): tc.ourHash(firstShelley),
				},
			}

			results := make(chan readChainResult, 1)
			results <- readChainResult{blocks: []gledger.Block{firstShelley}}
			close(results)
			require.NoError(t, ls.ledgerProcessBlocksFromSource(
				context.Background(),
				results,
			))

			var sources []string
			scanner := bufio.NewScanner(strings.NewReader(logs.String()))
			for scanner.Scan() {
				var rec struct {
					Msg    string `json:"msg"`
					Source string `json:"source"`
				}
				require.NoError(t, json.Unmarshal(scanner.Bytes(), &rec))
				if strings.HasPrefix(rec.Msg, "chain extended") {
					sources = append(sources, rec.Source)
				}
			}
			require.Equal(t, []string{tc.wantSource}, sources)
		})
	}
}
