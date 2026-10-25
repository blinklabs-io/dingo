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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// overlayStrictDigest is rowBatchingDigest with every surrogate id kept and
// every sqlite_sequence row added: the batch UTxO overlay assigns the
// per-row path's ids itself, so they must match exactly.
func overlayStrictDigest(t testing.TB, db *database.Database) string {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	tableRows, err := raw.Query(
		`SELECT name FROM sqlite_master
		 WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		 ORDER BY name`,
	)
	require.NoError(t, err)
	var tables []string
	for tableRows.Next() {
		var name string
		require.NoError(t, tableRows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, tableRows.Err())
	require.NoError(t, tableRows.Close())

	var out strings.Builder
	for _, table := range append(tables, "sqlite_sequence") {
		query := fmt.Sprintf(`SELECT * FROM "%s"`, table)
		if table == "sqlite_sequence" {
			query = `SELECT * FROM sqlite_sequence`
		}
		rows, err := raw.Query(query)
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		var lines []string
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			var parts []string
			for i, col := range cols {
				switch col {
				case "created_at", "updated_at", "deleted_at",
					"commit_timestamp", "timestamp":
					continue
				}
				switch v := values[i].(type) {
				case []byte:
					parts = append(parts, col+"="+hex.EncodeToString(v))
				case nil:
					parts = append(parts, col+"=NULL")
				default:
					parts = append(parts, fmt.Sprintf("%s=%v", col, v))
				}
			}
			line := strings.Join(parts, "|")
			if table == "node_settings_gate" &&
				strings.HasPrefix(line, "name=blob_store_id|") {
				continue
			}
			lines = append(lines, line)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		sort.Strings(lines)
		fmt.Fprintf(&out, "## %s (%d)\n%s\n", table, len(lines),
			strings.Join(lines, "\n"))
	}
	return out.String()
}

// overlayRun is one harness applying batches through ApplyTrustedBatch,
// on the per-row path or the overlay path.
type overlayRun struct {
	h        *externalIngestHarness
	overlays *atomic.Int64
}

func newOverlayRun(t testing.TB, overlay bool) *overlayRun {
	t.Helper()
	h := newExternalIngestHarness(t, false)
	h.ls.trustedBatchOverlayOff = !overlay
	var n atomic.Int64
	h.ls.afterTrustedBatchOverlay = func() { n.Add(1) }
	return &overlayRun{h: h, overlays: &n}
}

// applyBatches applies each batch in turn from the current ledger tip,
// running between after the batch at index i when between is not nil.
func (r *overlayRun) applyBatches(
	t testing.TB,
	batches [][]gledger.Block,
	between func(i int),
) error {
	t.Helper()
	for i, batch := range batches {
		if _, err := r.h.applyPaused(
			t, r.h.ls.Tip().Point, batch,
		); err != nil {
			return err
		}
		if between != nil {
			between(i)
		}
	}
	return nil
}

// requireOverlayMatchesPerRow applies batches on both paths and requires
// identical stored state, ids included, and identical in-memory ledger state.
func requireOverlayMatchesPerRow(
	t *testing.T,
	batches [][]gledger.Block,
	prepare func(*externalIngestHarness),
	between func(*externalIngestHarness, int),
) (perRow, overlay *overlayRun) {
	t.Helper()
	perRow = newOverlayRun(t, false)
	overlay = newOverlayRun(t, true)
	for _, run := range []*overlayRun{perRow, overlay} {
		if prepare != nil {
			prepare(run.h)
		}
		var hook func(int)
		if between != nil {
			hook = func(i int) { between(run.h, i) }
		}
		require.NoError(t, run.applyBatches(t, batches, hook))
	}
	require.Zero(t, perRow.overlays.Load(),
		"the reference must take the per-row path")
	require.Positive(t, overlay.overlays.Load(),
		"the batch must apply through the overlay")
	require.Equal(t,
		captureExternalIngestLedgerState(perRow.h.ls),
		captureExternalIngestLedgerState(overlay.h.ls),
		"in-memory ledger state diverged",
	)
	require.Equal(t,
		overlayStrictDigest(t, perRow.h.db),
		overlayStrictDigest(t, overlay.h.db),
		"the overlay path diverged from the per-row path",
	)
	return perRow, overlay
}

func TestTrustedBatchOverlayMatchesPerRowPath(t *testing.T) {
	t.Parallel()
	blocks, _ := externalIngestBlocks(t, 4)
	_, overlay := requireOverlayMatchesPerRow(
		t, [][]gledger.Block{blocks}, nil, nil,
	)
	require.Equal(t, int64(1), overlay.overlays.Load(),
		"one block-apply transaction, one overlay")
}

func TestTrustedBatchOverlayMatchesPerRowPathAcrossEpoch(t *testing.T) {
	t.Parallel()
	blocks, names := externalIngestBlocks(t, 0)
	blocks = extendExternalIngestBlocks(t, blocks, 20, 50)
	last := blocks[len(blocks)-1]
	names["late"] = newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{names["chain"].hash, uint64(0)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, 0x44, 0x55), 1: uint64(200_000),
		}},
		2: uint64(100_000),
	})
	blocks = append(blocks, rowBatchingBlock(
		t, last.SlotNumber()+10, last.BlockNumber()+1, last.Hash(),
		names["late"],
	))
	blocks = extendExternalIngestBlocks(t, blocks, 2, 10)
	_, overlay := requireOverlayMatchesPerRow(
		t, [][]gledger.Block{blocks},
		func(h *externalIngestHarness) {
			h.prepareRollover(t)
			// Hold the post-commit prune off: it would remove the spent
			// rows either path leaves, hiding which rows the batch itself
			// deleted.
			h.ls.cleanupConsumedUtxosRunning.Store(true)
		}, nil,
	)
	require.GreaterOrEqual(t, overlay.overlays.Load(), int64(2),
		"each side of the boundary applies through its own overlay")
	require.False(t, utxoRowExists(t, overlay.h.db, names["chain"].hash, 0),
		"an output spent after the boundary was kept")
}

// overlayPointerAddress is a testnet key/pointer address naming
// slot 10, transaction 0, certificate 0.
func overlayPointerAddress() []byte {
	return append(
		append([]byte{0x40}, rowBatchingKey(0x44)...),
		0x0a, 0x00, 0x00,
	)
}

// overlayAsset is a multi-asset output value of lovelace and one token.
func overlayAsset(t testing.TB, lovelace uint64, name string) cbor.RawMessage {
	t.Helper()
	value, err := cbor.Encode([]any{
		lovelace,
		map[cbor.ByteString]map[cbor.ByteString]uint64{
			cbor.NewByteString(rowBatchingKey(0x78)): {
				cbor.NewByteString([]byte(name)): 5,
			},
		},
	})
	require.NoError(t, err)
	return value
}

// The id-sequence repair: the last output the batch creates carries an
// asset and is spent again inside the batch, so the highest utxo and asset
// ids the per-row path assigns belong to rows it deletes. A pointer-address
// output survives, and a multi-asset output survives beside elided ones.
func TestTrustedBatchOverlayElidedHighestIDs(t *testing.T) {
	t.Parallel()
	seed := bytes.Repeat([]byte{0xA7}, 32)
	payStake := rowBatchingBaseAddress(t, 0x44, 0x22)
	payOther := rowBatchingBaseAddress(t, 0x44, 0x55)
	mint := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: []any{
			map[uint]any{0: payStake, 1: overlayAsset(t, 2_000_000, "keep")},
			map[uint]any{0: overlayPointerAddress(), 1: uint64(1_000_000)},
			map[uint]any{0: payStake, 1: overlayAsset(t, 3_000_000, "gone")},
		},
		2: uint64(100_000),
	})
	relay := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{mint.hash, uint64(2)}},
		1: []any{
			map[uint]any{0: payOther, 1: overlayAsset(t, 2_800_000, "hop")},
		},
		2: uint64(100_000),
	})
	// sink spends relay's only output and creates none.
	sink := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{relay.hash, uint64(0)}},
		1: []any{},
		2: uint64(2_800_000),
	})
	b1 := rowBatchingBlock(t, 10, 1, lcommon.Blake2b256{}, mint)
	b2 := rowBatchingBlock(t, 20, 2, b1.Hash(), relay)
	b3 := rowBatchingBlock(t, 30, 3, b2.Hash(), sink)
	_, overlay := requireOverlayMatchesPerRow(
		t, [][]gledger.Block{{b1, b2, b3}}, nil, nil,
	)
	raw, err := dbtest.RawSQLiteMetadata(t, overlay.h.db)
	require.NoError(t, err)
	maxID := rowBatchingQueryOne[int64](t, raw, `SELECT MAX(id) FROM utxo`)
	seq := rowBatchingQueryOne[int64](t, raw,
		`SELECT seq FROM sqlite_sequence WHERE name = 'utxo'`)
	require.Greater(t, seq, maxID,
		"the scenario must elide the highest utxo id")
	require.Equal(t, 1, rowBatchingQueryOne[int](t, raw,
		`SELECT COUNT(*) FROM utxo_pointer`), "the pointer output was lost")
}

// Withdrawals re-sum live stake from utxo, which the overlay has not
// written yet: the read barrier must write it first. The batch pays C in
// one block, withdraws from C in the next, and then spends both the output
// the barrier wrote and a pre-batch output of C.
func TestTrustedBatchOverlayWithdrawalAfterStakeChange(t *testing.T) {
	t.Parallel()
	seed := bytes.Repeat([]byte{0xAC}, 32)
	payStake := rowBatchingBaseAddress(t, 0x44, withdrawalResumStakeKey)
	payOther := rowBatchingBaseAddress(t, 0x44, 0x55)
	register := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: []any{
			map[uint]any{0: payOther, 1: uint64(1_000_000)},
			map[uint]any{0: payStake, 1: uint64(3_000_000)},
		},
		2: uint64(100_000),
		4: []any{[]any{
			uint64(0), rowBatchingCred(withdrawalResumStakeKey),
		}},
	})
	pay := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(1)}},
		1: []any{map[uint]any{0: payStake, 1: uint64(2_000_000)}},
		2: uint64(100_000),
	})
	withdraw := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(2)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(1_000_000)}},
		2: uint64(100_000),
		5: map[cbor.ByteString]uint64{
			cbor.NewByteString(
				rowBatchingRewardAccount(withdrawalResumStakeKey),
			): 1_000_000,
		},
	})
	spend := newRowBatchingTx(t, map[uint]any{
		0: []any{
			[]any{pay.hash, uint64(0)},
			[]any{register.hash, uint64(1)},
		},
		1: []any{map[uint]any{0: payStake, 1: uint64(4_000_000)}},
		2: uint64(1_000_000),
	})
	b1 := rowBatchingBlock(t, 10, 1, lcommon.Blake2b256{}, register)
	b2 := rowBatchingBlock(t, 20, 2, b1.Hash(), pay)
	b3 := rowBatchingBlock(t, 30, 3, b2.Hash(), withdraw)
	b4 := rowBatchingBlock(t, 40, 4, b3.Hash(), spend)
	_, overlay := requireOverlayMatchesPerRow(
		t, [][]gledger.Block{{b1}, {b2, b3, b4}}, nil,
		func(h *externalIngestHarness, i int) {
			if i == 0 {
				seedWithdrawalResumReward(t, h.db)
			}
		},
	)
	want := withdrawalResumStake{
		UtxoStake:   4_000_000,
		RewardStake: withdrawalResumReward - 1_000_000,
		TotalStake:  4_000_000 + withdrawalResumReward - 1_000_000,
	}
	require.Equal(t, want, withdrawalResumIndependent(t, overlay.h.db))
	require.Equal(t, want, withdrawalResumStored(t, overlay.h.db),
		"reward_live_stake diverged from the final UTxO set")
	require.False(t, utxoRowExists(t, overlay.h.db, pay.hash, 0),
		"an output the barrier wrote and the batch spent was kept")
}

// The same-block orderings of TestWithdrawalResumAfterSameBlockStakeChange,
// with the stake change and the withdrawal in one batch on both paths.
func TestTrustedBatchOverlayWithdrawalSameBlock(t *testing.T) {
	t.Parallel()
	for _, scenario := range withdrawalResumScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			blocks := withdrawalResumBlocks(t, scenario)
			_, overlay := requireOverlayMatchesPerRow(
				t, [][]gledger.Block{{blocks[0]}, {blocks[1]}}, nil,
				func(h *externalIngestHarness, i int) {
					if i == 0 {
						seedWithdrawalResumReward(t, h.db)
					}
				},
			)
			require.Equal(t,
				withdrawalResumIndependent(t, overlay.h.db),
				withdrawalResumStored(t, overlay.h.db),
			)
		})
	}
}

// An unregistered credential's live stake drops to zero in one block and
// recovers in a later one, and a second one drops to zero and is then
// registered with no stake. The per-row path deletes each reward_live_stake
// row at the block that empties it and inserts a new one later; the overlay
// writes each row once and must leave the same rows, ids and sequence.
func TestTrustedBatchOverlayStakeDropsToZeroAndRecovers(t *testing.T) {
	t.Parallel()
	seed := bytes.Repeat([]byte{0xAE}, 32)
	payD := rowBatchingBaseAddress(t, 0x44, 0x66)
	payOther := rowBatchingBaseAddress(t, 0x44, 0x55)
	first := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: []any{map[uint]any{0: payD, 1: uint64(3_000_000)}},
		2: uint64(100_000),
	})
	drain := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{first.hash, uint64(0)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(2_900_000)}},
		2: uint64(100_000),
	})
	refill := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(1)}},
		1: []any{map[uint]any{0: payD, 1: uint64(4_000_000)}},
		2: uint64(100_000),
	})
	// E is paid, emptied, then registered with no stake: its row goes and
	// comes back at zero because the account now exists.
	payE := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(2)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, 0x44, 0x67), 1: uint64(2_000_000),
		}},
		2: uint64(100_000),
	})
	drainE := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{payE.hash, uint64(0)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(1_900_000)}},
		2: uint64(100_000),
	})
	registerE := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(3)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(1_000_000)}},
		2: uint64(100_000),
		4: []any{[]any{uint64(0), rowBatchingCred(0x67)}},
	})
	// F first appears inside the batch and is emptied in it: its row is
	// inserted and deleted, leaving only a used id behind.
	payF := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(4)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, 0x44, 0x68), 1: uint64(5_000_000),
		}},
		2: uint64(100_000),
	})
	drainF := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{payF.hash, uint64(0)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(4_900_000)}},
		2: uint64(100_000),
	})
	b1 := rowBatchingBlock(t, 10, 1, lcommon.Blake2b256{}, first, payE)
	b2 := rowBatchingBlock(t, 20, 2, b1.Hash(), drain, drainE)
	b3 := rowBatchingBlock(t, 30, 3, b2.Hash(), refill, registerE)
	b4 := rowBatchingBlock(t, 40, 4, b3.Hash(), payF)
	b5 := rowBatchingBlock(t, 50, 5, b4.Hash(), drainF)
	_, overlay := requireOverlayMatchesPerRow(
		t, [][]gledger.Block{{b1}, {b2, b3, b4, b5}}, nil, nil,
	)
	raw, err := dbtest.RawSQLiteMetadata(t, overlay.h.db)
	require.NoError(t, err)
	require.Equal(t, "4000000", rowBatchingQueryOne[string](t, raw,
		`SELECT utxo_stake FROM reward_live_stake WHERE staking_key = ?`,
		rowBatchingKey(0x66)))
	require.Equal(t, "0", rowBatchingQueryOne[string](t, raw,
		`SELECT utxo_stake FROM reward_live_stake WHERE staking_key = ?`,
		rowBatchingKey(0x67)))
	require.Zero(t, rowBatchingQueryOne[int](t, raw,
		`SELECT COUNT(*) FROM reward_live_stake WHERE staking_key = ?`,
		rowBatchingKey(0x68)))
	require.Greater(t,
		rowBatchingQueryOne[int64](t, raw,
			`SELECT seq FROM sqlite_sequence
			 WHERE name = 'reward_live_stake'`),
		rowBatchingQueryOne[int64](t, raw,
			`SELECT MAX(id) FROM reward_live_stake`),
		"the scenario must leave a reserved reward_live_stake id unused")
}

// A double spend fails both paths with ErrUtxoConflict, whether the spent
// output was created in the batch or before it; a missing input is a no-op
// on both.
func TestTrustedBatchOverlayConflictAndMissingInput(t *testing.T) {
	t.Parallel()
	seed := bytes.Repeat([]byte{0xAD}, 32)
	payOther := rowBatchingBaseAddress(t, 0x44, 0x55)
	create := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(5_000_000)}},
		2: uint64(100_000),
	})
	spender := func(tag byte) rowBatchingTx {
		return newRowBatchingTx(t, map[uint]any{
			0: []any{[]any{create.hash, uint64(0)}},
			1: []any{map[uint]any{
				0: rowBatchingBaseAddress(t, tag, 0x55), 1: uint64(4_000_000),
			}},
			2: uint64(1_000_000),
		})
	}
	first, second := spender(0x61), spender(0x62)
	missing := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{bytes.Repeat([]byte{0xEE}, 32), uint64(3)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(1_000_000)}},
		2: uint64(100_000),
	})
	b1 := rowBatchingBlock(t, 10, 1, lcommon.Blake2b256{}, create)
	b2 := rowBatchingBlock(t, 20, 2, b1.Hash(), first)
	b3 := rowBatchingBlock(t, 30, 3, b2.Hash(), second)

	for _, tc := range []struct {
		name    string
		batches [][]gledger.Block
	}{
		{"created_in_batch", [][]gledger.Block{{b1, b2, b3}}},
		{"created_before_batch", [][]gledger.Block{{b1}, {b2, b3}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, overlay := range []bool{false, true} {
				run := newOverlayRun(t, overlay)
				err := run.applyBatches(t, tc.batches, nil)
				require.ErrorIs(t, err, types.ErrUtxoConflict,
					"overlay=%t", overlay)
			}
		})
	}
	t.Run("missing_input", func(t *testing.T) {
		t.Parallel()
		m1 := rowBatchingBlock(t, 10, 1, lcommon.Blake2b256{}, create)
		m2 := rowBatchingBlock(t, 20, 2, m1.Hash(), missing)
		requireOverlayMatchesPerRow(
			t, [][]gledger.Block{{m1, m2}}, nil, nil,
		)
	})
}

// A row the overlay does not expect -- here one a snapshot import created
// ahead of its producer -- makes the overlay's insert hit UNIQUE. The batch
// then applies once more on the per-row path, which repairs the row, with
// one Warn line and one fallback counted.
func TestTrustedBatchOverlayFallsBackOnConflict(t *testing.T) {
	t.Parallel()
	blocks, names := externalIngestBlocks(t, 2)
	precreate := func(h *externalIngestHarness) {
		raw, err := dbtest.RawSQLiteMetadata(t, h.db)
		require.NoError(t, err)
		_, err = raw.Exec(
			`INSERT INTO utxo (tx_id, output_idx, payment_key, staking_key,
			   credential_tag, added_slot, deleted_slot, amount,
			   payment_script)
			 VALUES (?, 0, ?, ?, 0, 10, 0, '1000000', 0)`,
			names["pool"].hash, rowBatchingKey(0x44), rowBatchingKey(0x55),
		)
		require.NoError(t, err)
	}

	perRow := newOverlayRun(t, false)
	precreate(perRow.h)
	require.NoError(t, perRow.applyBatches(
		t, [][]gledger.Block{blocks}, nil,
	))

	overlay := newOverlayRun(t, true)
	precreate(overlay.h)
	var logs bytes.Buffer
	overlay.h.ls.config.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	require.NoError(t, overlay.applyBatches(
		t, [][]gledger.Block{blocks}, nil,
	))
	require.Equal(t, 1, strings.Count(logs.String(), "level=WARN"),
		logs.String())
	require.Contains(t, logs.String(), "per-row path")
	require.InDelta(t, 1, promtestutil.ToFloat64(
		overlay.h.ls.metrics.trustedBatchOverlayFallbacks,
	), 0)
	require.Equal(t,
		overlayStrictDigest(t, perRow.h.db),
		overlayStrictDigest(t, overlay.h.db),
		"the fallback diverged from the per-row path",
	)
	require.Equal(t,
		captureExternalIngestLedgerState(perRow.h.ls),
		captureExternalIngestLedgerState(overlay.h.ls),
	)
}

// A failure after the overlay has written its rows, but before commit,
// commits nothing; the retry from the unchanged tip reaches the per-row
// path's state.
func TestTrustedBatchOverlayFailureBeforeCommit(t *testing.T) {
	t.Parallel()
	blocks, _ := externalIngestBlocks(t, 4)
	perRow := newOverlayRun(t, false)
	require.NoError(t, perRow.applyBatches(
		t, [][]gledger.Block{blocks}, nil,
	))

	run := newOverlayRun(t, true)
	injected := errors.New("injected failure after the overlay flush")
	var fail atomic.Bool
	fail.Store(true)
	run.h.ls.trustedBatchBeforeCommit = func([]gledger.Block) error {
		if fail.Load() {
			return injected
		}
		return nil
	}
	_, err := run.h.applyPaused(t, ocommon.Point{}, blocks)
	require.ErrorIs(t, err, injected)
	require.Positive(t, run.overlays.Load())
	require.Zero(t, run.h.ls.Tip().Point.Slot, "the ledger tip moved")
	require.Zero(t, utxoRowCount(t, run.h.db), "metadata rows were committed")

	fail.Store(false)
	_, err = run.h.applyPaused(t, ocommon.Point{}, blocks)
	require.NoError(t, err)
	require.Equal(t,
		overlayStrictDigest(t, perRow.h.db),
		overlayStrictDigest(t, run.h.db),
		"the retry after a failed overlay batch diverged",
	)
}

// overlayBenchBlocks returns a setup block that fans out outputs, followed
// by blocks blocks of txsPerBlock transactions. Each transaction spends one
// fan-out output and the change of the transaction before it, and pays one
// output that survives and one change output the next transaction spends.
func overlayBenchBlocks(
	tb testing.TB,
	blocks, txsPerBlock int,
) (setup gledger.Block, batch []gledger.Block) {
	tb.Helper()
	total := blocks * txsPerBlock
	seed := bytes.Repeat([]byte{0xB0}, 32)
	fanout := make([]any, total+1)
	for i := range fanout {
		fanout[i] = map[uint]any{
			0: rowBatchingBaseAddress(tb, 0x40, byte(0x80+i%16)),
			1: uint64(10_000_000),
		}
	}
	fan := newRowBatchingTx(tb, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: fanout,
		2: uint64(100_000),
	})
	setup = rowBatchingBlock(tb, 10, 1, lcommon.Blake2b256{}, fan)
	prev := setup
	change := []any{fan.hash, uint64(total)}
	for b := range blocks {
		txs := make([]rowBatchingTx, 0, txsPerBlock)
		for i := range txsPerBlock {
			n := b*txsPerBlock + i
			tx := newRowBatchingTx(tb, map[uint]any{
				0: []any{
					[]any{fan.hash, uint64(n)},
					change,
				},
				1: []any{
					map[uint]any{
						0: rowBatchingBaseAddress(tb, 0x41, byte(0x90+n%16)),
						1: uint64(9_000_000),
					},
					map[uint]any{
						0: rowBatchingBaseAddress(tb, 0x42, byte(0xA0+n%16)),
						1: uint64(10_000_000),
					},
				},
				2: uint64(1_000_000),
			})
			txs = append(txs, tx)
			change = []any{tx.hash, uint64(1)}
		}
		block := rowBatchingBlock(
			tb, prev.SlotNumber()+2, prev.BlockNumber()+1, prev.Hash(),
			txs...,
		)
		batch = append(batch, block)
		prev = block
	}
	return setup, batch
}

// BenchmarkApplyTrustedBatch applies the same synthetic batch through the
// per-row path and the overlay, each into a fresh store holding only the
// setup block. The batch is MaxTrustedBatchBlocks blocks; half the outputs
// it creates are spent again inside it.
func BenchmarkApplyTrustedBatch(b *testing.B) {
	const txsPerBlock = 4
	setup, batch := overlayBenchBlocks(b, MaxTrustedBatchBlocks, txsPerBlock)
	for _, overlay := range []bool{false, true} {
		name := "per_row"
		if overlay {
			name = "overlay"
		}
		b.Run(name, func(b *testing.B) {
			var applied time.Duration
			for range b.N {
				b.StopTimer()
				run := newOverlayRun(b, overlay)
				require.NoError(b, run.applyBatches(
					b, [][]gledger.Block{{setup}}, nil,
				))
				start := time.Now()
				b.StartTimer()
				_, err := run.h.applyPaused(b, run.h.ls.Tip().Point, batch)
				b.StopTimer()
				applied += time.Since(start)
				require.NoError(b, err)
				if overlay {
					require.Positive(b, run.overlays.Load())
				}
				run.h.shutdown(b)
				b.StartTimer()
			}
			b.ReportMetric(
				float64(applied.Nanoseconds())/float64(b.N*len(batch)),
				"ns/block",
			)
		})
	}
}

// The overlay is for ranking-block deltas whose inputs the blocks name, so
// Dijkstra and any later era keep the per-row path.
func TestTrustedBatchOverlayEraGate(t *testing.T) {
	t.Parallel()
	h := newExternalIngestHarness(t, false)
	h.ls.externalIngest.trusted.Store(true)
	defer h.ls.externalIngest.trusted.Store(false)
	raw := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{bytes.Repeat([]byte{0xAF}, 32), uint64(0)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, 0x44, 0x55), 1: uint64(1_000_000),
		}},
		2: uint64(100_000),
	})
	tx, err := conway.NewConwayTransactionFromCbor(raw.cbor)
	require.NoError(t, err)
	for _, tc := range []struct {
		era  uint
		want bool
	}{
		{eras.ConwayEraDesc.Id, true},
		{dijkstra.EraIdDijkstra, false},
		{dijkstra.EraIdDijkstra + 1, false},
	} {
		batch := NewLedgerDeltaBatch()
		delta := NewLedgerDelta(ocommon.NewPoint(10, raw.hash), tc.era, 1)
		delta.addTransaction(tx, 0)
		batch.addDelta(delta)
		txn := h.db.Transaction(t.Context(), true)
		acc := h.db.NewBatchAccumulator()
		enabled, err := batch.enableTrustedUtxoOverlay(h.ls, txn, acc)
		require.NoError(t, err)
		require.Equal(t, tc.want, enabled, "era %d", tc.era)
		acc.Reset()
		require.NoError(t, txn.Rollback())
		batch.Release()
	}
}

// Every asset-bearing output the batch creates is spent again inside it,
// on a store that never held an asset row: sqlite_sequence then has no
// asset row to raise, and the overlay must create the one the per-row
// path's insert-and-delete leaves.
func TestTrustedBatchOverlayCreatesMissingSequenceRow(t *testing.T) {
	t.Parallel()
	seed := bytes.Repeat([]byte{0xB1}, 32)
	payOther := rowBatchingBaseAddress(t, 0x44, 0x55)
	mint := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: []any{
			map[uint]any{0: payOther, 1: overlayAsset(t, 2_000_000, "x")},
		},
		2: uint64(100_000),
	})
	burn := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{mint.hash, uint64(0)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(1_900_000)}},
		2: uint64(100_000),
	})
	b1 := rowBatchingBlock(t, 10, 1, lcommon.Blake2b256{}, mint)
	b2 := rowBatchingBlock(t, 20, 2, b1.Hash(), burn)
	_, overlay := requireOverlayMatchesPerRow(
		t, [][]gledger.Block{{b1, b2}}, nil, nil,
	)
	raw, err := dbtest.RawSQLiteMetadata(t, overlay.h.db)
	require.NoError(t, err)
	require.Zero(t, rowBatchingQueryOne[int](t, raw,
		`SELECT COUNT(*) FROM asset`))
	require.Equal(t, 1, rowBatchingQueryOne[int](t, raw,
		`SELECT COUNT(*) FROM sqlite_sequence WHERE name = 'asset'`))
}

// Two account-existence cases the live-stake replay reads from
// created_slot: a deregistration of a never-registered credential creates a
// phantom account row after its stake was emptied, and a credential
// registered and emptied in one block keeps a zero row because its account
// existed at that block's end.
func TestTrustedBatchOverlayAccountCreatedInBatch(t *testing.T) {
	t.Parallel()
	seed := bytes.Repeat([]byte{0xB2}, 32)
	payOther := rowBatchingBaseAddress(t, 0x44, 0x55)
	payG := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, 0x44, 0x69), 1: uint64(2_000_000),
		}},
		2: uint64(100_000),
	})
	drainG := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{payG.hash, uint64(0)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(1_900_000)}},
		2: uint64(100_000),
	})
	deregisterG := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(1)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(1_000_000)}},
		2: uint64(100_000),
		4: []any{[]any{uint64(1), rowBatchingCred(0x69)}},
	})
	registerH := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(2)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, 0x44, 0x6A), 1: uint64(3_000_000),
		}},
		2: uint64(100_000),
		4: []any{[]any{uint64(0), rowBatchingCred(0x6A)}},
	})
	drainH := newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{registerH.hash, uint64(0)}},
		1: []any{map[uint]any{0: payOther, 1: uint64(2_900_000)}},
		2: uint64(100_000),
	})
	b1 := rowBatchingBlock(t, 10, 1, lcommon.Blake2b256{}, payG)
	b2 := rowBatchingBlock(t, 20, 2, b1.Hash(), drainG)
	b3 := rowBatchingBlock(t, 30, 3, b2.Hash(), deregisterG)
	b4 := rowBatchingBlock(t, 40, 4, b3.Hash(), registerH, drainH)
	_, overlay := requireOverlayMatchesPerRow(
		t, [][]gledger.Block{{b1}, {b2, b3, b4}}, nil, nil,
	)
	raw, err := dbtest.RawSQLiteMetadata(t, overlay.h.db)
	require.NoError(t, err)
	for _, key := range []byte{0x69, 0x6A} {
		require.Equal(t, 1, rowBatchingQueryOne[int](t, raw,
			`SELECT COUNT(*) FROM account WHERE staking_key = ?`,
			rowBatchingKey(key)), "credential %x has no account row", key)
	}
	require.Equal(t, 1, rowBatchingQueryOne[int](t, raw,
		`SELECT COUNT(*) FROM reward_live_stake WHERE staking_key = ?
		 AND utxo_stake = '0'`, rowBatchingKey(0x6A)),
		"the registered-and-drained credential lost its row")
}
