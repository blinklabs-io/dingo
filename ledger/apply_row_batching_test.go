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
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// rowBatchingKey returns a deterministic 28-byte key hash.
func rowBatchingKey(tag byte) []byte {
	return bytes.Repeat([]byte{tag}, 28)
}

func rowBatchingBaseAddress(t *testing.T, payment, stake byte) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyKey,
		lcommon.AddressNetworkTestnet,
		rowBatchingKey(payment),
		rowBatchingKey(stake),
	)
	require.NoError(t, err)
	raw, err := addr.Bytes()
	require.NoError(t, err)
	return raw
}

func rowBatchingRewardAccount(stake byte) []byte {
	return append([]byte{0xe0}, rowBatchingKey(stake)...)
}

func rowBatchingCred(tag byte) []any {
	return []any{uint64(0), rowBatchingKey(tag)}
}

// rowBatchingTx is one scripted transaction: its body map and the hash its
// outputs are spent by.
type rowBatchingTx struct {
	cbor []byte
	hash []byte
	// aux is the encoded auxiliary data, nil when the transaction has none.
	aux []byte
}

func newRowBatchingTx(
	t *testing.T,
	body map[uint]any,
) rowBatchingTx {
	t.Helper()
	return newRowBatchingTxWith(t, body, map[uint]any{}, nil)
}

// newRowBatchingTxWith builds a transaction carrying a witness set and
// auxiliary data, which in API mode produce the queued detail rows. Nothing
// is validated, so the witnesses need not verify.
func newRowBatchingTxWith(
	t *testing.T,
	body map[uint]any,
	witnesses map[uint]any,
	aux any,
) rowBatchingTx {
	t.Helper()
	txCbor, err := cbor.Encode([]any{
		body, witnesses, true, aux,
	})
	require.NoError(t, err)
	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	var auxCbor []byte
	if aux != nil {
		auxCbor, err = cbor.Encode(aux)
		require.NoError(t, err)
	}
	return rowBatchingTx{cbor: txCbor, hash: tx.Hash().Bytes(), aux: auxCbor}
}

func rowBatchingBlock(
	t *testing.T,
	slot, number uint64,
	prev lcommon.Blake2b256,
	txs ...rowBatchingTx,
) gledger.Block {
	t.Helper()
	block := &conway.ConwayBlock{
		BlockHeader: &conway.ConwayBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: number,
					Slot:        slot,
					PrevHash:    prev,
					VrfKey:      make([]byte, 32),
					VrfResult: lcommon.VrfResult{
						Output: make([]byte, 32),
						Proof:  make([]byte, 80),
					},
					OpCert: babbage.BabbageOpCert{
						HotVkey:   make([]byte, 32),
						Signature: make([]byte, 64),
					},
					ProtoVersion: babbage.BabbageProtoVersion{Major: 10},
				},
				Signature: make([]byte, 448),
			},
		},
	}
	auxByIndex := map[uint]cbor.RawMessage{}
	for i, raw := range txs {
		tx, err := conway.NewConwayTransactionFromCbor(raw.cbor)
		require.NoError(t, err)
		block.TransactionBodies = append(block.TransactionBodies, tx.Body)
		block.TransactionWitnessSets = append(
			block.TransactionWitnessSets, tx.WitnessSet,
		)
		if raw.aux != nil {
			auxByIndex[uint(i)] = raw.aux
		}
	}
	// A block carries auxiliary data beside the bodies, keyed by index.
	auxSet, err := cbor.Encode(auxByIndex)
	require.NoError(t, err)
	require.NoError(t, block.TransactionMetadataSet.UnmarshalCBOR(auxSet))
	// The block body hash covers the four body components, so it is computed
	// from a first encoding that carries every transaction.
	first, err := cbor.Encode(block)
	require.NoError(t, err)
	var comps []cbor.RawMessage
	_, err = cbor.Decode(first, &comps)
	require.NoError(t, err)
	require.Len(t, comps, 5)
	var concat []byte
	for _, comp := range comps[1:] {
		concat = append(concat, lcommon.Blake2b256Hash(comp).Bytes()...)
	}
	block.BlockHeader.Body.BlockBodyHash = lcommon.Blake2b256Hash(concat)
	encoded, err := cbor.Encode(block)
	require.NoError(t, err)
	decoded, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, encoded)
	require.NoError(t, err)
	return decoded
}

// rowBatchingScenario returns three linked blocks, applied as one chunk:
//
//	block 1: a pool registration, a stake registration whose output (carrying
//	         a native asset) is paid to that stake credential, a DRep
//	         registration, and a governance proposal.
//	block 2: a delegation of the registered stake credential and a vote
//	         delegation, plus a transaction spending an output block 1
//	         produced and withdrawing zero rewards from the registered
//	         account, and a treasury donation.
//	block 3: a transaction spending an output transaction 2 produced.
//
// The spend and chain transactions carry witnesses, a native script and
// metadata, and both carry the same Plutus datum, so the API-mode detail
// tables are populated and the shared datum row records the earlier slot.
//
// The returned names identify the transactions the assertions refer to.
func rowBatchingScenario(t *testing.T) (
	blocks []gledger.Block,
	names map[string]rowBatchingTx,
) {
	t.Helper()
	const (
		poolKey  = 0x11
		stakeKey = 0x22
		drepKey  = 0x33
		payKey   = 0x44
		otherKey = 0x55
	)
	seed := bytes.Repeat([]byte{0xAA}, 32)
	asset, err := cbor.Encode([]any{
		uint64(3_000_000),
		map[cbor.ByteString]map[cbor.ByteString]uint64{
			cbor.NewByteString(rowBatchingKey(0x77)): {
				cbor.NewByteString([]byte("tok")): 10,
			},
		},
	})
	require.NoError(t, err)
	anchor := []any{"https://example.invalid/a", bytes.Repeat([]byte{0x01}, 32)}

	names = map[string]rowBatchingTx{}
	names["pool"] = newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(1)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, payKey, otherKey), 1: uint64(1_000_000),
		}},
		2: uint64(100_000),
		4: []any{[]any{
			uint64(3), rowBatchingKey(poolKey), bytes.Repeat([]byte{0x05}, 32),
			uint64(1_000_000), uint64(340_000_000),
			cbor.Tag{Number: 30, Content: []uint64{1, 10}},
			rowBatchingRewardAccount(poolKey),
			[]any{rowBatchingKey(poolKey)},
			[]any{}, nil,
		}},
	})
	names["register"] = newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(0)}},
		1: []any{
			map[uint]any{
				0: rowBatchingBaseAddress(t, payKey, stakeKey),
				1: cbor.RawMessage(asset),
			},
			map[uint]any{
				0: rowBatchingBaseAddress(t, payKey, stakeKey),
				1: uint64(2_000_000),
			},
		},
		2: uint64(100_000),
		4: []any{[]any{uint64(0), rowBatchingCred(stakeKey)}},
	})
	names["drep"] = newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(2)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, payKey, otherKey), 1: uint64(1_500_000),
		}},
		2: uint64(100_000),
		4: []any{[]any{
			uint64(16), rowBatchingCred(drepKey), uint64(500_000_000), anchor,
		}},
		20: []any{[]any{
			uint64(100_000_000), rowBatchingRewardAccount(stakeKey),
			[]any{uint64(6)}, anchor,
		}},
	})
	registerOut := names["register"].hash
	names["delegate"] = newRowBatchingTx(t, map[uint]any{
		0: []any{[]any{seed, uint64(3)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, payKey, otherKey), 1: uint64(1_200_000),
		}},
		2: uint64(100_000),
		4: []any{
			[]any{uint64(2), rowBatchingCred(stakeKey), rowBatchingKey(poolKey)},
			[]any{
				uint64(9), rowBatchingCred(stakeKey),
				[]any{uint64(0), rowBatchingKey(drepKey)},
			},
		},
	})
	sharedDatum := cbor.Tag{Number: 121, Content: []any{uint64(7)}}
	names["spend"] = newRowBatchingTxWith(t, map[uint]any{
		0: []any{[]any{registerOut, uint64(0)}},
		1: []any{
			map[uint]any{
				0: rowBatchingBaseAddress(t, payKey, stakeKey),
				1: uint64(2_500_000),
			},
			map[uint]any{
				0: rowBatchingBaseAddress(t, payKey, otherKey),
				1: uint64(400_000),
			},
		},
		2: uint64(100_000),
		5: map[cbor.ByteString]uint64{
			cbor.NewByteString(rowBatchingRewardAccount(stakeKey)): 0,
		},
		22: uint64(1_000),
	}, map[uint]any{
		0: []any{[]any{
			bytes.Repeat([]byte{0x66}, 32), bytes.Repeat([]byte{0x67}, 64),
		}},
		1: []any{[]any{uint64(0), rowBatchingKey(payKey)}},
		4: []any{sharedDatum},
	}, map[uint64]any{674: "spend"})
	names["chain"] = newRowBatchingTxWith(t, map[uint]any{
		0: []any{[]any{names["spend"].hash, uint64(1)}},
		1: []any{map[uint]any{
			0: rowBatchingBaseAddress(t, payKey, stakeKey), 1: uint64(300_000),
		}},
		2: uint64(100_000),
	}, map[uint]any{
		4: []any{sharedDatum},
	}, map[uint64]any{674: "chain"})
	b1 := rowBatchingBlock(
		t, 10, 1, lcommon.Blake2b256{},
		names["pool"], names["register"], names["drep"],
	)
	b2 := rowBatchingBlock(
		t, 20, 2, b1.Hash(), names["delegate"], names["spend"],
	)
	b3 := rowBatchingBlock(t, 30, 3, b2.Hash(), names["chain"])
	return []gledger.Block{b1, b2, b3}, names
}

type rowBatchingRun struct {
	db         *database.Database
	batchedTxs int
}

// runRowBatchingScenario applies blocks, grouped batchSize to a read result,
// into a fresh file-backed store with none of the blocks validated.
func runRowBatchingScenario(
	t *testing.T,
	blocks []gledger.Block,
	storageMode string,
	batching bool,
	batchSize int,
) *rowBatchingRun {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: storageMode,
	})
	require.NoError(t, err)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, cm.PrimaryChain().AddBlock(t.Context(), block, nil))
	}
	cfg := newTestShelleyGenesisCfg(t)
	cfg.ShelleyGenesis().NetworkId = "Testnet"
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:                db,
		ChainManager:            cm,
		CardanoNodeConfig:       cfg,
		Logger:                  testLogger(),
		PromRegistry:            prometheus.NewRegistry(),
		ManualBlockProcessing:   true,
		ApplyRowBatchingEnabled: batching,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	run := &rowBatchingRun{db: db}
	var txs atomic.Int64
	ls.afterBatchedTransactionWrite = func() { txs.Add(1) }
	nonce := bytes.Repeat([]byte{0x42}, 32)
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = &conway.ConwayProtocolParameters{
		KeyDeposit:       2_000_000,
		PoolDeposit:      500_000_000,
		DRepDeposit:      500_000_000,
		GovActionDeposit: 100_000_000,
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionPlomin,
		},
	}
	ls.currentEpoch = models.Epoch{
		SlotLength:    1_000,
		LengthInSlots: 1_000,
		EraId:         eras.ConwayEraDesc.Id,
		Nonce:         nonce,
		EvolvingNonce: nonce,
	}
	ls.epochCache = []models.Epoch{ls.currentEpoch}
	ls.currentTip = ochainsync.Tip{}
	ls.currentTipBlockNonce = nonce
	ls.publishSnapshotsLocked()
	require.NoError(t, cm.SetLedger(ls))

	batches := make(chan []gledger.Block, len(blocks))
	for i := 0; i < len(blocks); i += batchSize {
		batches <- blocks[i:min(i+batchSize, len(blocks))]
	}
	close(batches)
	require.NoError(t, ls.ProcessTrustedBlockBatches(t.Context(), batches))
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, blocks[len(blocks)-1].Hash().Bytes(), tip.Point.Hash)
	run.batchedTxs = int(txs.Load())
	return run
}

// rowBatchingDigest dumps every metadata table, minus surrogate keys and
// wall-clock columns, as sorted text rows. Surrogate ids are excluded because
// queued API detail rows receive theirs at flush time; every column that
// references another row's id is kept.
func rowBatchingDigest(t *testing.T, db *database.Database) string {
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
	for _, table := range tables {
		rows, err := raw.Query(fmt.Sprintf(`SELECT * FROM "%s"`, table))
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
				case "id", "created_at", "updated_at", "deleted_at",
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
			// The blob store's random identity differs between any two stores.
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

func rowBatchingQueryOne[T any](
	t *testing.T,
	raw *sql.DB,
	query string,
	args ...any,
) T {
	t.Helper()
	var v T
	require.NoError(t, raw.QueryRow(query, args...).Scan(&v))
	return v
}

// TestApplyRowBatchingSerialEquivalence applies the same unvalidated blocks
// into two stores, with the setting off and on, and requires identical stored
// state: tip, nonces, UTxOs and their assets, accounts, pools, DReps,
// governance and every API-mode detail table. In core mode the setting
// selects the batched path; API mode batches with it off as well.
func TestApplyRowBatchingSerialEquivalence(t *testing.T) {
	t.Parallel()

	blocks, names := rowBatchingScenario(t)
	totalTxs := len(names)
	for _, mode := range []string{types.StorageModeCore, types.StorageModeAPI} {
		for _, batchSize := range []int{len(blocks), 1} {
			t.Run(fmt.Sprintf("%s/batch%d", mode, batchSize), func(t *testing.T) {
				t.Parallel()
				off := runRowBatchingScenario(t, blocks, mode, false, batchSize)
				on := runRowBatchingScenario(t, blocks, mode, true, batchSize)
				offAgain := runRowBatchingScenario(t, blocks, mode, false, batchSize)

				if mode == types.StorageModeAPI {
					require.Equal(t, totalTxs, off.batchedTxs,
						"API mode batches with the flag off")
				} else {
					require.Zero(t, off.batchedTxs,
						"flag off must not use the batched path")
				}
				require.Equal(t, totalTxs, on.batchedTxs,
					"flag on must write every transaction of the "+
						"unvalidated blocks through the batched path")

				if mode == types.StorageModeAPI {
					// The queued tables must be populated, or the digest
					// comparison below says nothing about the flush.
					raw, err := dbtest.RawSQLiteMetadata(t, on.db)
					require.NoError(t, err)
					for _, table := range []string{
						"address_transaction", "key_witness",
						"witness_scripts", "plutus_data", "datum",
						"transaction_metadata_label",
					} {
						require.Positive(t, rowBatchingQueryOne[int](t, raw,
							`SELECT COUNT(*) FROM "`+table+`"`),
							"scenario must queue %s rows", table)
					}
					require.Equal(t, uint64(20), rowBatchingQueryOne[uint64](
						t, raw, `SELECT added_slot FROM datum`),
						"a datum shared within a chunk keeps its first slot")
				}

				offDigest := rowBatchingDigest(t, off.db)
				require.Equal(t, offDigest, rowBatchingDigest(t, offAgain.db),
					"the digest must be deterministic between identical runs")
				require.Equal(t, offDigest, rowBatchingDigest(t, on.db),
					"batched application changed the stored state")
			})
		}
	}
}

// TestApplyRowBatchingSameChunkDependencies pins the stored effects of a
// chunk in which a stake credential is registered, then delegated, and an
// output produced earlier in the chunk is spent, with the batched path on.
func TestApplyRowBatchingSameChunkDependencies(t *testing.T) {
	t.Parallel()

	blocks, names := rowBatchingScenario(t)
	for _, mode := range []string{types.StorageModeCore, types.StorageModeAPI} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			run := runRowBatchingScenario(t, blocks, mode, true, len(blocks))
			require.Positive(t, run.batchedTxs)
			raw, err := dbtest.RawSQLiteMetadata(t, run.db)
			require.NoError(t, err)

			stake := rowBatchingKey(0x22)
			active := rowBatchingQueryOne[bool](t, raw,
				`SELECT active FROM account WHERE staking_key = ?`, stake)
			require.True(t, active, "the registered credential is active")
			pool := rowBatchingQueryOne[[]byte](t, raw,
				`SELECT pool FROM account WHERE staking_key = ?`, stake)
			require.Equal(t, rowBatchingKey(0x11), pool,
				"the delegation must see the registration from the same chunk")
			drep := rowBatchingQueryOne[[]byte](t, raw,
				`SELECT drep FROM account WHERE staking_key = ?`, stake)
			require.Equal(t, rowBatchingKey(0x33), drep)

			spentAt := rowBatchingQueryOne[[]byte](t, raw,
				`SELECT spent_at_tx_id FROM utxo WHERE tx_id = ? AND output_idx = 0`,
				names["register"].hash)
			require.Equal(t, names["spend"].hash, spentAt,
				"the output produced earlier in the chunk is marked spent by its consumer")
			deleted := rowBatchingQueryOne[uint64](t, raw,
				`SELECT deleted_slot FROM utxo WHERE tx_id = ? AND output_idx = 0`,
				names["register"].hash)
			require.Equal(t, uint64(20), deleted)
			chained := rowBatchingQueryOne[[]byte](t, raw,
				`SELECT spent_at_tx_id FROM utxo WHERE tx_id = ? AND output_idx = 1`,
				names["spend"].hash)
			require.Equal(t, names["chain"].hash, chained)
			live := rowBatchingQueryOne[int](t, raw,
				`SELECT COUNT(*) FROM utxo WHERE tx_id = ? AND deleted_slot = 0`,
				names["chain"].hash)
			require.Equal(t, 1, live)

			assets := rowBatchingQueryOne[int](t, raw,
				`SELECT COUNT(*) FROM asset`)
			require.Positive(t, assets, "the native asset output is stored")
			pools := rowBatchingQueryOne[int](t, raw,
				`SELECT COUNT(*) FROM pool`)
			require.Equal(t, 1, pools)
			dreps := rowBatchingQueryOne[int](t, raw,
				`SELECT COUNT(*) FROM drep`)
			require.Equal(t, 1, dreps)
			proposals := rowBatchingQueryOne[int](t, raw,
				`SELECT COUNT(*) FROM governance_proposal`)
			require.Equal(t, 1, proposals)
		})
	}
}
