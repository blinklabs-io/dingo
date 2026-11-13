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
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// ShadowScenario is the result of RunShadowScenarioForTest.
type ShadowScenario struct {
	// Points are the applied blocks' points, in chain order.
	Points []ocommon.Point
	// Rollback rolls the ledger back to point.
	Rollback func(point ocommon.Point) error
	// Resume is the ledger's ResumeUtxoSetFrom.
	Resume func(ctx context.Context, point ocommon.Point) (bool, error)
	// UtxoSetReads reports the verify-mode reads and how many of them
	// differed from the metadata store.
	UtxoSetReads func() (verified, mismatches float64)
	// ReadsForValidation resolves refs the way validation's block
	// prefetch does, outside any transaction.
	ReadsForValidation func(refs []models.UtxoId) ([]models.Utxo, error)
	// UtxosByAddress and UtxoIncludingSpent are the ledger's lookups.
	UtxosByAddress     func(addrs []gledger.Address) ([]models.Utxo, error)
	UtxoIncludingSpent func(txID []byte, index uint32) (*models.Utxo, error)
	// UtxoWhole answers LocalStateQuery GetUTxOWhole as of slot (zero for
	// live) and returns its encoded reply.
	UtxoWhole func(slot uint64, hash []byte) ([]byte, error)
	// UtxosByAddressAt and UtxosByTxInAt answer LocalStateQuery
	// GetUTxOByAddress and GetUTxOByTxIn as of point and return the
	// reply's outputs, CBOR-encoded, by "txid#index".
	UtxosByAddressAt func(point ocommon.Point, addrs []gledger.Address) ([]models.Utxo, error)
	UtxosByTxInAt    func(point ocommon.Point, refs []models.UtxoId) (map[string][]byte, error)
}

// RunShadowScenarioForTest applies the external-ingest blocks, which include
// an invalid transaction whose collateral return a later block spends, with
// bus as the ledger's event bus. attach runs on the database before any block
// is applied.
func RunShadowScenarioForTest(
	t *testing.T,
	storageMode string,
	bus *event.EventBus,
	utxoSet UtxoSetWriter,
	attach func(*database.Database),
) ShadowScenario {
	return RunUtxoSetScenarioForTest(
		t, storageMode, bus, utxoSet, UtxoSetReadsOff, attach,
	)
}

// RunUtxoSetScenarioForTest is RunShadowScenarioForTest with validation
// reading its inputs as reads selects.
func RunUtxoSetScenarioForTest(
	t *testing.T,
	storageMode string,
	bus *event.EventBus,
	utxoSet UtxoSetWriter,
	reads string,
	attach func(*database.Database),
) ShadowScenario {
	t.Helper()
	return runUtxoSetScenario(
		t,
		storageMode,
		bus,
		utxoSet,
		reads,
		false,
		attach,
	)
}

func runUtxoSetScenario(
	t *testing.T,
	storageMode string,
	bus *event.EventBus,
	utxoSet UtxoSetWriter,
	reads string,
	setOnly bool,
	attach func(*database.Database),
) ShadowScenario {
	t.Helper()
	blocks, _ := externalIngestBlocks(t, 0)
	run := runRowBatchingScenario(
		t,
		blocks,
		storageMode,
		true,
		2,
		func(ls *LedgerState) {
			ls.config.EventBus = bus
			ls.config.UtxoSet = utxoSet
			ls.config.UtxoSetReads = reads
			if setOnly {
				require.NoError(t, ls.EnableUtxoSetOnly())
			}
			attach(ls.db)
		},
	)
	points := make([]ocommon.Point, 0, len(blocks))
	for _, b := range blocks {
		points = append(points, blockPoint(b))
	}
	return ShadowScenario{
		Points: points,
		Rollback: func(point ocommon.Point) error {
			return run.ls.rollback(t.Context(), point)
		},
		Resume: run.ls.ResumeUtxoSetFrom,
		UtxoWhole: func(slot uint64, hash []byte) ([]byte, error) {
			res, err := run.ls.queryShelleyUtxoWhole(
				t.Context(), QueryPoint{Slot: slot, Hash: hash}, nil,
			)
			if err != nil {
				return nil, err
			}
			return res.(*utxoWholeResult).encoded, nil
		},
		UtxosByAddress: func(addrs []gledger.Address) ([]models.Utxo, error) {
			return run.ls.UtxosByAddress(t.Context(), addrs)
		},
		UtxosByAddressAt: func(p ocommon.Point, addrs []gledger.Address) ([]models.Utxo, error) {
			res, err := run.ls.queryShelleyUtxoByAddress(
				t.Context(), addrs, QueryPoint{Slot: p.Slot, Hash: p.Hash}, nil,
			)
			if err != nil {
				return nil, err
			}
			return lsqReplyUtxos(res)
		},
		UtxosByTxInAt: func(p ocommon.Point, refs []models.UtxoId) (map[string][]byte, error) {
			ins := make([]gledger.ShelleyTransactionInput, 0, len(refs))
			for _, r := range refs {
				ins = append(ins, gledger.MustNewShelleyTransactionInput(
					hex.EncodeToString(r.Hash), int(r.Idx),
				))
			}
			res, err := run.ls.queryShelleyUtxoByTxIn(
				t.Context(), ins, QueryPoint{Slot: p.Slot, Hash: p.Hash}, nil,
			)
			if err != nil {
				return nil, err
			}
			utxos, err := lsqReplyUtxos(res)
			if err != nil {
				return nil, err
			}
			ret := make(map[string][]byte, len(utxos))
			for _, u := range utxos {
				ret[fmt.Sprintf("%x#%d", u.TxId, u.OutputIdx)] = u.Cbor
			}
			return ret, nil
		},
		UtxoIncludingSpent: func(txID []byte, index uint32) (*models.Utxo, error) {
			return run.ls.UtxoByRefIncludingSpent(txID, index)
		},
		ReadsForValidation: func(refs []models.UtxoId) ([]models.Utxo, error) {
			return run.ls.utxosByRefsForValidation(t.Context(), refs, nil)
		},
		UtxoSetReads: func() (float64, float64) {
			return promtestutil.ToFloat64(run.ls.metrics.utxoSetReadsVerified),
				promtestutil.ToFloat64(run.ls.metrics.utxoSetReadMismatches)
		},
	}
}

// RunUtxoSetOnlyScenarioForTest is RunShadowScenarioForTest in core mode with
// utxoSet as the only copy of the live UTxO set.
func RunUtxoSetOnlyScenarioForTest(
	t *testing.T,
	utxoSet UtxoSetWriter,
	attach func(*database.Database),
) ShadowScenario {
	t.Helper()
	return runUtxoSetScenario(
		t,
		types.StorageModeCore,
		nil,
		utxoSet,
		UtxoSetReadsOff,
		true,
		attach,
	)
}

// MetadataDigestOutsideUtxoSetForTest digests the metadata store without what
// UtxoSetOnly moves to the live UTxO set: the utxo table and the tables keyed
// by its rows.
func MetadataDigestOutsideUtxoSetForTest(
	t *testing.T,
	db *database.Database,
) string {
	t.Helper()
	return metadataDigest(
		t,
		db,
		map[string]bool{
			"utxo":                  true,
			"asset":                 true,
			"utxo_pointer":          true,
			"utxo_collateral_input": true,
			"utxo_reference_input":  true,
			"utxo_address":          true,
			"utxo_reference_script": true,
		},
		nil,
		nil,
	)
}

// lsqReplyUtxos unpacks a GetUTxOByAddress or GetUTxOByTxIn reply into its
// outputs, each CBOR-encoded.
func lsqReplyUtxos(res any) ([]models.Utxo, error) {
	m := res.([]any)[0].(map[olocalstatequery.UtxoId]gledger.TransactionOutput)
	ret := make([]models.Utxo, 0, len(m))
	for id, out := range m {
		c, err := cbor.Encode(out)
		if err != nil {
			return nil, err
		}
		ret = append(ret, models.Utxo{
			TxId: id.Hash.Bytes(), OutputIdx: uint32(id.Idx), Cbor: c, //nolint:gosec // an output index
		})
	}
	return ret, nil
}
