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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func spanAttributes(
	span sdktrace.ReadOnlySpan,
) map[attribute.Key]attribute.Value {
	attrs := map[attribute.Key]attribute.Value{}
	for _, kv := range span.Attributes() {
		attrs[kv.Key] = kv.Value
	}
	return attrs
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestLedgerProcessBlockRecordsSpan(t *testing.T) {
	fx := loadUtxoMemoPreprodFixture(t)
	spans := testutil.RecordSpans(t)

	require.NoError(t, processFixtureBlock(t, fx, []lcommon.Transaction{fx.tx}))

	var found []sdktrace.ReadOnlySpan
	for _, span := range spans.Ended() {
		if span.Name() == "ledger.process_block" {
			found = append(found, span)
		}
	}
	require.Len(t, found, 1)
	attrs := spanAttributes(found[0])
	require.Equal(t, int64(fx.blockSlot), attrs["block.slot"].AsInt64())
	require.NotEmpty(t, attrs["block.hash"].AsString())
}

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestProcessEpochRolloverRecordsSpan(t *testing.T) {
	spans := testutil.RecordSpans(t)
	cfg := dijkstraRetentionNodeConfig(t)
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	currentEpoch := models.Epoch{
		EpochId:       5,
		StartSlot:     500,
		SlotLength:    1_000,
		LengthInSlots: 100,
		EraId:         eras.DijkstraEraDesc.Id,
	}
	require.NoError(t, db.SetEpoch(
		currentEpoch.StartSlot, currentEpoch.EpochId,
		nil, nil, nil, nil,
		currentEpoch.EraId, currentEpoch.SlotLength, currentEpoch.LengthInSlots,
		nil,
	))
	pparams := dijkstraRetentionPParams()
	ls := &LedgerState{
		db:             db,
		currentEra:     eras.DijkstraEraDesc,
		activeEras:     eras.ErasWithDijkstra,
		currentEpoch:   currentEpoch,
		currentPParams: pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	require.NoError(t, db.Transaction(context.Background(), true).Do(func(txn *database.Txn) error {
		_, err := ls.processEpochRollover(
			context.Background(),
			txn,
			currentEpoch,
			eras.DijkstraEraDesc,
			pparams,
			false,
		)
		return err
	}))

	ended := spans.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, "ledger.epoch_transition", ended[0].Name())
	require.Equal(
		t,
		int64(currentEpoch.EpochId),
		spanAttributes(ended[0])["epoch"].AsInt64(),
	)
}
