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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/utxoref"
)

// The validation session's applied predicate reads the LedgerDB's transaction
// index: an applied transaction is reported and an unknown hash is not.
func TestTxValidationSessionIsAppliedReadsStore(t *testing.T) {
	t.Parallel()

	ls := mainnetForecastLedgerState(t, eras.AlonzoEraDesc)
	ls.db = newTestDB(t)
	attachTestLedgerDB(t, ls)
	appliedHash := txIndexTestHash(0x93)
	commitTxIndexBlock(t, ls, 100, appliedHash)
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentEpoch = ls.epochCache[297]
	ls.currentPParams = &alonzo.AlonzoProtocolParameters{ProtocolMajor: 6}
	ls.publishSnapshotsLocked()

	ran := false
	require.NoError(t, ls.WithResolvingTxValidationSession(
		context.Background(),
		func(
			_ func(
				lcommon.Transaction,
				map[utxoref.Key]struct{},
				map[utxoref.Key]lcommon.Utxo,
				*utxoref.StateOverlay,
			) error,
			_ func() bool,
			_ func(func() error) (bool, error),
			_ func([]models.UtxoId) ([]models.Utxo, error),
			isApplied func([]byte) (bool, error),
		) error {
			ran = true
			applied, err := isApplied(appliedHash)
			require.NoError(t, err)
			require.True(t, applied, "an applied transaction")
			applied, err = isApplied(make([]byte, 32))
			require.NoError(t, err)
			require.False(t, applied, "an unknown hash")
			return nil
		},
	))
	require.True(t, ran)
}
