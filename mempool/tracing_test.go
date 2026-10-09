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

package mempool

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Not t.Parallel: installs a recording OpenTelemetry provider, which is a
// process global.
func TestAddTransactionRecordsSpanWithValidationResult(t *testing.T) {
	spans := testutil.RecordSpans(t)
	m := newTestMempool(t)
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	txType := uint(conway.EraIdConway)

	txBytes := getTestTxBytes(t)
	validator := m.validator.(*mockValidator)

	validator.setFailAll(true)
	require.Error(t, m.AddTransaction(context.Background(), txType, txBytes))
	ended := spans.Ended()
	require.Len(t, ended, 1)
	require.Equal(t, "mempool.add_transaction", ended[0].Name())
	require.Contains(
		t,
		ended[0].Attributes(),
		attribute.String("mempool.validation_result", "rejected"),
	)
	require.Equal(t, codes.Error, ended[0].Status().Code)

	validator.setFailAll(false)
	require.NoError(t, m.AddTransaction(context.Background(), txType, txBytes))
	ended = spans.Ended()
	require.Len(t, ended, 2)
	require.Contains(
		t,
		ended[1].Attributes(),
		attribute.String("mempool.validation_result", "accepted"),
	)
	require.Equal(t, codes.Unset, ended[1].Status().Code)

	m.validator = nil
	require.ErrorIs(
		t,
		m.AddTransaction(context.Background(), txType, txBytes),
		ErrNilValidator,
	)
	ended = spans.Ended()
	require.Len(t, ended, 3)
	require.Contains(
		t,
		ended[2].Attributes(),
		attribute.String("mempool.validation_result", "unavailable"),
	)
}
