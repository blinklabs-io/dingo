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

package forging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// forgeLogLevels returns the level of every JSON log line whose message is
// msg.
func forgeLogLevels(t *testing.T, logs *bytes.Buffer, msg string) []string {
	t.Helper()
	var levels []string
	for line := range bytes.SplitSeq(logs.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		require.NoError(t, json.Unmarshal(line, &rec))
		if rec.Msg == msg {
			levels = append(levels, rec.Level)
		}
	}
	return levels
}

const (
	forgedValidationFailedMsg = "forged block failed self-validation, dropping block"
	forgedValidationStaleMsg  = "forge skip: ledger state kept changing during self-validation, dropping block"
)

// A validator that could not reach a verdict because the ledger state moved
// under it has not found the block invalid. The block is still dropped, but
// the slot is a skip -- a Warn, and no self-validation failure -- the way a
// build attempt that the chain moved under is.
func TestForgeSkipsABlockWhoseSelfValidationStateKeptChanging(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	validator := &forgerTestValidator{
		err: fmt.Errorf("tip moved: %w", ErrForgedBlockStateChanged),
	}
	forger, _ := newForgerWithValidator(t, block, nil, broadcaster, validator)
	var logs bytes.Buffer
	forger.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(t, 1, validator.calls)
	assert.Zero(
		t,
		broadcaster.calls,
		"a block without a verdict must not be adopted",
	)
	assert.Zero(t, testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	assert.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeValidationFailed),
		"a changed ledger state is not a self-validation failure",
	)
	assert.Empty(t, forgeLogLevels(t, &logs, forgedValidationFailedMsg))
	assert.Equal(
		t,
		[]string{"WARN"},
		forgeLogLevels(t, &logs, forgedValidationStaleMsg),
	)
}

// A genuine rule failure keeps its loud path: the block is dropped, the
// failure is counted, and it is logged at Error.
func TestForgeStillFailsLoudlyOnAGenuineSelfValidationFailure(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	validator := &forgerTestValidator{
		err: errors.New(
			"tx 00 in forged block at slot 10: TX 00 failed validation: BadInputsUTxO",
		),
	}
	forger, _ := newForgerWithValidator(t, block, nil, broadcaster, validator)
	var logs bytes.Buffer
	forger.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	err := forger.checkAndForgeProduction(context.Background())

	require.ErrorContains(t, err, "self-validation failed")
	require.NotErrorIs(t, err, ErrForgedBlockStateChanged)
	assert.Zero(t, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeValidationFailed),
	)
	assert.Equal(
		t,
		[]string{"ERROR"},
		forgeLogLevels(t, &logs, forgedValidationFailedMsg),
	)
	assert.Empty(t, forgeLogLevels(t, &logs, forgedValidationStaleMsg))
}

type cancellingForgerValidator struct {
	cancel context.CancelFunc
}

func (v *cancellingForgerValidator) ValidateForgedBlock(
	context.Context,
	ledger.Block,
	[]byte,
) error {
	v.cancel()
	return fmt.Errorf("tip moved: %w", ErrForgedBlockStateChanged)
}

// A forge cycle cancelled during self-validation is a shutdown, not a slot
// skipped for a moving ledger: the cancellation is returned, and no
// skip is logged.
func TestForgeReturnsCancellationDuringSelfValidationState(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	forger, _ := newForgerWithValidator(
		t,
		block,
		nil,
		broadcaster,
		&forgerTestValidator{},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	forger.blockValidator = &cancellingForgerValidator{cancel: cancel}
	var logs bytes.Buffer
	forger.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	err := forger.checkAndForgeProduction(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, broadcaster.calls)
	assert.Empty(t, forgeLogLevels(t, &logs, forgedValidationStaleMsg))
}
