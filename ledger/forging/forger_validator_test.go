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
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// forgerTestValidator is a controllable BlockValidator stub.
type forgerTestValidator struct {
	err   error
	calls int
}

func (v *forgerTestValidator) ValidateForgedBlock(ledger.Block, []byte) error {
	v.calls++
	return v.err
}

func newForgerWithValidator(
	t *testing.T,
	block ledger.Block,
	blockCbor []byte,
	broadcaster *forgerTestBroadcaster,
	validator *forgerTestValidator,
) *BlockForger {
	t.Helper()
	creds := setupTestCredentials(t)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: blockCbor},
		BlockBroadcaster: broadcaster,
		BlockValidator:   validator,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestBlockValidatorPassesAllowsAdoption verifies that a passing validator
// does not prevent adoption: broadcaster is called and forgeAdopted increments.
func TestBlockValidatorPassesAllowsAdoption(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	validator := &forgerTestValidator{err: nil}

	forger := newForgerWithValidator(t, block, nil, broadcaster, validator)
	err := forger.checkAndForgeProduction(context.Background())

	require.NoError(t, err)
	assert.Equal(t, 1, validator.calls, "validator must be called once")
	assert.Equal(t, 1, broadcaster.calls, "block must be adopted after passing validation")
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(t, float64(0), testutil.ToFloat64(forger.metrics.forgeCouldNot))
}

// TestBlockValidatorFailureDropsBlock verifies that a failing validator
// prevents adoption: broadcaster is NOT called, couldNotForge increments.
func TestBlockValidatorFailureDropsBlock(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}
	validator := &forgerTestValidator{
		err: errors.New("header crypto: invalid KES signature"),
	}

	forger := newForgerWithValidator(t, block, nil, broadcaster, validator)
	err := forger.checkAndForgeProduction(context.Background())

	require.Error(t, err)
	require.ErrorContains(t, err, "self-validation failed")
	assert.Equal(t, 1, validator.calls, "validator must be called once")
	assert.Equal(t, 0, broadcaster.calls, "block must NOT be adopted after failed validation")
	assert.Equal(t, float64(0), testutil.ToFloat64(forger.metrics.forgeAdopted))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeCouldNot))
	assert.Equal(t, float64(1), testutil.ToFloat64(forger.metrics.forgeValidationFailed))
}

// TestNilBlockValidatorSkipsValidation confirms that the default nil validator
// does not affect normal forging: block is adopted without any validation call.
func TestNilBlockValidatorSkipsValidation(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	broadcaster := &forgerTestBroadcaster{}

	creds := setupTestCredentials(t)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block},
		BlockBroadcaster: broadcaster,
		// BlockValidator intentionally omitted (nil = disabled)
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	err = forger.checkAndForgeProduction(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, broadcaster.calls, "block must be adopted when validator is nil")
}

// TestBlockValidatorCalledBeforeBroadcaster verifies ordering: the validator
// runs before AddBlock. If the validator fails, AddBlock must never be called.
func TestBlockValidatorCalledBeforeBroadcaster(t *testing.T) {
	block := newForgerTestBlock(10, 2)

	var callOrder []string
	broadcaster := &forgerTestBroadcaster{}
	// Override the underlying AddBlock via a tracking broadcaster
	trackingBroadcaster := &trackingBroadcaster{
		inner: broadcaster,
		onAdd: func() { callOrder = append(callOrder, "broadcast") },
	}
	trackingValidator := &trackingBlockValidator{
		onValidate: func() error {
			callOrder = append(callOrder, "validate")
			return errors.New("intentional failure")
		},
	}

	creds := setupTestCredentials(t)
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      creds,
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block},
		BlockBroadcaster: trackingBroadcaster,
		BlockValidator:   trackingValidator,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	_ = forger.checkAndForgeProduction(context.Background())

	require.Equal(t, []string{"validate"}, callOrder,
		"validator must run before broadcaster; broadcaster must not run on failure")
}

// trackingBroadcaster wraps a broadcaster and calls a hook on AddBlock.
type trackingBroadcaster struct {
	inner BlockBroadcaster
	onAdd func()
}

func (b *trackingBroadcaster) AddBlock(block ledger.Block, cbor []byte) error {
	b.onAdd()
	return b.inner.AddBlock(block, cbor)
}

// trackingBlockValidator calls a hook on ValidateForgedBlock.
type trackingBlockValidator struct {
	onValidate func() error
}

func (v *trackingBlockValidator) ValidateForgedBlock(ledger.Block, []byte) error {
	return v.onValidate()
}
