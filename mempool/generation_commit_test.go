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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/utxoref"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

var errGenerationCommitTestInvalid = errors.New(
	"transaction is invalid in the published generation",
)

// generationCommitRaceValidator deterministically publishes generation 1
// after the first final current-generation read. A plain check then commit
// admits the generation-0 verdict after generation 1 is visible. The atomic
// provider rechecks while holding the same lock publication needs.
type generationCommitRaceValidator struct {
	mu             sync.RWMutex
	generation     uint64
	publishOnCheck atomic.Bool
}

func (v *generationCommitRaceValidator) ValidateTx(gledger.Transaction) error {
	return nil
}

func (v *generationCommitRaceValidator) ValidateTxWithOverlay(
	gledger.Transaction,
	map[utxoref.Key]struct{},
	map[utxoref.Key]common.Utxo,
	*utxoref.StateOverlay,
) error {
	return nil
}

func (v *generationCommitRaceValidator) snapshot() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.generation
}

func (v *generationCommitRaceValidator) session(
	fn func(
		func(
			gledger.Transaction,
			map[utxoref.Key]struct{},
			map[utxoref.Key]common.Utxo,
			*utxoref.StateOverlay,
		) error,
		func() bool,
		func(func() error) (bool, error),
	) error,
) error {
	snapshot := v.snapshot()
	validate := func(
		gledger.Transaction,
		map[utxoref.Key]struct{},
		map[utxoref.Key]common.Utxo,
		*utxoref.StateOverlay,
	) error {
		if snapshot > 0 {
			return errGenerationCommitTestInvalid
		}
		return nil
	}
	stillCurrent := func() bool {
		v.mu.RLock()
		current := v.generation == snapshot
		v.mu.RUnlock()
		if current && v.publishOnCheck.CompareAndSwap(true, false) {
			v.mu.Lock()
			v.generation++
			v.mu.Unlock()
		}
		return current
	}
	commitIfCurrent := func(commit func() error) (bool, error) {
		v.mu.RLock()
		defer v.mu.RUnlock()
		if v.generation != snapshot {
			return false, nil
		}
		return true, commit()
	}
	return fn(validate, stillCurrent, commitIfCurrent)
}

func (v *generationCommitRaceValidator) WithTxValidationSession(
	fn func(
		func(
			gledger.Transaction,
			map[utxoref.Key]struct{},
			map[utxoref.Key]common.Utxo,
			*utxoref.StateOverlay,
		) error,
		func() bool,
		func(func() error) (bool, error),
	) error,
) error {
	return v.session(fn)
}

func TestAdmissionCommitRejectsSupersededLedgerGeneration(t *testing.T) {
	t.Parallel()
	validator := &generationCommitRaceValidator{}
	pool := newTestMempoolWithValidator(t, validator)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Stop(ctx))
	})

	validator.publishOnCheck.Store(true)
	err := pool.AddTransaction(
		uint(conway.EraIdConway),
		getTestTxBytes(t),
	)
	require.ErrorIs(t, err, errGenerationCommitTestInvalid)
	require.Empty(t, pool.Transactions())
}

func TestRevalidationCommitRejectsSupersededLedgerGeneration(t *testing.T) {
	t.Parallel()
	validator := &generationCommitRaceValidator{}
	pool := newTestMempoolWithValidator(t, validator)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Stop(ctx))
	})

	require.NoError(t, pool.AddTransaction(
		uint(conway.EraIdConway),
		getTestTxBytes(t),
	))
	require.Len(t, pool.Transactions(), 1)

	validator.publishOnCheck.Store(true)
	require.NoError(t, pool.rebuildOverlay(context.Background()))
	require.Empty(t, pool.Transactions())
}

// alwaysMovingValidator models a ledger that publishes again before every
// commit, so no admission attempt can ever commit its verdict.
type alwaysMovingValidator struct {
	generationCommitRaceValidator
}

func (v *alwaysMovingValidator) WithTxValidationSession(
	fn func(
		func(
			gledger.Transaction,
			map[utxoref.Key]struct{},
			map[utxoref.Key]common.Utxo,
			*utxoref.StateOverlay,
		) error,
		func() bool,
		func(func() error) (bool, error),
	) error,
) error {
	return v.session(func(
		validate func(
			gledger.Transaction,
			map[utxoref.Key]struct{},
			map[utxoref.Key]common.Utxo,
			*utxoref.StateOverlay,
		) error,
		stillCurrent func() bool,
		_ func(func() error) (bool, error),
	) error {
		return fn(validate, stillCurrent, func(func() error) (bool, error) {
			return false, nil
		})
	})
}

// TestAdmissionReportsUnsettledLedgerAsPendingStateMoved pins that running
// out of reconcile attempts is reported as ErrPendingStateMoved, which API
// surfaces classify as unavailability rather than a ledger rejection.
func TestAdmissionReportsUnsettledLedgerAsPendingStateMoved(t *testing.T) {
	t.Parallel()
	pool := newTestMempoolWithValidator(t, &alwaysMovingValidator{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Stop(ctx))
	})

	err := pool.AddTransaction(uint(conway.EraIdConway), getTestTxBytes(t))
	require.ErrorIs(t, err, ErrPendingStateMoved)
	require.Empty(t, pool.Transactions())
}
