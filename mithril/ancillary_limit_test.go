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

package mithril

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	cardanocfg "github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unboundedSlotLimit() (uint64, error) { return math.MaxUint64, nil }

func fixedSlotLimit(limit uint64) slotLimitFunc {
	return func() (uint64, error) { return limit, nil }
}

func shelleyNodeConfig(t *testing.T, securityParam int) *cardanocfg.CardanoNodeConfig {
	t.Helper()
	cfg := &cardanocfg.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(
		`{"activeSlotsCoeff":0.05,"securityParam":`+strconv.Itoa(securityParam)+`}`,
	)))
	return cfg
}

func verifiedAncillaryAt(t *testing.T, slot string) *BootstrapResult {
	t.Helper()
	anc := t.TempDir()
	slotDir := filepath.Join(anc, "ledger", slot)
	require.NoError(t, os.MkdirAll(slotDir, 0o750))
	stateBytes := []byte("newer than certified")
	require.NoError(t, os.WriteFile(
		filepath.Join(slotDir, "state"), stateBytes, 0o640,
	))
	ancRoot, err := openVerifiedDir(anc)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ancRoot.Close() })
	sum := sha256.Sum256(stateBytes)
	return &BootstrapResult{
		AncillaryDir:      anc,
		AncillaryRoot:     ancRoot,
		AncillaryVerified: true,
		AncillaryDigests: map[string]string{
			"ledger/" + slot + "/state": hex.EncodeToString(sum[:]),
		},
	}
}

func TestSelectLedgerStateSnapshotRefusesStateBeyondSlotLimit(t *testing.T) {
	t.Parallel()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("above limit", func(t *testing.T) {
		t.Parallel()
		_, _, _, _, err := selectLedgerStateSnapshot(
			discard, verifiedAncillaryAt(t, "300"), 100, fixedSlotLimit(299),
		)
		require.Error(t, err)
		assert.ErrorContains(t, err, "exceeds artifact-relative limit 299")
	})
	t.Run("at limit", func(t *testing.T) {
		t.Parallel()
		snap, _, _, beyond, err := selectLedgerStateSnapshot(
			discard, verifiedAncillaryAt(t, "300"), 100, fixedSlotLimit(300),
		)
		require.NoError(t, err)
		defer snap.Close()
		assert.True(t, beyond)
	})
	t.Run("limit unresolvable", func(t *testing.T) {
		t.Parallel()
		_, _, _, _, err := selectLedgerStateSnapshot(
			discard, verifiedAncillaryAt(t, "300"), 100,
			func() (uint64, error) { return 0, io.ErrUnexpectedEOF },
		)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})
	t.Run("limit missing", func(t *testing.T) {
		t.Parallel()
		_, _, _, _, err := selectLedgerStateSnapshot(
			discard, verifiedAncillaryAt(t, "300"), 100, nil,
		)
		require.Error(t, err)
		assert.ErrorContains(t, err, "slot limit is required")
	})
}

func TestPrepareLedgerStateImportRefusesStateBeyondSlotLimit(t *testing.T) {
	t.Parallel()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	prepared, err := prepareLedgerStateImport(
		discard, verifiedAncillaryAt(t, "300"), 100, fixedSlotLimit(299),
	)
	require.Error(t, err)
	assert.Nil(t, prepared)
	assert.ErrorContains(t, err, "exceeds artifact-relative limit 299")
}

func TestAncillarySlotLimit(t *testing.T) {
	t.Parallel()

	t.Run("adds one stability window", func(t *testing.T) {
		t.Parallel()
		// 3k/f = 3*100/0.05
		got, err := ancillarySlotLimit(shelleyNodeConfig(t, 100), 1000)
		require.NoError(t, err)
		assert.Equal(t, uint64(1000+6000), got)
	})
	t.Run("missing genesis", func(t *testing.T) {
		t.Parallel()
		_, err := ancillarySlotLimit(&cardanocfg.CardanoNodeConfig{}, 1)
		assert.ErrorContains(t, err, "requires Shelley genesis")
		_, err = ancillarySlotLimit(nil, 1)
		assert.ErrorContains(t, err, "requires Shelley genesis")
	})
	t.Run("overflow", func(t *testing.T) {
		t.Parallel()
		_, err := ancillarySlotLimit(
			shelleyNodeConfig(t, 100), math.MaxUint64,
		)
		assert.ErrorContains(t, err, "overflows uint64")
	})
}

func TestAncillaryBlockLimit(t *testing.T) {
	t.Parallel()

	t.Run("twice the security parameter", func(t *testing.T) {
		t.Parallel()
		got, err := ancillaryBlockLimit(shelleyNodeConfig(t, 2160))
		require.NoError(t, err)
		assert.Equal(t, 4320, got)
	})
	t.Run("missing genesis", func(t *testing.T) {
		t.Parallel()
		_, err := ancillaryBlockLimit(&cardanocfg.CardanoNodeConfig{})
		assert.ErrorContains(t, err, "requires Shelley genesis")
	})
	t.Run("zero parameter", func(t *testing.T) {
		t.Parallel()
		_, err := ancillaryBlockLimit(shelleyNodeConfig(t, 0))
		assert.ErrorContains(t, err, "security parameter is zero")
	})
	t.Run("overflow", func(t *testing.T) {
		t.Parallel()
		_, err := ancillaryBlockLimit(shelleyNodeConfig(t, math.MaxInt/2+1))
		assert.ErrorContains(t, err, "overflows int")
	})
}

func TestGapBlockCollectorStopsAtLimit(t *testing.T) {
	t.Parallel()

	c := &gapBlockCollector{
		start:     ocommon.NewPoint(10, nil),
		maxBlocks: 2,
	}
	mk := func(slot uint64) *babbage.BabbageBlock {
		return &babbage.BabbageBlock{
			BlockHeader: &babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{Slot: slot},
			},
		}
	}
	require.NoError(t, c.add(1, mk(10))) // at start: skipped, not counted
	require.NoError(t, c.add(1, mk(11)))
	require.NoError(t, c.add(1, mk(12)))
	err := c.add(1, mk(13))
	require.Error(t, err)
	assert.ErrorContains(t, err, "volatile gap exceeds 2 blocks")
	assert.Len(t, c.blocks, 2)

}
