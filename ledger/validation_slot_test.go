package ledger

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestValidateTxRejectsInvalidHereafterAtActiveConwaySlot(t *testing.T) {
	const validationSlot = 200

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: &cardano.CardanoNodeConfig{},
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	ls.Lock()
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = &conway.ConwayProtocolParameters{}
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(validationSlot, []byte("tip")),
	}
	ls.publishSnapshotsLocked()
	ls.Unlock()

	tx := &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{Ttl: validationSlot},
	}
	err = ls.ValidateTx(tx)
	var invalidHereafterErr eras.InvalidHereafterError
	require.ErrorAs(t, err, &invalidHereafterErr)
}

func TestValidationReferenceSlotPrefersCurrentSlotWhenAhead(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(100, 125, nil)
	if got != 125 {
		t.Fatalf("expected current slot 125, got %d", got)
	}
}

func TestValidationReferenceSlotKeepsCurrentWhenEqual(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(125, 125, nil)
	if got != 125 {
		t.Fatalf("expected shared slot 125, got %d", got)
	}
}

func TestValidationReferenceSlotFallsBackToTipOnError(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(100, 125, errors.New("clock unavailable"))
	if got != 100 {
		t.Fatalf("expected tip slot 100 on error, got %d", got)
	}
}

func TestValidationReferenceSlotKeepsTipWhenAhead(t *testing.T) {
	t.Parallel()

	got := validationReferenceSlot(125, 100, nil)
	if got != 125 {
		t.Fatalf("expected tip slot 125, got %d", got)
	}
}

func TestHistoricalBlockValidationSkipsMithrilCoveredBlocks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		validationEnabled bool
		trustedReplay     bool
		chainsyncState    ChainsyncState
		blockSlot         uint64
		cutoffSlot        uint64
		mithrilLedgerSlot uint64
		shouldValidate    bool
		reachedTipRegion  bool
	}{
		{
			name:              "historical validation inside Mithril boundary",
			validationEnabled: true,
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         100,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			reachedTipRegion:  true,
		},
		{
			name:              "historical validation outside Mithril boundary",
			validationEnabled: true,
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         101,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			shouldValidate:    true,
			reachedTipRegion:  true,
		},
		{
			name:              "tip window inside Mithril boundary",
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         100,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			reachedTipRegion:  true,
		},
		{
			name:              "tip window outside Mithril boundary",
			chainsyncState:    SyncingChainsyncState,
			blockSlot:         101,
			cutoffSlot:        50,
			mithrilLedgerSlot: 100,
			shouldValidate:    true,
			reachedTipRegion:  true,
		},
		{
			name:           "trusted replay",
			trustedReplay:  true,
			chainsyncState: SyncingChainsyncState,
			blockSlot:      100,
			cutoffSlot:     50,
		},
		{
			name:           "before tip window",
			chainsyncState: SyncingChainsyncState,
			blockSlot:      49,
			cutoffSlot:     50,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			shouldValidate, reachedTipRegion := historicalBlockValidationDecision(
				testCase.validationEnabled,
				testCase.trustedReplay,
				testCase.chainsyncState,
				testCase.blockSlot,
				testCase.cutoffSlot,
				testCase.mithrilLedgerSlot,
			)
			require.Equal(t, testCase.shouldValidate, shouldValidate)
			require.Equal(t, testCase.reachedTipRegion, reachedTipRegion)
		})
	}
}
