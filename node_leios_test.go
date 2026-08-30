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

package dingo

import (
	"context"
	"io"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type startupLeiosStakeProvider struct{}

func (startupLeiosStakeProvider) GetStakeDistribution(
	uint64,
) (map[string]uint64, uint64, error) {
	return map[string]uint64{}, 0, nil
}

type startupLeiosEpochProvider struct{}

func (startupLeiosEpochProvider) CurrentEpoch() uint64 {
	return 5
}

func (startupLeiosEpochProvider) EpochForSlot(uint64) (uint64, error) {
	return 5, nil
}

type startupLeiosParamsProvider struct{}

func (startupLeiosParamsProvider) LeiosCommitteeParameters() (
	*big.Rat,
	*big.Rat,
	error,
) {
	return big.NewRat(99, 100), big.NewRat(3, 4), nil
}

type startupLeiosKeyProvider struct{}

func (startupLeiosKeyProvider) GetLeiosKeys(
	uint64,
	[]string,
) (map[string]*lcommon.LeiosKey, error) {
	return map[string]*lcommon.LeiosKey{}, nil
}

// TestLeiosCommitteeParamsFromPParamsDefaultsWhenBothUnset covers the issue
// #2836 root cause: musashi ships a refScript-only dijkstra genesis, so
// neither committee stake coverage (sigma_c) nor quorum stake threshold
// (tau) is configured. The genesis is immutable, so the adapter must fall
// back to the CIP-0164 defaults (0.99 / 0.75) rather than erroring or
// returning nil, which is what lets committee formation and certification
// proceed.
func TestLeiosCommitteeParamsFromPParamsDefaultsWhenBothUnset(t *testing.T) {
	pp := &gdijkstra.DijkstraProtocolParameters{}
	sigmaC, tau, err := leiosCommitteeParamsFromPParams(pp)
	require.NoError(t, err)
	require.NotNil(t, sigmaC)
	require.NotNil(t, tau)
	assert.Equal(t, 0, sigmaC.Cmp(big.NewRat(99, 100)))
	assert.Equal(t, 0, tau.Cmp(big.NewRat(3, 4)))
}

// TestLeiosCommitteeParamsFromPParamsDefaultsMissingCoverage confirms a
// configured tau is preserved while an unset sigma_c falls back to its
// default (tau=1/2 < default sigma_c=0.99 holds).
func TestLeiosCommitteeParamsFromPParamsDefaultsMissingCoverage(t *testing.T) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		QuorumStakeThreshold: &cbor.Rat{Rat: big.NewRat(1, 2)},
	}
	sigmaC, tau, err := leiosCommitteeParamsFromPParams(pp)
	require.NoError(t, err)
	require.NotNil(t, sigmaC)
	require.NotNil(t, tau)
	assert.Equal(t, 0, sigmaC.Cmp(big.NewRat(99, 100)))
	assert.Equal(t, 0, tau.Cmp(big.NewRat(1, 2)))
}

// TestLeiosCommitteeParamsFromPParamsDefaultsMissingQuorum confirms a
// configured sigma_c is preserved while an unset tau falls back to its
// default (default tau=0.75 < sigma_c=0.99 holds).
func TestLeiosCommitteeParamsFromPParamsDefaultsMissingQuorum(t *testing.T) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		CommitteeStakeCoverage: &cbor.Rat{Rat: big.NewRat(99, 100)},
	}
	sigmaC, tau, err := leiosCommitteeParamsFromPParams(pp)
	require.NoError(t, err)
	require.NotNil(t, sigmaC)
	require.NotNil(t, tau)
	assert.Equal(t, 0, sigmaC.Cmp(big.NewRat(99, 100)))
	assert.Equal(t, 0, tau.Cmp(big.NewRat(3, 4)))
}

// TestLeiosCommitteeParamsFromPParamsReturnsBothWhenConfigured confirms a
// fully configured genesis flows both values through unchanged (no default
// applied).
func TestLeiosCommitteeParamsFromPParamsReturnsBothWhenConfigured(
	t *testing.T,
) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		CommitteeStakeCoverage: &cbor.Rat{Rat: big.NewRat(95, 100)},
		QuorumStakeThreshold:   &cbor.Rat{Rat: big.NewRat(3, 5)},
	}
	sigmaC, tau, err := leiosCommitteeParamsFromPParams(pp)
	require.NoError(t, err)
	require.NotNil(t, sigmaC)
	require.NotNil(t, tau)
	assert.Equal(t, 0, sigmaC.Cmp(big.NewRat(95, 100)))
	assert.Equal(t, 0, tau.Cmp(big.NewRat(3, 5)))
}

// TestLeiosCommitteeParamsFromPParamsRejectsInvariantViolation confirms the
// tau < sigma_c invariant is enforced when both are configured.
func TestLeiosCommitteeParamsFromPParamsRejectsInvariantViolation(
	t *testing.T,
) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		CommitteeStakeCoverage: &cbor.Rat{Rat: big.NewRat(3, 4)},
		QuorumStakeThreshold:   &cbor.Rat{Rat: big.NewRat(3, 4)},
	}
	_, _, err := leiosCommitteeParamsFromPParams(pp)
	require.Error(t, err)
}

// TestLeiosCommitteeParamsFromPParamsRejectsDefaultInvariantViolation
// confirms the post-default re-check: a configured sigma_c below the default
// tau (0.75) with tau unset would otherwise yield tau >= sigma_c.
func TestLeiosCommitteeParamsFromPParamsRejectsDefaultInvariantViolation(
	t *testing.T,
) {
	pp := &gdijkstra.DijkstraProtocolParameters{
		CommitteeStakeCoverage: &cbor.Rat{Rat: big.NewRat(1, 2)},
	}
	_, _, err := leiosCommitteeParamsFromPParams(pp)
	require.Error(t, err)
}

func TestEnableLeiosVotingDefersUntilOnChainKeyAvailable(t *testing.T) {
	vrfPath, kesPath, opcertPath := devnetCredPaths(t)
	creds := forging.NewPoolCredentials()
	require.NoError(
		t,
		creds.LoadFromFiles(vrfPath, kesPath, opcertPath),
	)

	voteKeyPath := filepath.Join(t.TempDir(), "leios-vote.skey")
	require.NoError(
		t,
		os.WriteFile(
			voteKeyPath,
			[]byte(
				"0000000000000000000000000000000000000000000000000000000000000001",
			),
			0o600,
		),
	)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	voteManager, err := leios.NewVoteManager(leios.VoteManagerConfig{
		Logger:         logger,
		EventBus:       eventBus,
		StakeProvider:  startupLeiosStakeProvider{},
		EpochProvider:  startupLeiosEpochProvider{},
		ParamsProvider: startupLeiosParamsProvider{},
		KeyProvider:    startupLeiosKeyProvider{},
	})
	require.NoError(t, err)

	n := &Node{
		config: Config{
			logger:                  logger,
			blockProducer:           true,
			leiosVoteSigningKeyFile: voteKeyPath,
		},
		leiosVoteManager: voteManager,
	}

	require.NoError(
		t,
		n.enableLeiosVoting(creds),
		"startup must continue while the on-chain registration is behind the local tip",
	)
}

// TestInitLeiosVoteManagerUnsubscribesAcrossLiveLifecycleCycles guards a
// real bug: initLeiosVoteManager's VoteEmittedEventType subscription used
// to discard its subscriber ID, and this function runs again on every live
// database Restore/Truncate reinit for a Dijkstra/Leios-enabled node — but
// the EventBus itself is never recreated across that cycle. Without
// unsubscribing the previous cycle's handler first (mirroring the three
// other Node subscriber-ID fields this exact quiesce function already
// tracks for the identical reason), each cycle left one more permanently
// active subscription behind, so a single emitted vote got enqueued (and
// would be diffused to peers) once per accumulated cycle instead of once.
//
// Runs initLeiosVoteManager, then the real quiesceForLiveLifecycleOp
// (which now unsubscribes leiosVoteEmittedSubId), three times in a row —
// simulating three live Restore/Truncate cycles — then publishes exactly
// one VoteEmittedEvent and asserts exactly one vote is queued for
// diffusion, not three.
func TestInitLeiosVoteManagerUnsubscribesAcrossLiveLifecycleCycles(
	t *testing.T,
) {
	n, _ := newLiveLifecycleTestNode(t, 1)

	const cycles = 3
	for range cycles {
		require.NoError(t, n.initLeiosVoteManager(context.Background()))
		require.NoError(t, n.quiesceForLiveLifecycleOp(context.Background()))
	}
	// The last cycle's quiesce stopped leiosVoteManager without a
	// subsequent reinit rebuilding it; re-create it once more so a
	// handler is actually live to receive the event published below,
	// matching the shape of a real live-lifecycle op (quiesce always
	// pairs with a reinit that calls initLeiosVoteManager again).
	require.NoError(t, n.initLeiosVoteManager(context.Background()))

	require.Zero(t, n.ouroboros().LeiosVoteEnqueueCount())
	n.eventBus.Publish(leios.VoteEmittedEventType, event.NewEvent(
		leios.VoteEmittedEventType,
		leios.VoteEmittedEvent{Vote: lcommon.LeiosPrototypeVote{}},
	))

	// A single require.Eventually asserting the exact count (not >= 1)
	// both waits for delivery and stays red if a stale, over-counted
	// subscription pushes the count past 1: EventBus dispatches every
	// live subscriber for one Publish call around the same time, so if a
	// duplicate delivery were going to happen, it already would have by
	// the time any poll first observes the count reaching 1 -- no
	// additional settle-time sleep is needed to catch it.
	require.Eventually(t, func() bool {
		return n.ouroboros().LeiosVoteEnqueueCount() == 1
	}, 2*time.Second, 10*time.Millisecond,
		"exactly one vote must be enqueued for the single published event, "+
			"not once per accumulated live-lifecycle cycle")
}
