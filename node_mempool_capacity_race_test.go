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
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/stretchr/testify/require"
)

// The mempool's capacity function and the forge gate run on their own
// goroutines while a live restore replaces the node's ledger. They read the
// ledger published for such readers, never the field the restore rewrites, so
// a ledger cleared mid-call is neither raced nor dereferenced.
func TestLeiosQuorumReadsSurviveLedgerReplacement(t *testing.T) {
	t.Parallel()
	ls, _ := newSigmaDenominatorLedger(t)
	voteManager, err := leios.NewVoteManager(leios.VoteManagerConfig{
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		EventBus:       event.NewEventBus(nil, nil),
		StakeProvider:  startupLeiosStakeProvider{},
		EpochProvider:  startupLeiosEpochProvider{},
		ParamsProvider: startupLeiosParamsProvider{},
		KeyProvider:    startupLeiosKeyProvider{},
	})
	require.NoError(t, err)
	n := &Node{}
	n.leiosCertificateVerifier.Store(voteManager)
	n.setLedgerState(ls)
	n.publishStartedLedgerState()

	const rounds = 200
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range rounds {
			n.setLedgerState(nil)
			n.setLedgerState(ls)
			n.publishStartedLedgerState()
		}
	}()
	for range rounds {
		require.False(t, n.leiosQuorumPossible())
		_ = n.leiosEndorserBlockQuorumRefusalForSlot(100)
	}
	wg.Wait()
}
