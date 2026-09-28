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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/leios"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type occurrencePipelineClock struct{}

func (occurrencePipelineClock) CurrentOrTipSlot() uint64 { return 101 }

func (occurrencePipelineClock) CurrentEpoch() uint64 { return 1 }

func (occurrencePipelineClock) EpochForSlot(
	slot uint64,
) (uint64, error) {
	return slot / 100, nil
}

func TestLeiosPipelineAdapterEmbedsSelectedOccurrence(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Stop()
	mgr, err := leios.NewPipelineManager(leios.PipelineManagerConfig{
		EventBus: bus, SlotProvider: occurrencePipelineClock{}, EpochProvider: occurrencePipelineClock{}, Timing: leios.DefaultPipelineTiming(),
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Start(t.Context()))
	defer func() { require.NoError(t, mgr.Stop()) }()
	hash := lcommon.NewBlake2b256([]byte("same-content"))
	for _, slot := range []uint64{100, 101} {
		mgr.ObserveEndorserBlock(slot, hash)
		bus.Publish(
			leios.EbQuorumEventType,
			event.NewEvent(leios.EbQuorumEventType, leios.EbQuorumEvent{
				SlotNo: slot, EndorserBlockHash: hash, Certificate: &lcommon.LeiosEbCertificate{},
			}),
		)
	}
	testutil.WaitForCondition(
		t,
		func() bool { return len(mgr.EligibleCertifiedEbs()) == 2 },
		2*time.Second,
		"both occurrences certified",
	)
	adapter := &leiosPipelineAdapter{mgr: mgr}
	adapter.MarkEndorserBlockEmbedded(hash, 100)
	eligible := adapter.EligibleCertifiedEndorserBlocks()
	require.Len(t, eligible, 1)
	require.Equal(t, uint64(101), eligible[0].SlotNo)
}
