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

package leios

import (
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestPipelineQuorumPreservesOccurrenceSlot(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	defer f.eventBus.Stop()
	hash := ebHashFor("same-content")
	f.slot.slot = 100
	f.mgr.ObserveEndorserBlock(100, hash)
	f.slot.slot = 101
	f.mgr.ObserveEndorserBlock(101, hash)
	cert := &lcommon.LeiosEbCertificate{}
	f.mgr.handleEbQuorum(
		EbQuorumEvent{SlotNo: 100, EndorserBlockHash: hash, Certificate: cert},
	)
	eligible := f.mgr.EligibleCertifiedEbs()
	require.Len(t, eligible, 1)
	require.Equal(t, uint64(100), eligible[0].SlotNo)
	require.Same(t, cert, eligible[0].Certificate)
	stage, tracked := f.mgr.StageOf(101, hash)
	require.True(t, tracked)
	require.Equal(t, StageProduce, stage)
}

func TestPipelineCleanupPreservesOtherOccurrence(t *testing.T) {
	for _, kind := range []string{"ttl", "epoch", "rollback"} {
		t.Run(kind, func(t *testing.T) {
			timing := DefaultPipelineTiming()
			if kind == "epoch" {
				timing.InstanceTTLSlots = 1000
			}
			f := newPipelineFixture(t, timing)
			defer f.eventBus.Stop()
			hash := ebHashFor("same-content")
			f.slot.slot = 100
			f.mgr.ObserveEndorserBlock(100, hash)
			second := uint64(150)
			if kind == "epoch" {
				second = 200
			}
			f.slot.slot = second
			f.mgr.ObserveEndorserBlock(second, hash)
			retained := second
			switch kind {
			case "ttl":
				f.slot.slot = 200
				f.mgr.EligibleCertifiedEbs()
			case "epoch":
				f.mgr.handleEpochTransition(
					event.EpochTransitionEvent{NewEpoch: 3},
				)
			case "rollback":
				retained = 100
				f.mgr.handleRollback(
					chain.ChainRollbackEvent{Point: ocommon.Point{Slot: 100}},
				)
			}
			_, tracked := f.mgr.StageOf(retained, hash)
			require.True(t, tracked, "cleanup removed another occurrence")
		})
	}
}
