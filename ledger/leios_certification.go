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

package ledger

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// errLeiosCertificateTooYoung is the verdict on a certifying ranking block
// forged before the announcement, voting and diffusion periods have elapsed
// since the block that announced the endorser block.
var errLeiosCertificateTooYoung = errors.New(
	"certifying ranking block is too soon after the announcement",
)

// leiosCertificationGapSlots returns the number of slots that must pass
// between the block announcing an endorser block and a block certifying it:
// three announcement periods (which rule out equivocation), the voting period
// and the diffusion period, rounded up to whole slots because a block is
// forged at the onset of its slot. The periods are protocol parameters in
// milliseconds; a ledger without them (or with all three zero) imposes no gap.
func leiosCertificationGapSlots(
	pparams *dijkstra.DijkstraProtocolParameters,
	slotLengthMs uint64,
) (uint64, error) {
	totalMs := 3*uint64(pparams.LeiosAnnouncementPeriodLength) +
		uint64(pparams.LeiosVotePeriodLength) +
		uint64(pparams.LeiosDiffusionPeriodLength)
	if totalMs == 0 {
		return 0, nil
	}
	if slotLengthMs == 0 {
		return 0, errors.New("slot length is unknown")
	}
	return (totalMs + slotLengthMs - 1) / slotLengthMs, nil
}

// leiosEpochForSlot resolves a slot against the published epoch cache and, for
// a slot past it (the certifying block may open an epoch the cache does not yet
// hold), against the era history.
func (ls *LedgerState) leiosEpochForSlot(slot uint64) (models.Epoch, error) {
	if epoch, err := ls.epochForSlot(slot); err == nil {
		return epoch, nil
	}
	return ls.SlotToEpoch(slot)
}

// CheckLeiosCertificationTiming reports whether a ranking block at slot may
// certify the endorser block announced by the ranking block at announcingSlot.
// It returns nil when it may, errLeiosCertificateWithoutAnnouncement when the
// announcement was made in an earlier epoch (an endorser block is never
// certified across an epoch boundary, so there is nothing to certify), and
// errLeiosCertificateTooYoung when the certification gap has not elapsed. Any
// other error means the node could not decide, which says nothing about the
// block.
//
// The periods are read from the current protocol parameters. They cannot change
// between the announcing and the certifying block, because those share an
// epoch and parameters change only at an epoch boundary.
func (ls *LedgerState) CheckLeiosCertificationTiming(
	announcingSlot uint64,
	slot uint64,
) error {
	announcingEpoch, err := ls.leiosEpochForSlot(announcingSlot)
	if err != nil {
		return fmt.Errorf("resolve announcing block epoch: %w", err)
	}
	certifyingEpoch, err := ls.leiosEpochForSlot(slot)
	if err != nil {
		return fmt.Errorf("resolve certifying block epoch: %w", err)
	}
	if announcingEpoch.EpochId != certifyingEpoch.EpochId {
		return errLeiosCertificateWithoutAnnouncement
	}
	snapshot := ls.loadConsensusSnapshot()
	if snapshot == nil {
		return errors.New("protocol parameters not yet published")
	}
	pparams, ok := snapshot.currentPParams.(*dijkstra.DijkstraProtocolParameters)
	if !ok {
		return nil
	}
	gap, err := leiosCertificationGapSlots(
		pparams,
		uint64(announcingEpoch.SlotLength),
	)
	if err != nil {
		return fmt.Errorf("derive certification gap: %w", err)
	}
	if slot < announcingSlot+gap {
		return fmt.Errorf(
			"%w: announced at slot %d, certified at slot %d, earliest slot %d",
			errLeiosCertificateTooYoung,
			announcingSlot,
			slot,
			announcingSlot+gap,
		)
	}
	return nil
}
