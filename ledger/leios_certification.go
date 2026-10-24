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
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// errLeiosCertificateTooYoung is the verdict on a certifying ranking block
// forged before the announcement, voting and diffusion periods have elapsed
// since the block that announced the endorser block.
var errLeiosCertificateTooYoung = errors.New(
	"certifying ranking block is too soon after the announcement",
)

var errLeiosCertificateAcrossEpochBoundary = errors.New(
	"certifying ranking block crosses the announcement epoch boundary",
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
	if pparams == nil {
		return 0, errors.New("dijkstra protocol parameters are unavailable")
	}
	totalMs := 3*uint64(pparams.LeiosAnnouncementPeriodLength) +
		uint64(pparams.LeiosVotePeriodLength) +
		uint64(pparams.LeiosDiffusionPeriodLength)
	if totalMs == 0 {
		return 0, nil
	}
	if slotLengthMs == 0 {
		return 0, errors.New("slot length is unknown")
	}
	gap := totalMs / slotLengthMs
	if totalMs%slotLengthMs != 0 {
		gap++
	}
	return gap, nil
}

func leiosEpochFromInfo(infoEpoch uint64, startSlot uint64, eraID uint,
	slotLength time.Duration, lengthInSlots uint64,
) models.Epoch {
	// The hard-fork summary validates both values against the protocol bounds.
	// #nosec G115
	return models.Epoch{
		EpochId:       infoEpoch,
		StartSlot:     startSlot,
		EraId:         eraID,
		SlotLength:    uint(slotLength / time.Millisecond),
		LengthInSlots: uint(lengthInSlots),
	}
}

func (ls *LedgerState) leiosEpochsForSlots(
	snapshot *consensusSnapshot,
	tip *tipSnapshot,
	announcingSlot uint64,
	certifyingSlot uint64,
) (models.Epoch, models.Epoch, error) {
	announcingEpoch, announcingErr := epochForSlotInCache(
		snapshot.epochCache,
		announcingSlot,
	)
	certifyingEpoch, certifyingErr := epochForSlotInCache(
		snapshot.epochCache,
		certifyingSlot,
	)
	if announcingErr == nil && certifyingErr == nil {
		return announcingEpoch, certifyingEpoch, nil
	}
	if tip == nil {
		return models.Epoch{}, models.Epoch{}, errors.New(
			"ledger tip not yet published",
		)
	}
	summary, err := ls.buildHardForkSummary(
		snapshot,
		tip,
		0,
	)
	if err != nil {
		return models.Epoch{}, models.Epoch{}, err
	}
	resolve := func(slot uint64) (models.Epoch, error) {
		info, err := summary.SlotToEpoch(slot)
		if err != nil {
			return models.Epoch{}, err
		}
		return leiosEpochFromInfo(
			info.Epoch,
			info.StartSlot,
			info.EraID,
			info.SlotLength,
			info.LengthInSlots,
		), nil
	}
	announcingEpoch, err = resolve(announcingSlot)
	if err != nil {
		return models.Epoch{}, models.Epoch{}, fmt.Errorf(
			"resolve announcing block epoch: %w",
			err,
		)
	}
	certifyingEpoch, err = resolve(certifyingSlot)
	if err != nil {
		return models.Epoch{}, models.Epoch{}, fmt.Errorf(
			"resolve certifying block epoch: %w",
			err,
		)
	}
	return announcingEpoch, certifyingEpoch, nil
}

func (ls *LedgerState) leiosPParamsForEpoch(
	snapshot *consensusSnapshot,
	epoch models.Epoch,
) (*dijkstra.DijkstraProtocolParameters, error) {
	pparams := snapshot.currentPParams
	if epoch.EpochId != snapshot.currentEpoch.EpochId {
		if ls.db == nil {
			return nil, errors.New(
				"historical protocol parameters are unavailable",
			)
		}
		era := eras.GetEraById(epoch.EraId)
		if era == nil {
			return nil, fmt.Errorf("unknown era %d", epoch.EraId)
		}
		var err error
		pparams, err = ls.loadPersistedProtocolParameters(
			epoch.EpochId,
			*era,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}
	dijkstraPParams, ok := pparams.(*dijkstra.DijkstraProtocolParameters)
	if !ok || dijkstraPParams == nil {
		return nil, fmt.Errorf(
			"dijkstra protocol parameters are unavailable for epoch %d (got %T)",
			epoch.EpochId,
			pparams,
		)
	}
	return dijkstraPParams, nil
}

// CheckLeiosCertificationTiming reports whether a ranking block at slot may
// certify the endorser block announced by the ranking block at announcingSlot.
// It returns nil when it may, errLeiosCertificateAcrossEpochBoundary when the
// announcement was made in another epoch, and errLeiosCertificateTooYoung
// when the certification gap has not elapsed. Any other error means the node
// could not decide, which says nothing about the block.
func (ls *LedgerState) CheckLeiosCertificationTiming(
	announcingSlot uint64,
	slot uint64,
) error {
	_, err := ls.checkLeiosCertificationTiming(announcingSlot, slot)
	return err
}

func (ls *LedgerState) checkLeiosCertificationTiming(
	announcingSlot uint64,
	slot uint64,
) (models.Epoch, error) {
	snapshot, tip := ls.loadConsensusAndTipSnapshots()
	if snapshot == nil {
		return models.Epoch{}, errors.New("ledger state not yet published")
	}
	announcingEpoch, certifyingEpoch, err := ls.leiosEpochsForSlots(
		snapshot,
		tip,
		announcingSlot,
		slot,
	)
	if err != nil {
		return models.Epoch{}, err
	}
	if announcingEpoch.EpochId != certifyingEpoch.EpochId {
		return models.Epoch{}, errLeiosCertificateAcrossEpochBoundary
	}
	pparams, err := ls.leiosPParamsForEpoch(snapshot, announcingEpoch)
	if err != nil {
		return models.Epoch{}, fmt.Errorf(
			"resolve certification protocol parameters: %w",
			err,
		)
	}
	gap, err := leiosCertificationGapSlots(
		pparams,
		uint64(announcingEpoch.SlotLength),
	)
	if err != nil {
		return models.Epoch{}, fmt.Errorf("derive certification gap: %w", err)
	}
	if slot < announcingSlot || slot-announcingSlot < gap {
		return models.Epoch{}, fmt.Errorf(
			"%w: announced at slot %d, certified at slot %d, required gap %d slots",
			errLeiosCertificateTooYoung,
			announcingSlot,
			slot,
			gap,
		)
	}
	return announcingEpoch, nil
}
