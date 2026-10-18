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
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func TestLeiosCertificationGapSlots(t *testing.T) {
	t.Parallel()
	periods := func(ann, vote, diff uint32) *dijkstra.DijkstraProtocolParameters {
		p := &dijkstra.DijkstraProtocolParameters{}
		p.LeiosAnnouncementPeriodLength = ann
		p.LeiosVotePeriodLength = vote
		p.LeiosDiffusionPeriodLength = diff
		return p
	}
	tests := []struct {
		name       string
		pparams    *dijkstra.DijkstraProtocolParameters
		slotLength uint64
		want       uint64
		wantErr    bool
	}{
		// Three announcement periods plus voting and diffusion: 3+4+7 s.
		{
			"default parameters on one second slots",
			periods(1000, 4000, 7000),
			1000,
			14,
			false,
		},
		{"partial slot rounds up", periods(1000, 4000, 7500), 1000, 15, false},
		{
			"slots longer than the gap",
			periods(1000, 4000, 7000),
			20000,
			1,
			false,
		},
		{"half second slots", periods(1000, 4000, 7000), 500, 28, false},
		{"no periods imposes no gap", periods(0, 0, 0), 1000, 0, false},
		{"no periods needs no slot length", periods(0, 0, 0), 0, 0, false},
		{"unknown slot length", periods(1000, 4000, 7000), 0, 0, true},
		{"nil parameters", nil, 1000, 0, true},
		{
			"ceiling does not overflow",
			periods(1000, 4000, 7000),
			math.MaxUint64,
			1,
			false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := leiosCertificationGapSlots(tc.pparams, tc.slotLength)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// setLeiosCertificationEnv gives the fixture ledger epochs of the given
// boundaries (each epoch starts where the previous ends), one second slots, and
// the given Leios periods in milliseconds.
func setLeiosCertificationEnv(
	f *leiosCertApplyFixture,
	epochEnds []uint64,
	announcement, vote, diffusion uint32,
) {
	f.pparams.LeiosAnnouncementPeriodLength = announcement
	f.pparams.LeiosVotePeriodLength = vote
	f.pparams.LeiosDiffusionPeriodLength = diffusion
	cache := make([]models.Epoch, 0, len(epochEnds))
	start := uint64(0)
	for id, end := range epochEnds {
		cache = append(cache, models.Epoch{
			EpochId:       uint64(id),
			StartSlot:     start,
			SlotLength:    1000,
			LengthInSlots: uint(end - start),
			EraId:         eras.DijkstraEraDesc.Id,
		})
		start = end
	}
	f.ls.epochCache = cache
	f.ls.currentEpoch = cache[len(cache)-1]
	f.ls.publishSnapshotsLocked()
	encoded, err := cbor.Encode(f.pparams)
	require.NoError(f.t, err)
	for _, epoch := range cache {
		require.NoError(f.t, f.db.SetPParams(
			encoded,
			epoch.StartSlot,
			epoch.EpochId,
			eras.DijkstraEraDesc.Id,
			nil,
		))
	}
}

// A block may certify once announcement, voting and diffusion periods have
// elapsed since the announcing block, and one slot earlier is too young.
func TestLeiosCertificationTimingBoundary(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	setLeiosCertificationEnv(f, []uint64{1_000}, 1000, 4000, 7000)
	const announcing = 100
	for _, tc := range []struct {
		slot uint64
		want error
	}{
		{announcing, errLeiosCertificateTooYoung},
		{announcing + 13, errLeiosCertificateTooYoung},
		{announcing + 14, nil},
		{announcing + 15, nil},
	} {
		err := f.ls.CheckLeiosCertificationTiming(announcing, tc.slot)
		if tc.want == nil {
			require.NoError(t, err, "slot %d", tc.slot)
			continue
		}
		require.ErrorIs(t, err, tc.want, "slot %d", tc.slot)
	}
}

func TestLeiosCertificationTimingUsesAnnouncementEpochParameters(
	t *testing.T,
) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	setLeiosCertificationEnv(f, []uint64{100, 200}, 1000, 4000, 7000)
	current := *f.pparams
	current.LeiosAnnouncementPeriodLength = 0
	current.LeiosVotePeriodLength = 0
	current.LeiosDiffusionPeriodLength = 0
	f.ls.currentPParams = &current
	f.ls.publishSnapshotsLocked()

	err := f.ls.CheckLeiosCertificationTiming(80, 90)
	require.ErrorIs(t, err, errLeiosCertificateTooYoung)
}

func TestLeiosCertificationTimingRejectsTypedNilParameters(t *testing.T) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	setLeiosCertificationEnv(f, []uint64{100}, 0, 0, 0)
	var pparams *dijkstra.DijkstraProtocolParameters
	f.ls.currentPParams = pparams
	f.ls.publishSnapshotsLocked()

	err := f.ls.CheckLeiosCertificationTiming(10, 20)
	require.ErrorContains(t, err, "dijkstra protocol parameters are unavailable")
}

func TestLeiosCertificationTimingDoesNotExtendTheForecastHorizon(
	t *testing.T,
) {
	t.Parallel()
	f := newLeiosCertApplyFixture(t)
	setLeiosCertificationEnv(f, []uint64{100}, 0, 0, 0)

	err := f.ls.CheckLeiosCertificationTiming(10, math.MaxUint64)
	require.Error(t, err)
	require.NotErrorIs(t, err, errLeiosCertificateAcrossEpochBoundary)
}

// An endorser block is never certified across an epoch boundary, however long
// ago it was announced, and the rule holds with no certification gap at all.
func TestLeiosCertificationEpochBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		announcement, vote, dif uint32
		announcing, slot        uint64
		want                    error
	}{
		{"no gap, same epoch up to the last slot", 0, 0, 0, 98, 99, nil},
		{"no gap, last slot to first slot of next epoch", 0, 0, 0, 99, 100, errLeiosCertificateAcrossEpochBoundary},
		{"no gap, first slot of next epoch to its second", 0, 0, 0, 100, 101, nil},
		{"gap elapsed but across the boundary", 1000, 4000, 7000, 99, 113, errLeiosCertificateAcrossEpochBoundary},
		{"gap elapsed across the boundary by a wide margin", 1000, 4000, 7000, 10, 150, errLeiosCertificateAcrossEpochBoundary},
		{"gap elapsed on the last slot of the epoch", 1000, 4000, 7000, 85, 99, nil},
		{"one slot short of the gap on the last slot", 1000, 4000, 7000, 86, 99, errLeiosCertificateTooYoung},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLeiosCertApplyFixture(t)
			setLeiosCertificationEnv(
				f,
				[]uint64{100, 200},
				tc.announcement,
				tc.vote,
				tc.dif,
			)
			err := f.ls.CheckLeiosCertificationTiming(tc.announcing, tc.slot)
			if tc.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.want)
		})
	}
}

// The ledger rejects a certifying block that is too young or that certifies
// across an epoch boundary as an invalid block, before the certificate is
// checked, and accepts it exactly at the boundary slot.
func TestLeiosCertifyingBlockTimingIsEnforcedOnApply(t *testing.T) {
	t.Parallel()
	// The fixture announces at slot 11 and certifies at slot 12.
	for _, tc := range []struct {
		name       string
		epochEnds  []uint64
		vote       uint32
		wantReject error
	}{
		{"exactly at the certification gap", []uint64{1_000}, 1000, nil},
		{"one slot short of the certification gap", []uint64{1_000}, 2000, errLeiosCertificateTooYoung},
		{"after the certification gap", []uint64{1_000}, 0, nil},
		{"announced in the previous epoch", []uint64{12, 1_000}, 0, errLeiosCertificateAcrossEpochBoundary},
		{"same epoch as the announcement", []uint64{13, 1_000}, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLeiosCertApplyFixture(t)
			parent := f.announce()
			setLeiosCertificationEnv(f, tc.epochEnds, 0, tc.vote, 0)
			certifier, offsets, envParent := f.certifyingChild(parent)

			err := f.process(certifier, offsets, envParent)
			if tc.wantReject == nil {
				require.NoError(t, err)
				require.Equal(t, 1, f.certCalls)
				return
			}
			require.ErrorIs(t, err, tc.wantReject)
			var rejected *headerValidationError
			require.ErrorAs(t, err, &rejected)
			require.Zero(
				t,
				f.certCalls,
				"a timing verdict precedes the certificate check",
			)
		})
	}
}
