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

package nodesettings_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

func TestFamilyBitsAreStable(t *testing.T) {
	t.Parallel()
	// Persisted masks name families by bit position, so a release that
	// inserted or reordered a family would misread every latched database.
	want := []nodesettings.Family{
		nodesettings.FamilyNonces,
		nodesettings.FamilyEpochs,
		nodesettings.FamilySummaries,
		nodesettings.FamilyEraSwitch,
		nodesettings.FamilyNetworkDonations,
		nodesettings.FamilyPParamsUpdates,
		nodesettings.FamilyCommitteeConstitution,
		nodesettings.FamilyOpCerts,
		nodesettings.FamilyPoolsHistory,
		nodesettings.FamilyAccounts,
		nodesettings.FamilyDReps,
		nodesettings.FamilyMIRGenesis,
		nodesettings.FamilyGovernance,
		nodesettings.FamilyRewardInputs,
		nodesettings.FamilyLiveStake,
		nodesettings.FamilyRewardBlocksFees,
		nodesettings.FamilyRewardOutputs,
	}
	require.Equal(t, want, nodesettings.Families())
	for i, f := range want {
		require.Equal(t, nodesettings.Family(i), f)
		require.NotContains(t, f.String(), "family-")
	}
	require.Equal(
		t, nodesettings.FamilyMask(1<<len(want)-1), nodesettings.AllFamilies(),
	)
}

func TestFamilyMaskEncodingRoundTrips(t *testing.T) {
	t.Parallel()
	mask := nodesettings.FamilyMask(0).
		With(nodesettings.FamilyNonces).
		With(nodesettings.FamilyLiveStake)
	encoded := nodesettings.EncodeFamilyMask(mask)
	require.Equal(t, "f1:0000000000004001", encoded)
	got, err := nodesettings.DecodeFamilyMask(encoded)
	require.NoError(t, err)
	require.Equal(t, mask, got)

	empty, err := nodesettings.DecodeFamilyMask("")
	require.NoError(t, err)
	require.Zero(t, empty)
}

func TestFamilyMaskDecodeRejectsMalformedValues(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"0000000000000001",
		"f2:0000000000000001",
		"f1:1",
		"f1:000000000000000g",
		"on",
	} {
		_, err := nodesettings.DecodeFamilyMask(value)
		require.ErrorIs(t, err, nodesettings.ErrLatchFormat, value)
	}
}

func TestWidenFamilyMaskOnlyWidens(t *testing.T) {
	t.Parallel()
	a := nodesettings.FamilyMask(0).With(nodesettings.FamilyNonces)
	ab := a.With(nodesettings.FamilyEpochs)

	got, err := nodesettings.WidenFamilyMask(a, ab)
	require.NoError(t, err)
	require.Equal(t, ab, got)
	got, err = nodesettings.WidenFamilyMask(ab, ab)
	require.NoError(t, err)
	require.Equal(t, ab, got)
	got, err = nodesettings.WidenFamilyMask(0, 0)
	require.NoError(t, err)
	require.Zero(t, got)

	_, err = nodesettings.WidenFamilyMask(ab, a)
	require.ErrorIs(t, err, nodesettings.ErrLatchNarrowed)
	require.ErrorContains(t, err, "epochs")
	_, err = nodesettings.WidenFamilyMask(
		ab, nodesettings.FamilyMask(0).With(nodesettings.FamilyOpCerts),
	)
	require.ErrorIs(t, err, nodesettings.ErrLatchNarrowed)
}

func TestUnknownFamilies(t *testing.T) {
	t.Parallel()
	known := nodesettings.AllFamilies()
	require.Zero(t, nodesettings.UnknownFamilies(known, known))
	future := nodesettings.FamilyMask(1) << 20
	require.Equal(t, future, nodesettings.UnknownFamilies(known|future, known))
}
