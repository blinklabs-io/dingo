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

package eras_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// HardForkFunc is invoked when the chain crosses an era boundary. Its
// postcondition must be: the returned pparams reflect the new era,
// including ProtocolMajor at or above the new era's MinMajorVersion.
//
// Without this guarantee, schedule-driven era transitions
// (Test*HardForkAtEpoch overrides, HardForkInitiation gov actions)
// leave the major version stale at the prior era's value, and
// downstream consumers that disambiguate eras by ProtocolMajor (e.g.
// ledger/forging/eras.go extractPParamsLimits) treat the post-fork
// pparams as belonging to the prior era. The chain then never
// advances.
//
// The version-bump tests below cover the era transitions whose HardForkFunc
// takes no genesis configuration. Shelley, Alonzo, and Conway are excluded
// from those because they require ShelleyGenesis / AlonzoGenesis /
// ConwayGenesis respectively to produce a valid result; their version-bump
// postcondition is identical and is verified end-to-end by the eras
// integration test.

func TestHardForkAllegraBumpsProtocolMajor(t *testing.T) {
	prev := &shelley.ShelleyProtocolParameters{
		ProtocolMajor: 2,
		ProtocolMinor: 0,
	}
	got, err := eras.HardForkAllegra(nil, prev)
	require.NoError(t, err)
	pp, ok := got.(*shelley.ShelleyProtocolParameters)
	require.Truef(t, ok, "expected *shelley.ShelleyProtocolParameters, got %T", got)
	require.Equalf(
		t, eras.AllegraEraDesc.MinMajorVersion, pp.ProtocolMajor,
		"HardForkAllegra must bump ProtocolMajor to AllegraEraDesc.MinMajorVersion (%d), got %d",
		eras.AllegraEraDesc.MinMajorVersion, pp.ProtocolMajor,
	)
}

func TestHardForkMaryBumpsProtocolMajor(t *testing.T) {
	// Allegra pparams are a type alias of Shelley pparams; pass the
	// underlying type directly. ProtocolMajor=3 reflects an in-Allegra
	// chain about to transition to Mary.
	prev := &shelley.ShelleyProtocolParameters{
		ProtocolMajor: 3,
		ProtocolMinor: 0,
	}
	got, err := eras.HardForkMary(nil, prev)
	require.NoError(t, err)
	pp, ok := got.(*mary.MaryProtocolParameters)
	require.Truef(t, ok, "expected *mary.MaryProtocolParameters, got %T", got)
	require.Equalf(
		t, eras.MaryEraDesc.MinMajorVersion, pp.ProtocolMajor,
		"HardForkMary must bump ProtocolMajor to MaryEraDesc.MinMajorVersion (%d), got %d",
		eras.MaryEraDesc.MinMajorVersion, pp.ProtocolMajor,
	)
}

func TestHardForkBabbageBumpsProtocolMajor(t *testing.T) {
	// Alonzo pparams with ProtocolMajor=6 (top of Alonzo's range)
	// transitioning to Babbage's MinMajorVersion (7).
	prev := &alonzo.AlonzoProtocolParameters{
		ProtocolMajor: 6,
		ProtocolMinor: 0,
	}
	got, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)
	pp, ok := got.(*babbage.BabbageProtocolParameters)
	require.Truef(t, ok, "expected *babbage.BabbageProtocolParameters, got %T", got)
	require.Equalf(
		t, eras.BabbageEraDesc.MinMajorVersion, pp.ProtocolMajor,
		"HardForkBabbage must bump ProtocolMajor to BabbageEraDesc.MinMajorVersion (%d), got %d",
		eras.BabbageEraDesc.MinMajorVersion, pp.ProtocolMajor,
	)
}

// TestHardForkBabbageDoesNotMutatePreviousCostModels verifies that the
// Babbage upgrade does not modify CostModels owned by the previous era.
func TestHardForkBabbageDoesNotMutatePreviousCostModels(t *testing.T) {
	prev := &alonzo.AlonzoProtocolParameters{
		ProtocolMajor: 6,
		CostModels: map[uint][]int64{
			0: {1, 2, 3},
		},
	}

	got, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)
	pp := got.(*babbage.BabbageProtocolParameters)
	pp.CostModels[0][0] = 9
	pp.CostModels[2] = []int64{4}

	require.Equal(t, []int64{1, 2, 3}, prev.CostModels[0])
	require.NotContains(t, prev.CostModels, uint(1))
	require.NotContains(t, prev.CostModels, uint(2))
}

// TestHardForkConwayDoesNotMutatePreviousCostModels verifies that the
// Conway upgrade does not modify CostModels owned by the previous era.
func TestHardForkConwayDoesNotMutatePreviousCostModels(t *testing.T) {
	prev := &babbage.BabbageProtocolParameters{
		ProtocolMajor: 8,
		CostModels: map[uint][]int64{
			0: {1, 2, 3},
		},
	}

	got, err := eras.HardForkConway(&cardano.CardanoNodeConfig{}, prev)
	require.NoError(t, err)
	pp := got.(*conway.ConwayProtocolParameters)
	pp.CostModels[0][0] = 9
	pp.CostModels[2] = []int64{4}

	require.Equal(t, []int64{1, 2, 3}, prev.CostModels[0])
	require.NotContains(t, prev.CostModels, uint(1))
	require.NotContains(t, prev.CostModels, uint(2))
}
