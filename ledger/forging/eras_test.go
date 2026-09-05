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

package forging

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
	utxorpc_cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// unrecognizedProtocolParameters satisfies lcommon.ProtocolParameters
// without matching any case extractPParamsLimits recognizes.
type unrecognizedProtocolParameters struct{}

func (unrecognizedProtocolParameters) Utxorpc() (*utxorpc_cardano.PParams, error) {
	return nil, nil
}

// TestExtractPParamsLimitsRejectsNilInterface covers the ordinary nil case:
// no concrete type is stored in the interface at all.
func TestExtractPParamsLimitsRejectsNilInterface(t *testing.T) {
	_, err := extractPParamsLimits(nil)
	require.ErrorContains(t, err, "nil")
}

// TestExtractPParamsLimitsRejectsTypedNilPointer covers a provider
// returning a non-nil lcommon.ProtocolParameters interface value that
// wraps a nil pointer of a known era's concrete type -- e.g. a
// ProtocolParamsForSlot implementation that assigns a typed *T(nil) to an
// interface-typed field. `p == nil` is false for that value (the
// interface itself has a concrete type), so without this check the type
// switch below would match the era's case and panic dereferencing pp.
func TestExtractPParamsLimitsRejectsTypedNilPointer(t *testing.T) {
	var nilConway *conway.ConwayProtocolParameters
	_, err := extractPParamsLimits(nilConway)
	require.ErrorContains(t, err, "nil")

	var nilBabbage *babbage.BabbageProtocolParameters
	_, err = extractPParamsLimits(nilBabbage)
	require.ErrorContains(t, err, "nil")
}

// TestExtractPParamsLimitsRejectsUnrecognizedType covers a concrete type
// extractPParamsLimits does not recognize at all.
func TestExtractPParamsLimitsRejectsUnrecognizedType(t *testing.T) {
	_, err := extractPParamsLimits(unrecognizedProtocolParameters{})
	require.ErrorContains(t, err, "unsupported")
}

// TestEnforceOpCertNoGapRule pins the exported era classification callers
// outside this package (startup credential validation) rely on to match
// the forge loop's own classification: Praos (Babbage onward) enforces the
// no-gap rule, TPraos (Shelley-Alonzo) does not.
func TestEnforceOpCertNoGapRule(t *testing.T) {
	enforce, err := EnforceOpCertNoGapRule(
		&babbage.BabbageProtocolParameters{},
	)
	require.NoError(t, err)
	require.True(
		t,
		enforce,
		"Babbage is Praos and must enforce the no-gap rule",
	)

	enforce, err = EnforceOpCertNoGapRule(
		&conway.ConwayProtocolParameters{},
	)
	require.NoError(t, err)
	require.True(t, enforce, "Conway is Praos and must enforce the no-gap rule")

	enforce, err = EnforceOpCertNoGapRule(
		&shelley.ShelleyProtocolParameters{ProtocolMajor: 2},
	)
	require.NoError(t, err)
	require.False(
		t,
		enforce,
		"Shelley is TPraos and must not enforce the no-gap rule",
	)

	_, err = EnforceOpCertNoGapRule(nil)
	require.Error(t, err)
}
