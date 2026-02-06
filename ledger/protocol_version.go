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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// Compile-time check: allegra.AllegraProtocolParameters is
// currently a type alias for shelley.ShelleyProtocolParameters
// in gouroboros, so the Shelley case in GetProtocolVersion
// handles Allegra as well. If this alias is ever changed to a
// distinct type, this assignment will fail to compile,
// reminding us to add an explicit Allegra case.
var _ *shelley.ShelleyProtocolParameters = (*allegra.AllegraProtocolParameters)(nil)

// ProtocolVersion represents the major and minor protocol
// version numbers used in Cardano protocol parameters.
type ProtocolVersion struct {
	Major uint
	Minor uint
}

// GetProtocolVersion extracts the protocol version from
// protocol parameters. This works across all eras by type-
// switching on the concrete pparams type.
// Returns an error if the pparams type is not recognized or
// nil.
func GetProtocolVersion(
	pparams lcommon.ProtocolParameters,
) (ProtocolVersion, error) {
	if pparams == nil {
		return ProtocolVersion{}, errors.New("protocol parameters are nil")
	}
	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		// Also covers Allegra (protocol version 3):
		// allegra.AllegraProtocolParameters is a type alias
		// (=) for shelley.ShelleyProtocolParameters in
		// gouroboros, so both resolve to this case. The
		// compile-time assertion above guards against the
		// alias being changed to a distinct type.
		return ProtocolVersion{
			Major: pp.ProtocolMajor,
			Minor: pp.ProtocolMinor,
		}, nil
	case *mary.MaryProtocolParameters:
		return ProtocolVersion{
			Major: pp.ProtocolMajor,
			Minor: pp.ProtocolMinor,
		}, nil
	case *alonzo.AlonzoProtocolParameters:
		return ProtocolVersion{
			Major: pp.ProtocolMajor,
			Minor: pp.ProtocolMinor,
		}, nil
	case *babbage.BabbageProtocolParameters:
		return ProtocolVersion{
			Major: pp.ProtocolMajor,
			Minor: pp.ProtocolMinor,
		}, nil
	case *conway.ConwayProtocolParameters:
		return ProtocolVersion{
			Major: pp.ProtocolVersion.Major,
			Minor: pp.ProtocolVersion.Minor,
		}, nil
	default:
		return ProtocolVersion{}, fmt.Errorf(
			"unsupported protocol parameters type: %T",
			pparams,
		)
	}
}

// EraForVersion returns the era ID for a given protocol
// major version using the ProtocolMajorVersionToEra map in
// ledger/eras/. Returns false if the version is not mapped.
func EraForVersion(majorVersion uint) (uint, bool) {
	eraDesc, ok := eras.ProtocolMajorVersionToEra[majorVersion]
	if !ok {
		return 0, false
	}
	return eraDesc.Id, true
}

// IsHardForkTransition returns true if the new protocol
// version triggers an era change compared to the old version.
func IsHardForkTransition(
	oldVersion, newVersion ProtocolVersion,
) bool {
	oldEra, oldOk := EraForVersion(oldVersion.Major)
	newEra, newOk := EraForVersion(newVersion.Major)
	if !oldOk || !newOk {
		return false
	}
	return newEra != oldEra
}
