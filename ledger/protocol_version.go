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
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ProtocolVersion is the major and minor protocol version of a parameter set.
type ProtocolVersion = epochengine.ProtocolVersion

// GetProtocolVersion extracts the protocol version from protocol parameters
// of any era. It returns an error for nil or unrecognized parameters.
func GetProtocolVersion(
	pparams lcommon.ProtocolParameters,
) (ProtocolVersion, error) {
	return epochengine.GetProtocolVersion(pparams)
}

// EraForVersion returns the era ID for a given protocol major version,
// resolved against the provided runtime era table. Returns false if no era
// covers the given version.
func EraForVersion(eraList []eras.EraDesc, majorVersion uint) (uint, bool) {
	eraDesc, ok := eras.EraForVersionIn(eraList, majorVersion)
	if !ok {
		return 0, false
	}
	return eraDesc.Id, true
}

// IsHardForkTransition returns true if the new protocol
// version triggers an era change compared to the old version.
func IsHardForkTransition(
	eraList []eras.EraDesc,
	oldVersion, newVersion ProtocolVersion,
) bool {
	oldEra, oldOk := EraForVersion(eraList, oldVersion.Major)
	newEra, newOk := EraForVersion(eraList, newVersion.Major)
	if !oldOk || !newOk {
		return false
	}
	return newEra != oldEra
}
