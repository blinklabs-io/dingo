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

package nodesettings

import (
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
)

// LedgerStateDrainedGateName is the node_settings_gate row recording which
// ledger-state families the metadata store no longer holds. An absent row is
// the empty mask.
const LedgerStateDrainedGateName = "ledger_state_drained"

// DrainStatementTag leads every SQL statement of the chunked row delete that
// drains a family. The statement guard lets a tagged statement through, so
// the drain is the one writer allowed to touch a table it has armed.
const DrainStatementTag = "/* ledger-state-drain */"

// Family names one group of SQL ledger-state tables that the live UTxO set
// computes as well. A family's bit is its position in FamilyMask, so a value
// is never renumbered or reused once a release has persisted it.
type Family uint8

// The ledger-state families, in the order they graduate.
const (
	FamilyNonces Family = iota
	FamilyEpochs
	FamilySummaries
	FamilyEraSwitch
	FamilyNetworkDonations
	FamilyPParamsUpdates
	FamilyCommitteeConstitution
	FamilyOpCerts
	FamilyPoolsHistory
	FamilyAccounts
	FamilyDReps
	FamilyMIRGenesis
	FamilyGovernance
	FamilyRewardInputs
	FamilyLiveStake
	FamilyRewardBlocksFees
	FamilyRewardOutputs
	familyEnd
)

var familyNames = [familyEnd]string{
	FamilyNonces:                "nonces",
	FamilyEpochs:                "epochs",
	FamilySummaries:             "summaries",
	FamilyEraSwitch:             "era-switch",
	FamilyNetworkDonations:      "network-donations",
	FamilyPParamsUpdates:        "pparams-updates",
	FamilyCommitteeConstitution: "committee-constitution",
	FamilyOpCerts:               "op-certs",
	FamilyPoolsHistory:          "pools-history",
	FamilyAccounts:              "accounts",
	FamilyDReps:                 "dreps",
	FamilyMIRGenesis:            "mir-genesis",
	FamilyGovernance:            "governance",
	FamilyRewardInputs:          "reward-inputs",
	FamilyLiveStake:             "live-stake",
	FamilyRewardBlocksFees:      "reward-blocks-fees",
	FamilyRewardOutputs:         "reward-outputs",
}

// String names the family for logs and errors.
func (f Family) String() string {
	if f < familyEnd {
		return familyNames[f]
	}
	return "family-" + strconv.Itoa(int(f))
}

// Families returns every family this binary knows, in bit order.
func Families() []Family {
	out := make([]Family, 0, familyEnd)
	for f := range familyEnd {
		out = append(out, f)
	}
	return out
}

// FamilyMask is a set of families.
type FamilyMask uint64

// AllFamilies is the mask of every family this binary knows.
func AllFamilies() FamilyMask {
	return FamilyMask(1)<<familyEnd - 1
}

// Has reports whether f is in the mask.
func (m FamilyMask) Has(f Family) bool {
	return f < 64 && m&(FamilyMask(1)<<f) != 0
}

// With returns the mask with f added.
func (m FamilyMask) With(f Family) FamilyMask {
	if f >= 64 {
		return m
	}
	return m | FamilyMask(1)<<f
}

// Contains reports whether every family of other is in m.
func (m FamilyMask) Contains(other FamilyMask) bool {
	return m&other == other
}

// Families lists the members in bit order.
func (m FamilyMask) Families() []Family {
	out := make([]Family, 0, bits.OnesCount64(uint64(m)))
	for f := range Family(64) {
		if m.Has(f) {
			out = append(out, f)
		}
	}
	return out
}

// latchFormat prefixes the persisted mask so a later release can change the
// encoding without an old binary misreading it.
const latchFormat = "f1:"

// EncodeFamilyMask renders the persisted latch value: the format tag, then
// the mask as 16 hexadecimal digits.
func EncodeFamilyMask(m FamilyMask) string {
	return fmt.Sprintf("%s%016x", latchFormat, uint64(m))
}

// ErrLatchFormat reports a latch value this binary cannot decode.
var ErrLatchFormat = errors.New("unrecognised ledger-state latch value")

// ErrLatchNarrowed reports a write that would clear a drained family.
var ErrLatchNarrowed = errors.New("ledger-state latch only widens")

// DecodeFamilyMask parses a persisted latch value. An empty value is the
// empty mask.
func DecodeFamilyMask(value string) (FamilyMask, error) {
	if value == "" {
		return 0, nil
	}
	digits, ok := strings.CutPrefix(value, latchFormat)
	if !ok || len(digits) != 16 {
		return 0, fmt.Errorf("%w: %q", ErrLatchFormat, value)
	}
	m, err := strconv.ParseUint(digits, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrLatchFormat, value)
	}
	return FamilyMask(m), nil
}

// WidenFamilyMask returns the mask to persist when next replaces current. It
// refuses a next that lacks a family current drained: a drained family's rows
// are gone, so the bit can never be cleared.
func WidenFamilyMask(current, next FamilyMask) (FamilyMask, error) {
	if !next.Contains(current) {
		return current, fmt.Errorf(
			"%w: %s would be cleared",
			ErrLatchNarrowed,
			familyList(current&^next),
		)
	}
	return next, nil
}

// UnknownFamilies returns the members of m outside known.
func UnknownFamilies(m, known FamilyMask) FamilyMask {
	return m &^ known
}

func familyList(m FamilyMask) string {
	names := make([]string, 0, 4)
	for _, f := range m.Families() {
		names = append(names, f.String())
	}
	return strings.Join(names, ", ")
}
