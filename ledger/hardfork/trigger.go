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

package hardfork

import "fmt"

// TriggerKind identifies which of the three TriggerHardFork variants applies.
// Mirrors Haskell's Ouroboros.Consensus.HardFork.Simple.TriggerHardFork.
type TriggerKind uint8

const (
	// TriggerAtVersion means the transition fires when the on-chain pparams
	// advertise the configured next-era major protocol version (and, in the
	// full HFC, the vote has stabilised). Version holds that major version.
	TriggerAtVersion TriggerKind = iota
	// TriggerAtEpoch means the transition is administratively pinned to a
	// specific epoch (the "TestXHardForkAtEpoch" override). Epoch holds
	// that epoch ID. The pparams major-version bump is bypassed.
	TriggerAtEpoch
	// TriggerNotDuringThisExecution means no transition out of this era can
	// occur under the currently-running binary — the node does not know
	// about the successor era.
	TriggerNotDuringThisExecution
)

// TriggerHardFork describes how a transition OUT of a given era is armed.
// It is stored per-era on ShapeEntry and is the Go equivalent of Haskell's
// TriggerHardFork sum type.
type TriggerHardFork struct {
	Kind    TriggerKind
	Version uint   // valid when Kind == TriggerAtVersion
	Epoch   uint64 // valid when Kind == TriggerAtEpoch
}

// NewTriggerAtVersion returns a TriggerAtVersion variant targeting the given
// next-era major protocol version.
func NewTriggerAtVersion(majorVersion uint) TriggerHardFork {
	return TriggerHardFork{Kind: TriggerAtVersion, Version: majorVersion}
}

// NewTriggerAtEpoch returns a TriggerAtEpoch variant pinning the transition
// to the given epoch. This corresponds to the TestXHardForkAtEpoch YAML
// override.
func NewTriggerAtEpoch(epoch uint64) TriggerHardFork {
	return TriggerHardFork{Kind: TriggerAtEpoch, Epoch: epoch}
}

// NewTriggerNotDuringThisExecution returns the variant used by the last era
// a binary knows about: no successor, so no in-execution transition.
func NewTriggerNotDuringThisExecution() TriggerHardFork {
	return TriggerHardFork{Kind: TriggerNotDuringThisExecution}
}

// String returns a human-readable form, e.g. "AtVersion(4)", "AtEpoch(500)",
// "NotDuringThisExecution".
func (t TriggerHardFork) String() string {
	switch t.Kind {
	case TriggerAtVersion:
		return fmt.Sprintf("AtVersion(%d)", t.Version)
	case TriggerAtEpoch:
		return fmt.Sprintf("AtEpoch(%d)", t.Epoch)
	case TriggerNotDuringThisExecution:
		return "NotDuringThisExecution"
	default:
		return fmt.Sprintf("TriggerHardFork(?kind=%d)", t.Kind)
	}
}
