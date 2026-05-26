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

package format

import (
	"encoding/json"
	"errors"
	"fmt"
)

// EncodeTestVector marshals v to pretty-printed JSON. The vector is
// validated first (see validate); encoding fails if it does not satisfy
// the invariants documented on TestVector.
func EncodeTestVector(v TestVector) ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, fmt.Errorf("test vector: %w", err)
	}
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("test vector: marshal: %w", err)
	}
	// MarshalIndent does not emit a trailing newline; add one so files
	// end in a newline like every other text file in the repo.
	return append(out, '\n'), nil
}

// validate enforces the structural invariants the format documents:
// supported schema version, exactly one of Capture or LedgerPhase set
// matching Category, and per-message protocol/msg-type pairs that map
// to known gouroboros IDs.
func (v TestVector) validate() error {
	if v.SchemaVersion != CurrentSchemaVersion {
		return fmt.Errorf(
			"unsupported schema_version %d (this build supports %d)",
			v.SchemaVersion, CurrentSchemaVersion,
		)
	}
	switch v.Category {
	case CategoryConsensus:
		if v.Capture == nil {
			return errors.New(
				"category=consensus requires capture to be set",
			)
		}
		if v.LedgerPhase != nil {
			return errors.New(
				"category=consensus must not set ledger_phase",
			)
		}
		if err := v.Capture.validate(); err != nil {
			return fmt.Errorf("capture: %w", err)
		}
	case CategoryLedger:
		if v.LedgerPhase == nil {
			return errors.New(
				"category=ledger requires ledger_phase to be set",
			)
		}
		if v.Capture != nil {
			return errors.New(
				"category=ledger must not set capture",
			)
		}
		if err := v.LedgerPhase.validate(); err != nil {
			return fmt.Errorf("ledger_phase: %w", err)
		}
	default:
		return fmt.Errorf("unknown category %q", v.Category)
	}
	return nil
}

func (c *ConsensusCapture) validate() error {
	for i, p := range c.Peers {
		for j, m := range p.Served {
			if err := validateServedMessage(m); err != nil {
				return fmt.Errorf(
					"peers[%d].served[%d]: %w", i, j, err,
				)
			}
		}
	}
	for i, m := range c.ExpectedOutput.DownstreamChainSync {
		if err := validateServedMessage(m); err != nil {
			return fmt.Errorf(
				"expected_output.downstream_chainsync[%d]: %w", i, err,
			)
		}
	}
	return nil
}

func (l *LedgerPhase) validate() error {
	for i, e := range l.Events {
		switch e.Type {
		case LedgerEventTransaction:
			if len(e.TxCbor) == 0 || e.Success == nil || e.Slot == nil {
				return fmt.Errorf(
					"events[%d] (transaction): missing tx_cbor/success/slot",
					i,
				)
			}
		case LedgerEventPassTick:
			if e.Slot == nil {
				return fmt.Errorf(
					"events[%d] (pass_tick): missing slot", i,
				)
			}
		case LedgerEventPassEpoch:
			if e.Epoch == nil {
				return fmt.Errorf(
					"events[%d] (pass_epoch): missing epoch", i,
				)
			}
		case LedgerEventRollback:
			if e.TargetSlot == nil {
				return fmt.Errorf(
					"events[%d] (rollback): missing target_slot",
					i,
				)
			}
		default:
			return fmt.Errorf(
				"events[%d]: unknown type %q", i, e.Type,
			)
		}
	}
	return nil
}
