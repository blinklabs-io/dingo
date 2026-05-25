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

package main

import (
	"fmt"

	"github.com/blinklabs-io/dingo/internal/test/consensus/format"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
)

// convertVector rewraps an Amaru-shape conformance.TestVector into a
// new-format ledger-category format.TestVector. The opaque CBOR blobs
// (config, initial_state, final_state, tx_cbor) pass through
// byte-for-byte; the event positional tuples become discriminated
// objects with the same numeric content.
//
// This is mechanical structural translation. No semantic
// reinterpretation — in particular, a PassEpoch event's `epoch`
// field in the new format carries the source's `EpochDelta` value
// unchanged. The replay-side ledger driver is what decides what
// "epoch" means in context.
func convertVector(src *conformance.TestVector) (format.TestVector, error) {
	events := make([]format.LedgerEvent, 0, len(src.Events))
	for i, e := range src.Events {
		out, err := convertEvent(e)
		if err != nil {
			return format.TestVector{}, fmt.Errorf(
				"event[%d]: %w", i, err,
			)
		}
		events = append(events, out)
	}
	return format.TestVector{
		SchemaVersion: format.CurrentSchemaVersion,
		Title:         src.Title,
		Category:      format.CategoryLedger,
		LedgerPhase: &format.LedgerPhase{
			Config:       format.HexBytes(src.Config),
			InitialState: format.HexBytes(src.InitialState),
			FinalState:   format.HexBytes(src.FinalState),
			Events:       events,
		},
	}, nil
}

// convertEvent maps one Amaru positional event tuple into a typed
// new-format LedgerEvent. Unknown event types fail loudly so a
// corpus change or tool drift surfaces immediately.
func convertEvent(e conformance.VectorEvent) (format.LedgerEvent, error) {
	switch e.Type {
	case conformance.EventTypeTransaction:
		success := e.Success
		slot := e.Slot
		return format.LedgerEvent{
			Type:    format.LedgerEventTransaction,
			TxCbor:  format.HexBytes(e.TxBytes),
			Success: &success,
			Slot:    &slot,
		}, nil
	case conformance.EventTypePassTick:
		slot := e.TickSlot
		return format.LedgerEvent{
			Type: format.LedgerEventPassTick,
			Slot: &slot,
		}, nil
	case conformance.EventTypePassEpoch:
		// The source stores this as EpochDelta; the new format
		// names the field `epoch`. Per the conversion contract this
		// is a structural rewrap — the numeric value is preserved
		// verbatim. The semantic interpretation belongs to the
		// replay-side ledger driver.
		epoch := e.EpochDelta
		return format.LedgerEvent{
			Type:  format.LedgerEventPassEpoch,
			Epoch: &epoch,
		}, nil
	case conformance.EventTypeRollback:
		target := e.RollbackSlot
		return format.LedgerEvent{
			Type:       format.LedgerEventRollback,
			TargetSlot: &target,
		}, nil
	}
	return format.LedgerEvent{}, fmt.Errorf(
		"unrecognized event type %d", e.Type,
	)
}
