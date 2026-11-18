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

package bark

import (
	"log/slog"

	"github.com/blinklabs-io/bark/eventtype"
	eventv1alpha1 "github.com/blinklabs-io/bark/proto/v1alpha1/event"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/peergov"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ledgerbyron "github.com/blinklabs-io/gouroboros/ledger/byron"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// eventAdapter converts the node's internal EventBus events into Event
// Service events with typed payloads. Internal event structs stay internal:
// only the fields a payload message defines are copied out.
type eventAdapter struct {
	// epochForSlot and eraNameForEpoch resolve ledger context a block event
	// does not carry. Either may be nil, which leaves the field zero.
	epochForSlot    func(slot uint64) (uint64, error)
	eraNameForEpoch func(epoch uint64) (string, error)
}

// sourceTypes lists the internal event types the adapter consumes.
func (a *eventAdapter) sourceTypes() []string {
	return []string{
		peergov.PeerChurnEventType,
		chain.ChainUpdateEventType,
		string(event.EpochTransitionEventType),
		string(mempool.AddTransactionEventType),
		string(mempool.RemoveTransactionEventType),
		connmanager.InboundConnectionEventType,
		connmanager.ConnectionClosedEventType,
	}
}

// convert returns the Event Service events for one internal event, or none
// when the event carries nothing the registry describes.
func (a *eventAdapter) convert(
	evt event.Event,
	logger *slog.Logger,
) []*eventv1alpha1.Event {
	var out []*eventv1alpha1.Event
	add := func(name string, payload proto.Message) {
		packed, err := anypb.New(payload)
		if err != nil {
			logger.Warn("event service could not pack payload",
				"event_type", name, "error", err)
			return
		}
		out = append(out, &eventv1alpha1.Event{
			EventType: name,
			Timestamp: timestamppb.New(evt.Timestamp),
			TypedData: packed,
		})
	}
	switch d := evt.Data.(type) {
	case peergov.PeerChurnEvent:
		add(eventtype.PeerChurn, &eventv1alpha1.PeerChurnEventData{
			Address:  d.Address,
			Source:   d.Source,
			OldState: d.OldState,
			NewState: d.NewState,
			Score:    d.Score,
			Reason:   d.Reason,
		})
	case chain.ChainBlockEvent:
		add(eventtype.ChainUpdate, a.chainUpdate(d))
	case chain.ChainRollbackEvent:
		add(eventtype.ChainUpdate, &eventv1alpha1.ChainUpdateEventData{
			Rollback: chainRollback(d),
		})
	case event.EpochTransitionEvent:
		add(eventtype.EpochTransition, a.epochTransition(d))
	case mempool.AddTransactionEvent:
		add(eventtype.MempoolAddTx, &eventv1alpha1.TransactionEventData{
			TxHash:    d.Hash,
			SizeBytes: uint64(len(d.Body)),
			Action:    eventv1alpha1.TransactionAction_TRANSACTION_ACTION_ADDED,
		})
	case mempool.RemoveTransactionEvent:
		add(eventtype.MempoolRemoveTx, &eventv1alpha1.TransactionEventData{
			TxHash: d.Hash,
			Action: eventv1alpha1.TransactionAction_TRANSACTION_ACTION_REMOVED,
		})
	case connmanager.InboundConnectionEvent:
		address := d.NormalizedRemoteAddr
		if address == "" && d.RemoteAddr != nil {
			address = d.RemoteAddr.String()
		}
		add(eventtype.ConnManagerInboundConn, &eventv1alpha1.ConnectionEventData{
			Address:   address,
			Direction: eventv1alpha1.ConnectionDirection_CONNECTION_DIRECTION_INBOUND,
			Action:    eventv1alpha1.ConnectionAction_CONNECTION_ACTION_ACCEPTED,
		})
	case connmanager.ConnectionClosedEvent:
		add(eventtype.ConnManagerConnClosed, a.connectionClosed(d))
	}
	return out
}

func (a *eventAdapter) chainUpdate(
	d chain.ChainBlockEvent,
) *eventv1alpha1.ChainUpdateEventData {
	data := &eventv1alpha1.ChainUpdateEventData{
		Slot:        d.Block.Slot,
		BlockHash:   d.Block.Hash,
		BlockNumber: d.Block.Number,
		Era:         eraNameForBlockType(d.Block.Type),
	}
	if a.epochForSlot != nil {
		if epoch, err := a.epochForSlot(d.Block.Slot); err == nil {
			data.Epoch = epoch
		}
	}
	if decoded, err := d.Block.Decode(); err == nil {
		// A block holds far fewer transactions than uint32 can count.
		data.TxCount = uint32(len(decoded.Transactions())) //nolint:gosec
	}
	return data
}

func chainRollback(d chain.ChainRollbackEvent) *eventv1alpha1.ChainRollbackEventData {
	fromSlot := d.Point.Slot
	if len(d.RolledBackBlocks) > 0 {
		// RolledBackBlocks is newest first, so its head was the old tip.
		fromSlot = d.RolledBackBlocks[0].Slot
	}
	return &eventv1alpha1.ChainRollbackEventData{
		FromSlot:         fromSlot,
		ToSlot:           d.Point.Slot,
		BlocksRolledBack: uint32(len(d.RolledBackBlocks)), //nolint:gosec
		NewTipHash:       d.Point.Hash,
	}
}

func (a *eventAdapter) epochTransition(
	d event.EpochTransitionEvent,
) *eventv1alpha1.EpochTransitionEventData {
	data := &eventv1alpha1.EpochTransitionEventData{
		PreviousEpoch: d.PreviousEpoch,
		NewEpoch:      d.NewEpoch,
		Slot:          d.BoundarySlot,
	}
	if a.eraNameForEpoch != nil {
		if name, err := a.eraNameForEpoch(d.NewEpoch); err == nil {
			data.Era = name
		}
	}
	return data
}

func (a *eventAdapter) connectionClosed(
	d connmanager.ConnectionClosedEvent,
) *eventv1alpha1.ConnectionEventData {
	reason := d.LocalCloseReason
	if reason == "" && d.Error != nil {
		reason = d.Error.Error()
	}
	data := &eventv1alpha1.ConnectionEventData{
		Direction: eventv1alpha1.ConnectionDirection_CONNECTION_DIRECTION_UNSPECIFIED,
		Action:    eventv1alpha1.ConnectionAction_CONNECTION_ACTION_CLOSED,
		Reason:    reason,
	}
	if d.ConnectionId.RemoteAddr != nil {
		data.Address = d.ConnectionId.RemoteAddr.String()
	}
	return data
}

// eraNameForBlockType names the era a block type belongs to, or "" for a type
// this build does not know.
func eraNameForBlockType(blockType uint) string {
	switch blockType {
	case ledgerbyron.BlockTypeByronEbb, ledgerbyron.BlockTypeByronMain:
		return "byron"
	case gledger.BlockTypeShelley:
		return "shelley"
	case gledger.BlockTypeAllegra:
		return "allegra"
	case gledger.BlockTypeMary:
		return "mary"
	case gledger.BlockTypeAlonzo:
		return "alonzo"
	case gledger.BlockTypeBabbage:
		return "babbage"
	case gledger.BlockTypeConway:
		return "conway"
	case gledger.BlockTypeDijkstra:
		return "dijkstra"
	}
	return ""
}
