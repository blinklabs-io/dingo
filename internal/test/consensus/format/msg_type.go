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

import "fmt"

// requiredFields describes which ServedMessage payload fields must be
// populated for a given protocol + msg_type. validateServedMessage
// also rejects any field outside this set ("none" means no fields are
// allowed beyond Protocol + MsgType).
type fieldSet struct {
	era        bool
	headerCbor bool
	tip        bool
	point      bool
	points     bool
	start      bool
	end        bool
	blockCbor  bool
}

var msgSchemas = map[Protocol]map[string]fieldSet{
	ProtocolChainSync: {
		ChainSyncMsgRequestNext:       {},
		ChainSyncMsgAwaitReply:        {},
		ChainSyncMsgRollForward:       {era: true, headerCbor: true, tip: true},
		ChainSyncMsgRollBackward:      {point: true, tip: true},
		ChainSyncMsgFindIntersect:     {points: true},
		ChainSyncMsgIntersectFound:    {point: true, tip: true},
		ChainSyncMsgIntersectNotFound: {tip: true},
		ChainSyncMsgDone:              {},
	},
	ProtocolBlockFetch: {
		BlockFetchMsgRequestRange: {start: true, end: true},
		BlockFetchMsgClientDone:   {},
		BlockFetchMsgStartBatch:   {},
		BlockFetchMsgNoBlocks:     {},
		BlockFetchMsgBlock:        {blockCbor: true},
		BlockFetchMsgBatchDone:    {},
	},
}

// validateServedMessage checks that m.Protocol/m.MsgType are
// recognised and that exactly the right payload fields are populated.
func validateServedMessage(m ServedMessage) error {
	byMsgType, ok := msgSchemas[m.Protocol]
	if !ok {
		return fmt.Errorf("unknown protocol %q", m.Protocol)
	}
	schema, ok := byMsgType[m.MsgType]
	if !ok {
		return fmt.Errorf(
			"unknown %s message %q", m.Protocol, m.MsgType,
		)
	}

	// Required-field presence.
	if schema.era && m.Era == nil {
		return fmt.Errorf("%s/%s: era required", m.Protocol, m.MsgType)
	}
	if schema.headerCbor && len(m.HeaderCbor) == 0 {
		return fmt.Errorf("%s/%s: header_cbor required",
			m.Protocol, m.MsgType,
		)
	}
	if schema.tip && m.Tip == nil {
		return fmt.Errorf("%s/%s: tip required", m.Protocol, m.MsgType)
	}
	if schema.point && m.Point == nil {
		return fmt.Errorf("%s/%s: point required",
			m.Protocol, m.MsgType,
		)
	}
	if schema.points && len(m.Points) == 0 {
		return fmt.Errorf("%s/%s: points required",
			m.Protocol, m.MsgType,
		)
	}
	if schema.start && m.Start == nil {
		return fmt.Errorf("%s/%s: start required",
			m.Protocol, m.MsgType,
		)
	}
	if schema.end && m.End == nil {
		return fmt.Errorf("%s/%s: end required",
			m.Protocol, m.MsgType,
		)
	}
	if schema.blockCbor && len(m.BlockCbor) == 0 {
		return fmt.Errorf("%s/%s: block_cbor required",
			m.Protocol, m.MsgType,
		)
	}

	// Unexpected-field rejection.
	if !schema.era && m.Era != nil {
		return fmt.Errorf("%s/%s: era unexpected",
			m.Protocol, m.MsgType,
		)
	}
	if !schema.headerCbor && len(m.HeaderCbor) > 0 {
		return fmt.Errorf("%s/%s: header_cbor unexpected",
			m.Protocol, m.MsgType,
		)
	}
	if !schema.tip && m.Tip != nil {
		return fmt.Errorf("%s/%s: tip unexpected",
			m.Protocol, m.MsgType,
		)
	}
	if !schema.point && m.Point != nil {
		return fmt.Errorf("%s/%s: point unexpected",
			m.Protocol, m.MsgType,
		)
	}
	if !schema.points && len(m.Points) > 0 {
		return fmt.Errorf("%s/%s: points unexpected",
			m.Protocol, m.MsgType,
		)
	}
	if !schema.start && m.Start != nil {
		return fmt.Errorf("%s/%s: start unexpected",
			m.Protocol, m.MsgType,
		)
	}
	if !schema.end && m.End != nil {
		return fmt.Errorf("%s/%s: end unexpected",
			m.Protocol, m.MsgType,
		)
	}
	if !schema.blockCbor && len(m.BlockCbor) > 0 {
		return fmt.Errorf("%s/%s: block_cbor unexpected",
			m.Protocol, m.MsgType,
		)
	}
	return nil
}
