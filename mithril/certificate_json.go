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
//
// The certificate message shape is ported from mithril-common 0.8.3
// (src/messages/certificate.rs, src/entities/certificate_metadata.rs,
// src/entities/protocol_message.rs) at Mithril release 2642.0
// (e85df3ddc52b528c7b9ddbdddf1941fec6d5f348), by way of mithril2pq
// (internal/mithril/chain/strict.go).
// Mithril: Copyright 2022 Input Output (Hong Kong) Ltd., Apache-2.0.
// mithril2pq: Copyright 2026 Chris Guiney, Apache-2.0.
// Modifications: translated from Rust to Go.

package mithril

import (
	"encoding/json"
	"fmt"
	"slices"
)

// certificateSerdeShape is the certificate message as mithril-client
// decodes it.
var certificateSerdeShape = serdeField{kind: serdeObject, fields: []serdeField{
	{name: "hash", kind: serdeString},
	{name: "previous_hash", kind: serdeString},
	{name: "epoch", kind: serdeU64},
	// Mithril reads any value here (unknown types become Unknown) and
	// rejects an unusable one only where it needs the beacon; so does
	// SignedEntityType.
	{name: "signed_entity_type", kind: serdeAny, null: true},
	{name: "metadata", kind: serdeObject, fields: []serdeField{
		{name: "network", kind: serdeString},
		{name: "version", kind: serdeString},
		{name: "parameters", kind: serdeObject, fields: []serdeField{
			{name: "k", kind: serdeU64},
			{name: "m", kind: serdeU64},
			{name: "phi_f", kind: serdeF64},
		}},
		{name: "initiated_at", kind: serdeString},
		{name: "sealed_at", kind: serdeString},
		{name: "signers", kind: serdeArray, fields: []serdeField{{
			kind: serdeObject,
			fields: []serdeField{
				{name: "party_id", kind: serdeString},
				{name: "stake", kind: serdeU64},
			},
		}}},
	}},
	{name: "protocol_message", kind: serdeObject, fields: []serdeField{
		{name: "message_parts", kind: serdeStringMap},
		// serde(default): may be absent, but not null. Only the legacy
		// scheme exists outside Mithril's SNARK builds.
		{
			name:   "hash_scheme",
			kind:   serdeString,
			absent: true,
			values: []string{"legacy"},
		},
	}},
	{name: "signed_message", kind: serdeString},
	{name: "aggregate_verification_key", kind: serdeString},
	{name: "multi_signature", kind: serdeString},
	{name: "genesis_signature", kind: serdeString},
	{
		name:   "ancillary_prover_data",
		kind:   serdeString,
		absent: true,
		null:   true,
	},
	{
		name:   "ancillary_verifier_data",
		kind:   serdeString,
		absent: true,
		null:   true,
	},
}}

// UnmarshalJSON decodes a certificate message as mithril-client does: exact
// keys, no repeated fields, every field present and non-null unless Mithril
// makes it optional, known protocol message parts, and timestamps chrono
// reads.
func (c *Certificate) UnmarshalJSON(data []byte) error {
	clean, err := serdeJSON(data, &certificateSerdeShape)
	if err != nil {
		return fmt.Errorf("certificate message: %w", err)
	}
	type plain Certificate
	var out plain
	if err := json.Unmarshal(clean, &out); err != nil {
		return fmt.Errorf("certificate message: %w", err)
	}
	for k := range out.ProtocolMessage.MessageParts {
		if !slices.Contains(protocolMessageHashOrder, k) {
			return fmt.Errorf(
				"certificate message: protocol message has unknown part %q",
				k,
			)
		}
	}
	for _, ts := range []string{
		out.Metadata.InitiatedAt,
		out.Metadata.SealedAt,
	} {
		if _, err := chronoUnixNanos(ts); err != nil {
			return fmt.Errorf("certificate message: %w", err)
		}
	}
	*c = Certificate(out)
	return nil
}
