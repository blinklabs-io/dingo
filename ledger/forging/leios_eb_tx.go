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
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
)

// leiosBlockTransactionCbor converts a mempool transaction into the Dijkstra
// block_transaction an endorser block carries,
// [body, witnesses, auxiliary_data / null, is_valid].
//
// A mempool transaction is either [body, witnesses, auxiliary_data] or, with
// the validity flag before the auxiliary data, [body, witnesses, is_valid,
// auxiliary_data]. The mempool only admits transactions that pass phase-2
// validation, so is_valid is always true there and a false flag in mempool
// form is rejected rather than forwarded. A four-element input whose flag is
// already last is a block_transaction and is returned unchanged.
func leiosBlockTransactionCbor(txCbor []byte) ([]byte, error) {
	var parts []cbor.RawMessage
	if _, err := cbor.Decode(txCbor, &parts); err != nil {
		return nil, fmt.Errorf("decode transaction: %w", err)
	}
	isBool := func(raw cbor.RawMessage) bool {
		return len(raw) == 1 && (raw[0] == 0xf4 || raw[0] == 0xf5)
	}
	switch len(parts) {
	case 3:
		parts = append(parts, cbor.RawMessage{0xf5})
	case 4:
		switch {
		case isBool(parts[2]):
			if parts[2][0] == 0xf4 {
				return nil, errors.New("mempool transaction is marked invalid")
			}
			parts[2], parts[3] = parts[3], parts[2]
		case isBool(parts[3]):
			return txCbor, nil
		default:
			return nil, errors.New("transaction has no is_valid flag")
		}
	default:
		return nil, fmt.Errorf(
			"transaction has %d elements, expected 3 or 4",
			len(parts),
		)
	}
	out, err := cbor.Encode(parts)
	if err != nil {
		return nil, fmt.Errorf("encode block transaction: %w", err)
	}
	return out, nil
}
