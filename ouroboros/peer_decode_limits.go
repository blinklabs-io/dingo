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

package ouroboros

import (
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/internal/txsize"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// peerDecodeLimits are the bounds applied to peer bytes before the ledger
// decoder runs. Each is a rule the ledger itself enforces, so a value it
// refuses is one the ledger would refuse after the decode.
type peerDecodeLimits struct {
	// maxTxSize is the ledger's MaxTxSize for the transaction's era. Zero
	// means unknown.
	maxTxSize uint
	// rationalWidth is the widest tag-30 component, in significant bytes,
	// the ledger accepts as written. Zero means no raw limit applies.
	rationalWidth int
}

// peerDecodeLimits reads the limits for a transaction of txType from the
// ledger's current parameters. Without a ledger, or without parameters, no
// limit is known and nothing is refused on that account.
//
// The protocol version never decreases, so a peer block from a later epoch
// than the ledger tip is checked against a rule no stricter than its own.
func (o *Ouroboros) peerDecodeLimits(txType uint) peerDecodeLimits {
	if o.decodeLimits != nil {
		return o.decodeLimits(txType)
	}
	var limits peerDecodeLimits
	if o.ledgerState == nil {
		return limits
	}
	if maxSize, known := o.ledgerState.MaxTxSize(txType); known {
		limits.maxTxSize = maxSize
	}
	version, err := ledger.GetProtocolVersion(o.ledgerState.GetCurrentPParams())
	if err == nil {
		limits.rationalWidth = rationalWidthForVersion(version.Major)
	}
	return limits
}

// rationalWidthForVersion returns the widest tag-30 component, in significant
// bytes, that the ledger accepts as written at protocol version major, or zero
// when it accepts any width. Below version 12 the ledger bounds the reduced
// rational, so a raw component of any width can be valid. From it, the ledger
// requires each component to fit a Word64 as written.
func rationalWidthForVersion(major uint) int {
	if major >= common.ProtocolVersionDijkstra {
		return safedecode.Word64Bytes
	}
	return 0
}

// decodeTxsubmissionTx is the production txsubmissionTxDecoder. It applies
// the ledger's own rules that bound the decode's cost before the decode runs:
// the mempool's MaxTxSize check otherwise runs only after it, and the ledger
// decoder is quadratic in an oversized rational component.
func (o *Ouroboros) decodeTxsubmissionTx(
	txType uint,
	txCbor []byte,
) (common.Transaction, error) {
	limits := o.peerDecodeLimits(txType)
	if limits.maxTxSize > 0 {
		size := uint(txsize.FromCBOR(txType, txCbor)) //nolint:gosec // bounded by len(txCbor)
		if size > limits.maxTxSize {
			return nil, shelley.MaxTxSizeUtxoError{
				TxSize:    size,
				MaxTxSize: limits.maxTxSize,
			}
		}
	}
	if err := safedecode.RejectOversizedRationals(
		txCbor,
		limits.rationalWidth,
	); err != nil {
		return nil, err
	}
	return gledger.NewTransactionFromCbor(txType, txCbor)
}
