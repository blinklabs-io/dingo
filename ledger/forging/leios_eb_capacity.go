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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// leiosEBReferenceHashBytes is the encoded size of a manifest reference's
// transaction hash: a two-byte byte-string head and the 32-byte hash.
const leiosEBReferenceHashBytes = 2 + 32

// effectiveLeiosEBCaps bounds the operator's endorser-block caps by the
// capacity the protocol parameters in force at the slot allow:
//
//   - maxEndorserBlockTxsSize bounds the total size of the referenced
//     transactions. The reference node never fetches the closure of an
//     endorser block over it (ExnLeiosClosureTooBig in ouroboros-consensus
//     LeiosDemoLogic.processLeiosBlock), so no honest committee member can
//     validate or vote for one, and it can never be certified.
//   - maxEndorserBlockReferencesSize bounds the encoded manifest, which is
//     the size a ranking-block header announces; a header announcing more is
//     invalid (LeiosEbTooBig, dijkstra.ValidateLeiosAnnouncementSize).
//
// A zero operator cap means unlimited, and a zero protocol limit, or
// parameters of an era without endorser blocks, leaves the operator cap.
func effectiveLeiosEBCaps(
	operator leiosEBCaps,
	pparams lcommon.ProtocolParameters,
) leiosEBCaps {
	pp, ok := pparams.(*dijkstra.DijkstraProtocolParameters)
	if !ok || pp == nil {
		return operator
	}
	caps := operator
	caps.maxBytes = minNonZeroCap(
		caps.maxBytes,
		uint64(pp.MaxEndorserBlockTxsSize),
	)
	caps.maxManifestBytes = minNonZeroCap(
		caps.maxManifestBytes,
		uint64(pp.MaxEndorserBlockReferencesSize),
	)
	return caps
}

// minNonZeroCap returns the smaller of two caps, where zero is unlimited.
func minNonZeroCap(a, b uint64) uint64 {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	default:
		return min(a, b)
	}
}

// leiosEBCapacity tracks how much of an endorser block's capacity the
// references admitted so far use.
type leiosEBCapacity struct {
	caps leiosEBCaps
	refs uint64
	// txBytes is the total size of the referenced transactions.
	txBytes uint64
	// entryBytes is the encoded size of the manifest's map entries, without
	// the map head.
	entryBytes uint64
}

// admit reports whether a transaction of txSize block_transaction bytes fits
// the remaining capacity, and accounts for it when it does.
func (c *leiosEBCapacity) admit(txSize uint64) bool {
	if c.caps.maxRefs > 0 && c.refs >= c.caps.maxRefs {
		return false
	}
	txBytes := c.txBytes + txSize
	if c.caps.maxBytes > 0 && txBytes > c.caps.maxBytes {
		return false
	}
	entryBytes := c.entryBytes + leiosEBReferenceHashBytes +
		cborHeadSize(txSize)
	if c.caps.maxManifestBytes > 0 &&
		cborHeadSize(c.refs+1)+entryBytes > c.caps.maxManifestBytes {
		return false
	}
	c.refs++
	c.txBytes = txBytes
	c.entryBytes = entryBytes
	return true
}

// cborHeadSize is the size of the shortest CBOR head carrying argument n: an
// unsigned integer's whole encoding, or a map head for n entries.
func cborHeadSize(n uint64) uint64 {
	switch {
	case n < 24:
		return 1
	case n <= 0xff:
		return 2
	case n <= 0xffff:
		return 3
	case n <= 0xffffffff:
		return 5
	default:
		return 9
	}
}
