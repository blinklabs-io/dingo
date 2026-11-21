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
	"math"
	"math/big"

	"github.com/blinklabs-io/dingo/ledger/eras"
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
//   - maxEndorserBlockExUnits bounds the execution units summed over the
//     referenced transactions, memory and steps each, and
//     maxRefScriptSizePerEndorserBlock their reference-script bytes. Both
//     are the capacity the reference mempool partitions an endorser block
//     against (leiosEndorserBlockCapacity in ouroboros-consensus
//     Shelley/Ledger/Mempool.hs); zero is a bound that admits only
//     transactions costing nothing in that dimension.
//   - maxEndorserBlockReferencesSize bounds the encoded manifest, which is
//     the size a ranking-block header announces; a header announcing more is
//     invalid (LeiosEbTooBig, dijkstra.ValidateLeiosAnnouncementSize).
//
// A zero operator cap means unlimited. Missing or unrecognized parameters,
// and zero Dijkstra capacity, refuse EB production. Known earlier eras retain
// the operator caps for the Conway-header Leios prototype.
func effectiveLeiosEBCaps(
	operator leiosEBCaps,
	pparams lcommon.ProtocolParameters,
) (leiosEBCaps, bool) {
	return effectiveLeiosEBCapsScaled(operator, pparams, 1)
}

// effectiveLeiosEBCapsScaled is effectiveLeiosEBCaps for a producer that
// forges with only a share of the protocol's capacity: every protocol bound
// is scaled and rounded down before the operator's caps bound it further, as
// the reference scales the protocol capacity and has no other cap
// (scaleEbCapacity in ouroboros-consensus Ledger/SupportsMempool.hs). The
// operator's caps are not part of that capacity and stay as configured. A
// scale at or above one, or one that is not a number, leaves the capacity
// whole; one below zero is zero.
//
// A scaled transaction-size or manifest bound that rounds to nothing admits
// no endorser block, like the zero protocol bound it resembles, so capacity
// is then unavailable rather than unlimited.
func effectiveLeiosEBCapsScaled(
	operator leiosEBCaps,
	pparams lcommon.ProtocolParameters,
	scale float64,
) (leiosEBCaps, bool) {
	if _, err := extractPParamsLimits(pparams); err != nil {
		return leiosEBCaps{}, false
	}
	pp, ok := pparams.(*dijkstra.DijkstraProtocolParameters)
	if !ok {
		return operator, true
	}
	// Zero is a protocol bound, not the operator's unlimited sentinel. No
	// nonempty EB can fit either zero limit.
	if pp == nil || pp.MaxEndorserBlockTxsSize == 0 ||
		pp.MaxEndorserBlockReferencesSize == 0 {
		return leiosEBCaps{}, false
	}
	share := newLeiosEBScale(scale)
	txsSize := share.apply(uint64(pp.MaxEndorserBlockTxsSize))
	manifest := share.manifest(uint64(pp.MaxEndorserBlockReferencesSize))
	if txsSize == 0 || manifest == 0 {
		return leiosEBCaps{}, false
	}
	caps := operator
	caps.aggregateBounded = true
	caps.maxExUnits = lcommon.ExUnits{
		Memory: share.applyInt64(pp.MaxEndorserBlockExUnits.Memory),
		Steps:  share.applyInt64(pp.MaxEndorserBlockExUnits.Steps),
	}
	caps.maxRefScriptBytes = share.apply(
		uint64(pp.MaxRefScriptSizePerEndorserBlock),
	)
	caps.maxBytes = minNonZeroCap(caps.maxBytes, txsSize)
	caps.maxManifestBytes = minNonZeroCap(caps.maxManifestBytes, manifest)
	return caps, true
}

// leiosEBMaxFramingBytes is the widest map head the manifest's framing can
// take: the head of a count that fills 32 bits. The reference subtracts it
// once from the manifest bound to get the capacity its references share
// (leiosReferencesCapacity), and scales only that capacity.
const leiosEBMaxFramingBytes = 5

// leiosEBScale is a share of the protocol capacity, held as the exact
// rational value of the float the controller produced, as the reference
// holds it (toRational of a Double), so a product rounds down from its true
// value rather than from a float multiplication that can land on a whole
// number.
type leiosEBScale struct {
	// whole is a scale of one or more, or one that is not a number.
	whole bool
	// share is nil for a scale of zero or less.
	share *big.Rat
}

func newLeiosEBScale(scale float64) leiosEBScale {
	if math.IsNaN(scale) || scale >= 1 {
		return leiosEBScale{whole: true}
	}
	if scale <= 0 {
		return leiosEBScale{}
	}
	return leiosEBScale{share: new(big.Rat).SetFloat64(scale)}
}

// apply scales n, rounding down.
func (s leiosEBScale) apply(n uint64) uint64 {
	if s.whole {
		return n
	}
	if s.share == nil {
		return 0
	}
	scaled := new(big.Int).Mul(s.share.Num(), new(big.Int).SetUint64(n))
	return scaled.Quo(scaled, s.share.Denom()).Uint64()
}

func (s leiosEBScale) applyInt64(n int64) int64 {
	if n <= 0 {
		return n
	}
	return int64(s.apply(uint64(n))) // #nosec G115 -- at most n
}

// manifest scales a manifest bound. A bound within the framing leaves no room
// for a reference, and stays as it is.
func (s leiosEBScale) manifest(bound uint64) uint64 {
	if bound <= leiosEBMaxFramingBytes {
		return bound
	}
	return leiosEBMaxFramingBytes +
		s.apply(bound-leiosEBMaxFramingBytes)
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

// leiosEBMeasure is the resources one transaction takes from an endorser
// block beyond its manifest entry: the execution units its redeemers and its
// sub-transactions' redeemers declare, and the bytes of reference scripts its
// top-level inputs and reference inputs resolve to, counted per input as the
// ledger's non-distinct measure counts them.
type leiosEBMeasure struct {
	exUnits        lcommon.ExUnits
	refScriptBytes uint64
}

// leiosEBCapacity tracks how much of an endorser block's capacity the
// references admitted so far use.
type leiosEBCapacity struct {
	caps leiosEBCaps
	refs uint64
	// exUnits and refScriptBytes are the sums over the admitted
	// transactions, compared with the protocol bounds in caps.
	exUnits        lcommon.ExUnits
	refScriptBytes uint64
	// txBytes is the total size of the referenced transactions.
	txBytes uint64
	// entryBytes is the encoded size of the manifest's map entries, without
	// the map head.
	entryBytes uint64
}

// admit reports whether a transaction of txSize block_transaction bytes that
// declares no execution units and resolves no reference scripts fits the
// remaining capacity, and accounts for it when it does.
func (c *leiosEBCapacity) admit(txSize uint64) bool {
	return c.admitMeasured(txSize, leiosEBMeasure{})
}

// admitMeasured is admit for a transaction with a measured cost. The running
// sums must stay at or below the protocol bounds in every dimension, memory
// and steps separately, as the reference mempool's endorser-block partition
// requires of the summed measure; a bound of zero admits only transactions
// that cost nothing in that dimension.
func (c *leiosEBCapacity) admitMeasured(
	txSize uint64,
	m leiosEBMeasure,
) bool {
	if c.caps.maxRefs > 0 && c.refs >= c.caps.maxRefs {
		return false
	}
	exUnits := c.exUnits
	refScriptBytes := c.refScriptBytes
	if c.caps.aggregateBounded {
		var err error
		exUnits, err = eras.SafeAddExUnits(c.exUnits, m.exUnits)
		if err != nil ||
			exUnits.Memory > c.caps.maxExUnits.Memory ||
			exUnits.Steps > c.caps.maxExUnits.Steps {
			return false
		}
		if m.refScriptBytes > math.MaxUint64-c.refScriptBytes {
			return false
		}
		refScriptBytes += m.refScriptBytes
		if refScriptBytes > c.caps.maxRefScriptBytes {
			return false
		}
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
	c.exUnits = exUnits
	c.refScriptBytes = refScriptBytes
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
