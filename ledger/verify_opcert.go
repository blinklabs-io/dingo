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

package ledger

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// opCertFromHeader extracts the operational certificate from a Praos/TPraos
// block header. The Shelley-family headers carry the opcert fields flat on the
// header body; the Babbage-family headers (Babbage/Conway/Dijkstra) nest them
// under a BabbageOpCert. Byron and unknown headers have no opcert, so ok is
// false.
func opCertFromHeader(header ledger.BlockHeader) (*ledger.OpCert, bool) {
	switch h := header.(type) {
	case *dijkstra.DijkstraBlockHeader:
		// Distinct concrete type embedding BabbageBlockHeader; a type
		// switch won't fall through to the Babbage case, so it needs an
		// explicit entry or opcert validation is skipped for Dijkstra blocks.
		return babbageOpCert(h.Body.OpCert), true
	case *shelley.ShelleyBlockHeader:
		return shelleyOpCert(
			h.Body.OpCertHotVkey,
			uint64(h.Body.OpCertSequenceNumber),
			uint64(h.Body.OpCertKesPeriod),
			h.Body.OpCertSignature,
		), true
	case *allegra.AllegraBlockHeader:
		return shelleyOpCert(
			h.Body.OpCertHotVkey,
			uint64(h.Body.OpCertSequenceNumber),
			uint64(h.Body.OpCertKesPeriod),
			h.Body.OpCertSignature,
		), true
	case *mary.MaryBlockHeader:
		return shelleyOpCert(
			h.Body.OpCertHotVkey,
			uint64(h.Body.OpCertSequenceNumber),
			uint64(h.Body.OpCertKesPeriod),
			h.Body.OpCertSignature,
		), true
	case *alonzo.AlonzoBlockHeader:
		return shelleyOpCert(
			h.Body.OpCertHotVkey,
			uint64(h.Body.OpCertSequenceNumber),
			uint64(h.Body.OpCertKesPeriod),
			h.Body.OpCertSignature,
		), true
	case *babbage.BabbageBlockHeader:
		return babbageOpCert(h.Body.OpCert), true
	case *conway.ConwayBlockHeader:
		return babbageOpCert(h.Body.OpCert), true
	default:
		return nil, false
	}
}

// shelleyOpCert takes the counter and KES period as uint64 because that is
// what cardano-ledger decodes them as (Word64 and KESPeriod{Word}) and what
// ledger.OpCert already carries. The TPraos header bodies they come from are
// gouroboros types whose declared width is the release's to choose, so the
// call sites convert; widening this signature keeps that conversion the only
// place a release change is visible and keeps it lossless in either direction.
func shelleyOpCert(
	hotVkey []byte,
	sequenceNumber uint64,
	kesPeriod uint64,
	signature []byte,
) *ledger.OpCert {
	return &ledger.OpCert{
		KesVkey:       hotVkey,
		IssueNumber:   sequenceNumber,
		KesPeriod:     kesPeriod,
		ColdSignature: signature,
	}
}

func babbageOpCert(oc babbage.BabbageOpCert) *ledger.OpCert {
	return &ledger.OpCert{
		KesVkey:       oc.HotVkey,
		IssueNumber:   uint64(oc.SequenceNumber),
		KesPeriod:     uint64(oc.KesPeriod),
		ColdSignature: oc.Signature,
	}
}

// opCertNoGapRuleApplies reports whether the operational-certificate
// over-increment (no-gap) counter rule applies to a block in the given era.
// That rule — reject a counter that skips ahead of the last seen by more than
// one — is part of the Praos protocol (Babbage onward). TPraos eras
// (Shelley–Alonzo) enforce only counter monotonicity, so a valid TPraos block
// may advance its opcert counter by more than one.
func opCertNoGapRuleApplies(eraId uint8) bool {
	return eraId >= babbage.EraIdBabbage
}

// validateOpCertCounter enforces operational-certificate counter rules for the
// pool's issuer. A counter below the last-seen value (candidate < stored)
// signals a stale or stolen hot key and is rejected in every era. A counter
// that skips ahead (candidate > stored+1) is the Praos over-increment case and
// is rejected only when enforceNoGap is set — the rule is Praos-only (Babbage
// onward); TPraos eras (Shelley–Alonzo) accept any candidate >= stored, so the
// gap check must be scoped by era rather than by validation mode. See
// opCertNoGapRuleApplies.
//
// When the pool has no recorded counter (found is false) there is no baseline
// to compare against — a genuine first sighting, or a pool that last forged
// before this node's local history begins (e.g. a Mithril-restored start) — so
// the candidate is accepted and becomes the baseline. Enforcing a baseline of
// zero here would falsely reject a valid high-counter block and stall the
// chain; the honest chain we follow already enforced monotonicity at that
// pool's real baseline.
func validateOpCertCounter(
	stored uint64,
	found bool,
	candidate uint64,
	enforceNoGap bool,
) error {
	return eras.ValidateOpCertCounter(stored, found, candidate, enforceNoGap)
}

const (
	// opCertKesVkeySize is the length of the KES (hot) verification key carried
	// in an operational certificate.
	opCertKesVkeySize = 32
	// opCertSignableSize is the length of the cardano-ledger OCertSignable
	// representation: KES vkey || counter (uint64 BE) || KES period (uint64 BE).
	opCertSignableSize = opCertKesVkeySize + 8 + 8
)

// verifyOpCertColdSignature verifies the pool cold-key signature over the
// operational certificate.
//
// The signed message is the cardano-ledger OCertSignable representation: the
// raw concatenation of the KES (hot) verification key, the issue counter as a
// big-endian uint64, and the KES period as a big-endian uint64 — NOT a CBOR
// encoding. This matches what cardano-node signs (verified byte-for-byte
// against a real cardano-cli NodeOperationalCertificate; see
// TestVerifyOpCertColdSignature_RealCardanoCliCert) and the forging-side check
// in ledger/forging/keys.go ValidateOpCert.
//
// gouroboros' ledger.VerifyOpCertSignature builds this same raw
// representation (via ledger/common.OpCertSignableBytes), so this delegates
// to it directly instead of re-deriving the byte layout locally. That was not
// always true: gouroboros previously hashed a CBOR array
// ([kes_vkey, issue_number, kes_period]) here, which does not match real
// opcerts and would have rejected every inbound block.
func verifyOpCertColdSignature(opCert *ledger.OpCert, coldVkey []byte) error {
	return ledger.VerifyOpCertSignature(opCert, coldVkey)
}

// verifyOpCertHeaderCrypto performs the stateless inbound operational
// certificate checks for a block header: the cold-key signature and the KES
// period expiry. The cold verification key is the header's issuer vkey — a
// registered pool's cold key is, by construction, the vkey whose Blake2b224
// hash is its pool id, so verifying against header.IssuerVkey() is verifying
// against the registered cold key.
//
// The counter-monotonicity check is intentionally NOT done here: it depends on
// per-pool ledger state and must be a read-before-write inside the block-apply
// transaction (see ledgerProcessBlock), where ordering and rollback are
// correct.
//
// Byron and unknown headers carry no opcert and return nil.
func verifyOpCertHeaderCrypto(
	header ledger.BlockHeader,
	slot uint64,
	slotsPerKesPeriod uint64,
	maxKesEvolutions uint64,
) error {
	opCert, ok := opCertFromHeader(header)
	if !ok {
		return nil
	}
	coldVkey := header.IssuerVkey()
	if err := verifyOpCertColdSignature(opCert, coldVkey[:]); err != nil {
		return fmt.Errorf("opcert cold-key signature invalid: %w", err)
	}
	// KES expiry needs both genesis parameters. ValidateKesPeriod errors when
	// either is zero, so when they're unavailable we fall back to the lighter
	// future-cert KES guard that VerifyKesComponents already performed inside
	// VerifyBlock rather than failing the block.
	if slotsPerKesPeriod > 0 && maxKesEvolutions > 0 {
		if _, err := ledger.ValidateKesPeriod(
			opCert.KesPeriod,
			slot,
			slotsPerKesPeriod,
			maxKesEvolutions,
		); err != nil {
			return fmt.Errorf("opcert KES period invalid: %w", err)
		}
	}
	return nil
}
