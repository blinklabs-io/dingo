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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// shelleyGenesisMainnet/TestnetNetworkId are the two values cardano-
// ledger's NetworkId enum serializes into shelley-genesis.json's
// networkId field. BBODY's `netId == Mainnet` predicate reads the
// same field. Anything else is a configuration error and we refuse to
// guess in either direction.
const (
	shelleyGenesisMainnetNetworkId = "Mainnet"
	shelleyGenesisTestnetNetworkId = "Testnet"
)

// errUnknownNetwork is returned when the network identity cannot be
// determined — Cardano node config or Shelley genesis is missing, or
// the genesis carries an unrecognized networkId. The header-version
// validator surfaces this as a validation failure rather than picking
// a silent default that could weaken or strengthen the rule.
var errUnknownNetwork = errors.New(
	"cannot determine network identity from Shelley genesis",
)

// dijkstraMajorVersion is the protocol major version at which the
// header pvMajor "too high" check becomes mandatory on every network.
// This value matches the Haskell `natVersion @12` in cardano-ledger's
// BBODY rule and the Dijkstra era major-version range in gouroboros.
const dijkstraMajorVersion uint = 12

// HeaderProtocolVersionTooHighError is returned when a block header's
// protocol major version is more than one ahead of the current pparams
// protocol major version. Mirrors cardano-ledger's HeaderProtVerTooHigh
// failure from the BBODY rule.
type HeaderProtocolVersionTooHighError struct {
	Supplied uint
	Expected uint
}

func (e *HeaderProtocolVersionTooHighError) Error() string {
	return fmt.Sprintf(
		"header protocol major version %d exceeds expected maximum %d",
		e.Supplied, e.Expected,
	)
}

// HeaderProtocolMajor extracts the protocol major version stored in a
// block header. Returns (0, false) for Byron-era headers, which use a
// different consensus mechanism (PBFT) and do not carry a Praos-style
// ProtVer field.
func HeaderProtocolMajor(header lcommon.BlockHeader) (uint, bool) {
	switch h := header.(type) {
	case *dijkstra.DijkstraBlockHeader:
		// DijkstraBlockHeader embeds BabbageBlockHeader but is a
		// distinct concrete type, so it needs its own case — a type
		// switch matches the dynamic type, not embedded promotions, and
		// would otherwise fall through to default and silently skip the
		// "too high" check for every Dijkstra-era block.
		return uint(h.Body.ProtoVersion.Major), true
	case *conway.ConwayBlockHeader:
		return uint(h.Body.ProtoVersion.Major), true
	case *babbage.BabbageBlockHeader:
		return uint(h.Body.ProtoVersion.Major), true
	case *alonzo.AlonzoBlockHeader:
		return uint(h.Body.ProtoMajorVersion), true
	case *mary.MaryBlockHeader:
		return uint(h.Body.ProtoMajorVersion), true
	case *allegra.AllegraBlockHeader:
		return uint(h.Body.ProtoMajorVersion), true
	case *shelley.ShelleyBlockHeader:
		return uint(h.Body.ProtoMajorVersion), true
	default:
		// Byron and any unknown header type — skip the check rather
		// than fabricating a value. NOTE: every new Praos-family era
		// must be given an explicit case above. A missing case lands
		// here and silently disables the "too high" check for that
		// era's blocks (see the Dijkstra case).
		return 0, false
	}
}

// ValidateHeaderProtocolVersion enforces cardano-ledger's BBODY-rule
// check that a block header's protocol major version is not more than
// one ahead of the current pparams protocol major version. A header
// equal to or one greater than current is accepted; anything beyond
// that is rejected with HeaderProtocolVersionTooHighError.
//
// The check is skipped on testnets (isMainnet == false) while the
// current pparams major version is below Dijkstra (12). This mirrors
// the relaxation introduced in cardano-ledger PR 5785 to support
// ephemeral testnets that enable experimental hard forks or rebuild
// chains in much older eras.
//
// Byron-era headers are also skipped, as they have no ProtVer field.
func ValidateHeaderProtocolVersion(
	header lcommon.BlockHeader,
	curPvMajor uint,
	isMainnet bool,
) error {
	if !isMainnet && curPvMajor < dijkstraMajorVersion {
		return nil
	}
	bhMajor, ok := HeaderProtocolMajor(header)
	if !ok {
		return nil
	}
	nextMajor := curPvMajor + 1
	if bhMajor > nextMajor {
		return &HeaderProtocolVersionTooHighError{
			Supplied: bhMajor,
			Expected: nextMajor,
		}
	}
	return nil
}

// isMainnet reports whether this LedgerState is configured for Cardano
// mainnet, derived from the Shelley genesis networkId field. This is
// the literal port of cardano-ledger's `netId == Mainnet` predicate
// used by the BBODY rule. Network magic alone is not a reliable
// discriminator (devnet shares mainnet's RequiresNoMagic wire setting);
// only the genesis-declared identity has the right semantics.
//
// Fails closed: when CardanoNodeConfig or Shelley genesis is missing,
// or the networkId field carries an unrecognized value, returns
// errUnknownNetwork rather than silently picking a default. Callers
// must treat that as a validation failure — picking either branch of
// `Mainnet || pre-Dijkstra` blindly would either bypass the rule on a
// real testnet or weaken it on mainnet.
func (ls *LedgerState) isMainnet() (bool, error) {
	if ls.config.CardanoNodeConfig == nil {
		return false, errUnknownNetwork
	}
	sg := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if sg == nil {
		return false, errUnknownNetwork
	}
	switch sg.NetworkId {
	case shelleyGenesisMainnetNetworkId:
		return true, nil
	case shelleyGenesisTestnetNetworkId:
		return false, nil
	default:
		return false, fmt.Errorf(
			"%w: unrecognized networkId %q",
			errUnknownNetwork, sg.NetworkId,
		)
	}
}

// validateBlockHeaderProtocolVersion is the LedgerState-bound entry
// point for ValidateHeaderProtocolVersion. It pulls the current
// protocol major version from pparams and the network identity from
// the Shelley genesis, then delegates the BBODY-rule check. If the
// network cannot be identified, returns the underlying error so the
// block is rejected rather than validated against a guessed network.
func (ls *LedgerState) validateBlockHeaderProtocolVersion(
	header lcommon.BlockHeader,
	pparams lcommon.ProtocolParameters,
) error {
	pv, err := GetProtocolVersion(pparams)
	if err != nil {
		return fmt.Errorf(
			"validate header protocol version: %w", err,
		)
	}
	mainnet, err := ls.isMainnet()
	if err != nil {
		return fmt.Errorf(
			"validate header protocol version: %w", err,
		)
	}
	return ValidateHeaderProtocolVersion(header, pv.Major, mainnet)
}
