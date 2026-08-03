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
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// eraKind selects the block layout, header body shape, and VRF input
// scheme BuildBlock must emit. The choice comes from the pparams type
// rather than the major version, which can legitimately be zero in
// synthetic test pparams.
type eraKind uint8

const (
	eraShelley eraKind = iota + 1
	eraAllegra
	eraMary
	eraAlonzo
	eraBabbage
	eraConway
	eraDijkstra
)

// isTPraos reports whether the era runs on TPraos (Shelley→Alonzo).
// TPraos eras carry two VRF proofs (nonce + leader) in flat header
// fields; Praos eras (Babbage/Conway) carry a single combined VRF
// proof in a struct.
func (e eraKind) isTPraos() bool {
	return e <= eraAlonzo
}

// hasInvalidTxs reports whether the block layout includes the
// invalid_transactions list. Plutus arrived in Alonzo, so the list
// is present from Alonzo onward.
func (e eraKind) hasInvalidTxs() bool {
	return e >= eraAlonzo
}

// usesIndefInvalidList reports whether invalid_transactions is encoded
// as a CBOR indefinite-length list. Only Alonzo does this on-chain;
// Babbage and Conway emit definite-length lists.
func (e eraKind) usesIndefInvalidList() bool {
	return e == eraAlonzo
}

// pparamsLimits collects the protocol-parameter values BuildBlock reads.
// It is filled by extractPParamsLimits via per-era type-switch so the
// builder does not depend on a single era's pparams struct.
type pparamsLimits struct {
	era          eraKind
	maxTxSize    uint64
	maxBlockSize uint64
	maxExUnits   lcommon.ExUnits // zero for pre-Alonzo eras (no Plutus)
	protoMajor   uint64
	protoMinor   uint64
}

// extractPParamsLimits returns the BuildBlock-relevant fields from any
// era's protocol parameters. Returns an error only if pparams is nil
// or of an unrecognized type.
func extractPParamsLimits(p lcommon.ProtocolParameters) (pparamsLimits, error) {
	if p == nil {
		return pparamsLimits{}, errors.New("protocol parameters are nil")
	}
	switch pp := p.(type) {
	case *dijkstra.DijkstraProtocolParameters:
		// Dijkstra shares Conway's Praos header/pparam limits; its
		// block-body layout is handled later by the era-specific encoder.
		return pparamsLimits{
			era:          eraDijkstra,
			maxTxSize:    uint64(pp.MaxTxSize),
			maxBlockSize: uint64(pp.MaxBlockBodySize),
			maxExUnits:   pp.MaxBlockExUnits,
			protoMajor:   uint64(pp.ProtocolVersion.Major),
			protoMinor:   uint64(pp.ProtocolVersion.Minor),
		}, nil
	case *conway.ConwayProtocolParameters:
		return pparamsLimits{
			era:          eraConway,
			maxTxSize:    uint64(pp.MaxTxSize),
			maxBlockSize: uint64(pp.MaxBlockBodySize),
			maxExUnits:   pp.MaxBlockExUnits,
			protoMajor:   uint64(pp.ProtocolVersion.Major),
			protoMinor:   uint64(pp.ProtocolVersion.Minor),
		}, nil
	case *babbage.BabbageProtocolParameters:
		return pparamsLimits{
			era:          eraBabbage,
			maxTxSize:    uint64(pp.MaxTxSize),
			maxBlockSize: uint64(pp.MaxBlockBodySize),
			maxExUnits:   pp.MaxBlockExUnits,
			protoMajor:   uint64(pp.ProtocolMajor),
			protoMinor:   uint64(pp.ProtocolMinor),
		}, nil
	case *alonzo.AlonzoProtocolParameters:
		return pparamsLimits{
			era:          eraAlonzo,
			maxTxSize:    uint64(pp.MaxTxSize),
			maxBlockSize: uint64(pp.MaxBlockBodySize),
			maxExUnits:   pp.MaxBlockExUnits,
			protoMajor:   uint64(pp.ProtocolMajor),
			protoMinor:   uint64(pp.ProtocolMinor),
		}, nil
	case *mary.MaryProtocolParameters:
		return pparamsLimits{
			era:          eraMary,
			maxTxSize:    uint64(pp.MaxTxSize),
			maxBlockSize: uint64(pp.MaxBlockBodySize),
			protoMajor:   uint64(pp.ProtocolMajor),
			protoMinor:   uint64(pp.ProtocolMinor),
		}, nil
	case *shelley.ShelleyProtocolParameters:
		// AllegraProtocolParameters is a Go type alias for the
		// Shelley pparams struct, so this case matches both eras.
		// Distinguish by major version: Shelley is 2, Allegra is 3.
		era := eraShelley
		if pp.ProtocolMajor >= 3 {
			era = eraAllegra
		}
		return pparamsLimits{
			era:          era,
			maxTxSize:    uint64(pp.MaxTxSize),
			maxBlockSize: uint64(pp.MaxBlockBodySize),
			protoMajor:   uint64(pp.ProtocolMajor),
			protoMinor:   uint64(pp.ProtocolMinor),
		}, nil
	default:
		return pparamsLimits{}, fmt.Errorf(
			"unsupported protocol parameter type %T", p,
		)
	}
}

// decodeMempoolTx decodes a mempool transaction using the era-specific
// constructor selected by mempoolTx.Type. The mempool tags each tx with
// its era at admit time, so this layer trusts that tag.
func decodeMempoolTx(mempoolTx MempoolTransaction) (ledger.Transaction, error) {
	tx, err := ledger.NewTransactionFromCbor(mempoolTx.Type, mempoolTx.Cbor)
	if err != nil {
		return nil, fmt.Errorf(
			"decode tx (type=%d): %w", mempoolTx.Type, err,
		)
	}
	return tx, nil
}

// splitTxCbor decomposes a transaction's CBOR into the raw body and
// witness-set bytes. Working at the byte level keeps block assembly
// era-agnostic: we don't need typed body / witness slices once we
// have the canonical encoded forms.
//
// Pre-Alonzo (Shelley/Allegra/Mary) txs are 2- or 3-element arrays
// ([body, witnesses] or [body, witnesses, metadata]); Alonzo+ txs are
// 4-element arrays ([body, witnesses, isValid, auxData]). We accept
// both and always pull positions 0 and 1.
func splitTxCbor(txCbor []byte) (body, witnesses cbor.RawMessage, err error) {
	var parts []cbor.RawMessage
	if _, decErr := cbor.Decode(txCbor, &parts); decErr != nil {
		return nil, nil, fmt.Errorf("decode tx as array: %w", decErr)
	}
	if len(parts) < 2 || len(parts) > 4 {
		return nil, nil, fmt.Errorf(
			"expected 2-4 element tx array, got %d", len(parts),
		)
	}
	return parts[0], parts[1], nil
}

// dijkstraBlockTransactionCbor returns the Dijkstra block-body form of a
// transaction. Local tx submission may admit the 4-field mempool shape
// [body, witnesses, is_valid, aux], but Dijkstra blocks store inline
// transactions as [body, witnesses, aux] and express phase-2 failures through
// the block body's invalid_transactions set.
func dijkstraBlockTransactionCbor(
	txCbor []byte,
) (cbor.RawMessage, error) {
	var parts []cbor.RawMessage
	if _, decErr := cbor.Decode(txCbor, &parts); decErr != nil {
		return nil, fmt.Errorf("decode Dijkstra tx as array: %w", decErr)
	}
	switch len(parts) {
	case 3:
		return cbor.RawMessage(txCbor), nil
	case 4:
		var isValid bool
		if _, decErr := cbor.Decode(parts[2], &isValid); decErr != nil {
			return nil, fmt.Errorf("decode Dijkstra is_valid: %w", decErr)
		}
		if !isValid {
			return nil, errors.New("dijkstra admitted transaction has is_valid=false")
		}
		blockTxCbor, err := cbor.Encode([]cbor.RawMessage{
			parts[0],
			parts[1],
			parts[3],
		})
		if err != nil {
			return nil, fmt.Errorf(
				"encode Dijkstra block transaction: %w",
				err,
			)
		}
		return cbor.RawMessage(blockTxCbor), nil
	default:
		return nil, fmt.Errorf(
			"expected 3-4 element Dijkstra tx array, got %d",
			len(parts),
		)
	}
}

// decodeBlockFromCbor re-decodes a freshly-encoded block via the
// constructor matching the era we emitted for. Hash() and other
// accessors rely on the original CBOR being stored on the typed
// block, so we round-trip through the matching era's decoder.
func decodeBlockFromCbor(era eraKind, blockCbor []byte) (ledger.Block, error) {
	switch era {
	case eraDijkstra:
		return dijkstra.NewDijkstraBlockFromCbor(blockCbor)
	case eraConway:
		return conway.NewConwayBlockFromCbor(blockCbor)
	case eraBabbage:
		return babbage.NewBabbageBlockFromCbor(blockCbor)
	case eraAlonzo:
		return alonzo.NewAlonzoBlockFromCbor(blockCbor)
	case eraMary:
		return mary.NewMaryBlockFromCbor(blockCbor)
	case eraAllegra:
		return allegra.NewAllegraBlockFromCbor(blockCbor)
	case eraShelley:
		return shelley.NewShelleyBlockFromCbor(blockCbor)
	default:
		return nil, fmt.Errorf("unknown era kind: %d", era)
	}
}

// forgedBlockDiagnostics renders a freshly-encoded block in Cardano-aware
// CBOR diagnostic notation, for logging when the re-decode in BuildBlock
// fails. The notation labels each top-level block field — header,
// transaction_bodies, transaction_witnesses, auxiliary_data_set,
// invalid_transactions — and shows its CBOR shape, so a structural
// encoder/decoder mismatch (e.g. a field serialized as a map where the
// decoder expects an array, the failure mode in issue #2063) is visible
// at a glance instead of hidden behind a generic unmarshal error.
//
// Array fan-out and nesting depth are capped to keep the dump bounded;
// this runs only on the (expected-never) decode-failure path. Any error
// from the formatter is folded into the returned string so logging the
// diagnostics can never itself fail the forge.
func forgedBlockDiagnostics(blockCbor []byte) string {
	opts := cbor.DiagnosticOptions{
		CardanoAware:  true,
		MaxArrayItems: 8,
		MaxDepth:      6,
		MaxByteLength: 64,
	}
	if diag, err := cbor.FormatBlockDiagnostic(blockCbor, opts); err == nil {
		return diag
	}
	// The block-aware formatter rejects a malformed envelope outright;
	// fall back to a generic diagnostic so we still learn the shape.
	if res, err := cbor.Diagnose(blockCbor, opts); err == nil &&
		res.Root != nil {
		return res.Root.FormatDiagnosticPretty(opts)
	}
	return fmt.Sprintf("<diagnostics unavailable for %d-byte block>", len(blockCbor))
}
