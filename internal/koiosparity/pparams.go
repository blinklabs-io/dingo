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

package koiosparity

import (
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// DingoProtocolParams is the era-independent view of the protocol parameters
// Dingo has in force for one epoch, decoded from the `pparams` row that
// actually applies to it (see GetProtocolParams on RewardParitySource).
//
// Every value is a string, and "" means "this era does not define this
// parameter" — never "zero" and never "unknown". Integer parameters always
// format to at least "0", so an empty string is unambiguous, which is what
// lets CompareEpochProtocolParams report a presence disagreement between the
// two sides instead of silently skipping a field. Rationals are kept in
// Dingo's exact num/denom form ("577/10000") rather than converted to a
// float: Koios publishes the same number as a decimal ("0.0577") and the
// comparison reconciles the two with rationalsEqual, so nothing is rounded on
// either side (dingo #3931).
type DingoProtocolParams struct {
	// SourceEpoch is the epoch of the `pparams` row this view was decoded
	// from, which is <= the requested epoch: Dingo stores one row per
	// parameter *change*, not one per epoch (preview has ~12 rows spanning
	// 400+ epochs), so most epochs resolve to an older row. Recorded so a
	// mismatch report can say which stored change was in force.
	SourceEpoch uint64
	// EraID/EraName identify the era the row was decoded as — the era the
	// `epoch` table says is in force for the requested epoch, not the era of
	// whichever row happened to be inserted last. See GetProtocolParams.
	EraID   uint
	EraName string

	// Shelley-family parameters, defined in every post-Byron era.
	MinFeeA            string
	MinFeeB            string
	MaxBlockBodySize   string
	MaxTxSize          string
	MaxBlockHeaderSize string
	KeyDeposit         string
	PoolDeposit        string
	MaxEpoch           string
	NOpt               string
	A0                 string // rational
	Rho                string // rational
	Tau                string // rational
	ProtocolMajor      string
	ProtocolMinor      string
	MinPoolCost        string

	// Alonzo-and-later parameters. These gate phase-2 (script) validation and
	// are "" in Shelley/Allegra/Mary, where they do not exist.
	PriceMem             string // rational
	PriceStep            string // rational
	MaxTxExMem           string
	MaxTxExSteps         string
	MaxBlockExMem        string
	MaxBlockExSteps      string
	MaxValueSize         string
	CollateralPercentage string
	MaxCollateralInputs  string

	// CostModels holds the per-language Plutus operation prices, keyed by the
	// same language names Koios uses ("PlutusV1", "PlutusV2", ...) rather
	// than Dingo's stored numeric keys, so the two sides are directly
	// comparable. nil in Shelley/Allegra/Mary, where no scripts are priced.
	CostModels map[string][]int64
}

// decodeProtocolParams decodes one stored `pparams` CBOR blob as the given
// era and flattens it into the era-independent view above.
//
// eraID selects the decoder, and must be the era the epoch is actually in —
// each era's parameter set is a CBOR array with an era-specific element
// count, so decoding with the wrong era either fails outright or produces a
// different parameter layout. Callers resolve it from the `epoch` table; see
// GetProtocolParams.
func decodeProtocolParams(
	cborBytes []byte,
	eraID uint,
	sourceEpoch uint64,
) (*DingoProtocolParams, error) {
	era := eras.GetEraById(eraID)
	if era == nil {
		return nil, fmt.Errorf("unknown era ID %d", eraID)
	}
	if era.DecodePParamsFunc == nil {
		// Byron defines no decoder because it has no protocol-parameter CBOR
		// of the Shelley-family shape to decode.
		return nil, fmt.Errorf(
			"era %s (%d) has no protocol parameter decoder",
			era.Name,
			eraID,
		)
	}
	pparams, err := era.DecodePParamsFunc(cborBytes)
	if err != nil {
		return nil, fmt.Errorf(
			"decode %s protocol parameters from epoch %d row: %w",
			era.Name,
			sourceEpoch,
			err,
		)
	}
	out, err := protocolParamsFromNative(pparams)
	if err != nil {
		return nil, err
	}
	out.SourceEpoch = sourceEpoch
	out.EraID = eraID
	out.EraName = era.Name
	return out, nil
}

// protocolParamsFromNative flattens a decoded era-specific parameter struct.
//
// A per-era switch is deliberate rather than routing through
// ProtocolParameters.Utxorpc(): the values compared here are the ones the
// node's own validation rules read straight off these structs, so reading
// them the same way keeps the parity check measuring what the node actually
// enforces instead of a re-encoded projection of it. The cost is that a new
// era must be added here — which is why the default case is an error and not
// a silent empty result: an unhandled era must surface as a dingo_db_error,
// never as a PASS with nothing compared.
func protocolParamsFromNative(
	pparams lcommon.ProtocolParameters,
) (*DingoProtocolParams, error) {
	out := &DingoProtocolParams{}
	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		// Also covers Allegra: allegra.AllegraProtocolParameters is a type
		// alias for the Shelley struct, so it lands in this case.
		fillShelleyFamilyParams(
			out,
			pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize,
			pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch,
			pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolMajor, pp.ProtocolMinor,
			pp.MinPoolCost,
		)
	case *mary.MaryProtocolParameters:
		fillShelleyFamilyParams(
			out,
			pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize,
			pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch,
			pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolMajor, pp.ProtocolMinor,
			pp.MinPoolCost,
		)
	case *alonzo.AlonzoProtocolParameters:
		fillShelleyFamilyParams(
			out,
			pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize,
			pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch,
			pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolMajor, pp.ProtocolMinor,
			pp.MinPoolCost,
		)
		fillPlutusParams(
			out, pp.ExecutionCosts, pp.MaxTxExUnits, pp.MaxBlockExUnits,
			pp.MaxValueSize, pp.CollateralPercentage, pp.MaxCollateralInputs,
			pp.CostModels,
		)
	case *babbage.BabbageProtocolParameters:
		fillShelleyFamilyParams(
			out,
			pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize,
			pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch,
			pp.NOpt, pp.A0, pp.Rho, pp.Tau, pp.ProtocolMajor, pp.ProtocolMinor,
			pp.MinPoolCost,
		)
		fillPlutusParams(
			out, pp.ExecutionCosts, pp.MaxTxExUnits, pp.MaxBlockExUnits,
			pp.MaxValueSize, pp.CollateralPercentage, pp.MaxCollateralInputs,
			pp.CostModels,
		)
	case *conway.ConwayProtocolParameters:
		fillConwayFamilyParams(out, pp)
	case *dijkstra.DijkstraProtocolParameters:
		// Dijkstra embeds the Conway parameter set by value and adds
		// reference-script fields, none of which are compared here.
		fillConwayFamilyParams(out, &pp.ConwayProtocolParameters)
	default:
		return nil, fmt.Errorf(
			"unsupported protocol parameters type %T",
			pparams,
		)
	}
	return out, nil
}

func fillConwayFamilyParams(
	out *DingoProtocolParams,
	pp *conway.ConwayProtocolParameters,
) {
	fillShelleyFamilyParams(
		out,
		pp.MinFeeA, pp.MinFeeB, pp.MaxBlockBodySize, pp.MaxTxSize,
		pp.MaxBlockHeaderSize, pp.KeyDeposit, pp.PoolDeposit, pp.MaxEpoch,
		pp.NOpt, pp.A0, pp.Rho, pp.Tau,
		pp.ProtocolVersion.Major, pp.ProtocolVersion.Minor,
		pp.MinPoolCost,
	)
	fillPlutusParams(
		out, pp.ExecutionCosts, pp.MaxTxExUnits, pp.MaxBlockExUnits,
		pp.MaxValueSize, pp.CollateralPercentage, pp.MaxCollateralInputs,
		pp.CostModels,
	)
}

//nolint:revive // the parameter list mirrors the flat era struct it reads
func fillShelleyFamilyParams(
	out *DingoProtocolParams,
	minFeeA, minFeeB, maxBlockBodySize, maxTxSize, maxBlockHeaderSize,
	keyDeposit, poolDeposit, maxEpoch, nOpt uint,
	a0, rho, tau *cbor.Rat,
	protocolMajor, protocolMinor uint,
	minPoolCost uint64,
) {
	out.MinFeeA = uintString(minFeeA)
	out.MinFeeB = uintString(minFeeB)
	out.MaxBlockBodySize = uintString(maxBlockBodySize)
	out.MaxTxSize = uintString(maxTxSize)
	out.MaxBlockHeaderSize = uintString(maxBlockHeaderSize)
	out.KeyDeposit = uintString(keyDeposit)
	out.PoolDeposit = uintString(poolDeposit)
	out.MaxEpoch = uintString(maxEpoch)
	out.NOpt = uintString(nOpt)
	out.A0 = ratString(a0)
	out.Rho = ratString(rho)
	out.Tau = ratString(tau)
	out.ProtocolMajor = uintString(protocolMajor)
	out.ProtocolMinor = uintString(protocolMinor)
	out.MinPoolCost = strconv.FormatUint(minPoolCost, 10)
}

func fillPlutusParams(
	out *DingoProtocolParams,
	executionCosts lcommon.ExUnitPrice,
	maxTxExUnits, maxBlockExUnits lcommon.ExUnits,
	maxValueSize, collateralPercentage, maxCollateralInputs uint,
	costModels map[uint][]int64,
) {
	out.CostModels = namedCostModels(costModels)
	out.PriceMem = ratString(executionCosts.MemPrice)
	out.PriceStep = ratString(executionCosts.StepPrice)
	out.MaxTxExMem = strconv.FormatInt(maxTxExUnits.Memory, 10)
	out.MaxTxExSteps = strconv.FormatInt(maxTxExUnits.Steps, 10)
	out.MaxBlockExMem = strconv.FormatInt(maxBlockExUnits.Memory, 10)
	out.MaxBlockExSteps = strconv.FormatInt(maxBlockExUnits.Steps, 10)
	out.MaxValueSize = uintString(maxValueSize)
	out.CollateralPercentage = uintString(collateralPercentage)
	out.MaxCollateralInputs = uintString(maxCollateralInputs)
}

// namedCostModels re-keys Dingo's stored cost models from the numeric
// language identifiers in the CBOR (0, 1, 2, ...) to the names Koios and
// Blockfrost publish, so CompareEpochProtocolParams can match them up
// directly. The mapping matches api/blockfrost's plutusVersionName, which
// serves the same values over Dingo's own API; it is duplicated rather than
// imported because internal/koiosparity must not depend on the API layer.
//
// An unrecognised identifier still gets a name ("PlutusV8" for key 7) rather
// than being dropped: a language Dingo prices and Koios does not is a real
// finding, and silently discarding it would hide exactly that.
func namedCostModels(models map[uint][]int64) map[string][]int64 {
	if len(models) == 0 {
		return nil
	}
	out := make(map[string][]int64, len(models))
	for language, model := range models {
		out[plutusLanguageName(language)] = model
	}
	return out
}

func plutusLanguageName(language uint) string {
	// #nosec G115 -- language is a small CBOR map key
	return "PlutusV" + strconv.FormatUint(uint64(language)+1, 10)
}

func uintString(v uint) string {
	return strconv.FormatUint(uint64(v), 10)
}

// ratString renders a stored rational exactly ("577/10000"). A nil rational
// renders as "" — the same "this parameter is not defined" signal an
// era-gated field uses — so CompareEpochProtocolParams reports it against
// whatever Koios published rather than comparing a fabricated zero.
func ratString(r *cbor.Rat) string {
	if r == nil || r.Rat == nil {
		return ""
	}
	return r.String()
}
