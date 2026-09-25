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

package eras

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// byronLovelacePortionDenominator is the denominator of a Byron
// LovelacePortion, a rational in [0, 1] carried as its numerator.
const byronLovelacePortionDenominator = 1_000_000_000_000_000

// ByronSoftforkRule is the Byron softfork resolution rule, each threshold a
// LovelacePortion numerator over 10^15.
type ByronSoftforkRule struct {
	InitThd      uint64
	MinThd       uint64
	ThdDecrement uint64
}

// ByronProtocolParameters are the adopted Byron protocol parameters of the
// update system (Cardano.Chain.Update.ProtocolParameters). Byron genesis only
// initializes them; an adopted update proposal replaces them at an epoch
// boundary. The size and duration fields are Natural in the reference, so
// they are unbounded here too.
type ByronProtocolParameters struct {
	ScriptVersion     uint16
	SlotDuration      *big.Int
	MaxBlockSize      *big.Int
	MaxHeaderSize     *big.Int
	MaxTxSize         *big.Int
	MaxProposalSize   *big.Int
	MpcThd            uint64
	HeavyDelThd       uint64
	UpdateVoteThd     uint64
	UpdateProposalThd uint64
	// UpdateProposalTTL is ppUpdateProposalTTL, the number of slots an
	// unconfirmed proposal stays registered. Genesis calls it
	// updateImplicit.
	UpdateProposalTTL uint64
	SoftforkRule      ByronSoftforkRule
	// TxFeeSummand is the fee policy constant in lovelace, and
	// TxFeeMultiplierNano the per-byte coefficient scaled by 10^9, so the
	// multiplier is the exact rational TxFeeMultiplierNano / 10^9.
	TxFeeSummand        uint64
	TxFeeMultiplierNano *big.Int
	UnlockStakeEpoch    uint64
}

// Utxorpc satisfies lcommon.ProtocolParameters. utxorpc defines no Byron
// parameter shape.
func (p *ByronProtocolParameters) Utxorpc() (*cardano.PParams, error) {
	return &cardano.PParams{}, nil
}

// NewByronProtocolParametersFromGenesis loads the genesis blockVersionData the
// way the reference JSON decoder does. The fee summand is summand div 10^9 and
// the multiplier multiplier / 10^9 exactly (TxFeePolicy's FromJSON).
func NewByronProtocolParametersFromGenesis(
	genesis *byron.ByronGenesis,
) (*ByronProtocolParameters, error) {
	if genesis == nil {
		return nil, errors.New("byron genesis is unavailable")
	}
	data := genesis.BlockVersionData
	var errs []error
	natural := func(name string, value int64) uint64 {
		if value < 0 {
			errs = append(errs, fmt.Errorf(
				"byron genesis %s must not be negative, got %d",
				name,
				value,
			))
			return 0
		}
		return uint64(value)
	}
	params := &ByronProtocolParameters{
		SlotDuration: new(big.Int).SetUint64(
			natural("slotDuration", int64(data.SlotDuration)),
		),
		MaxBlockSize: new(big.Int).SetUint64(
			natural("maxBlockSize", int64(data.MaxBlockSize)),
		),
		MaxHeaderSize: new(big.Int).SetUint64(
			natural("maxHeaderSize", int64(data.MaxHeaderSize)),
		),
		MaxTxSize: new(big.Int).SetUint64(
			natural("maxTxSize", int64(data.MaxTxSize)),
		),
		MaxProposalSize: new(big.Int).SetUint64(
			natural("maxProposalSize", int64(data.MaxProposalSize)),
		),
		MpcThd:            natural("mpcThd", data.MpcThd),
		HeavyDelThd:       natural("heavyDelThd", data.HeavyDelThd),
		UpdateVoteThd:     natural("updateVoteThd", data.UpdateVoteThd),
		UpdateProposalThd: natural("updateProposalThd", data.UpdateProposalThd),
		UpdateProposalTTL: natural("updateImplicit", int64(data.UpdateImplicit)),
		SoftforkRule: ByronSoftforkRule{
			InitThd: natural("softforkRule.initThd", data.SoftforkRule.InitThd),
			MinThd:  natural("softforkRule.minThd", data.SoftforkRule.MinThd),
			ThdDecrement: natural(
				"softforkRule.thdDecrement",
				data.SoftforkRule.ThdDecrement,
			),
		},
		TxFeeSummand: natural(
			"txFeePolicy.summand",
			data.TxFeePolicy.Summand,
		) / byronFeePolicyScale,
		TxFeeMultiplierNano: new(big.Int).SetUint64(
			natural("txFeePolicy.multiplier", data.TxFeePolicy.Multiplier),
		),
		UnlockStakeEpoch: data.UnlockStakeEpoch,
	}
	scriptVersion := natural("scriptVersion", int64(data.ScriptVersion))
	if scriptVersion > math.MaxUint16 {
		errs = append(errs, fmt.Errorf(
			"byron genesis scriptVersion %d exceeds Word16",
			scriptVersion,
		))
	}
	//nolint:gosec // bounded by math.MaxUint16 above; an error is returned otherwise
	params.ScriptVersion = uint16(scriptVersion)
	if params.TxFeeSummand > byronMaxLovelace {
		errs = append(errs, fmt.Errorf(
			"byron genesis fee summand %d exceeds the maximum Lovelace value",
			params.TxFeeSummand,
		))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return params, nil
}

// Clone returns a deep copy.
func (p *ByronProtocolParameters) Clone() *ByronProtocolParameters {
	if p == nil {
		return nil
	}
	ret := *p
	for _, field := range []struct{ dst, src **big.Int }{
		{&ret.SlotDuration, &p.SlotDuration},
		{&ret.MaxBlockSize, &p.MaxBlockSize},
		{&ret.MaxHeaderSize, &p.MaxHeaderSize},
		{&ret.MaxTxSize, &p.MaxTxSize},
		{&ret.MaxProposalSize, &p.MaxProposalSize},
		{&ret.TxFeeMultiplierNano, &p.TxFeeMultiplierNano},
	} {
		if *field.src != nil {
			*field.dst = new(big.Int).Set(*field.src)
		}
	}
	return &ret
}

// Equal reports whether two parameter sets are identical.
func (p *ByronProtocolParameters) Equal(other *ByronProtocolParameters) bool {
	if p == nil || other == nil {
		return p == other
	}
	bigEqual := func(a, b *big.Int) bool {
		if a == nil || b == nil {
			return a == b
		}
		return a.Cmp(b) == 0
	}
	return p.ScriptVersion == other.ScriptVersion &&
		bigEqual(p.SlotDuration, other.SlotDuration) &&
		bigEqual(p.MaxBlockSize, other.MaxBlockSize) &&
		bigEqual(p.MaxHeaderSize, other.MaxHeaderSize) &&
		bigEqual(p.MaxTxSize, other.MaxTxSize) &&
		bigEqual(p.MaxProposalSize, other.MaxProposalSize) &&
		p.MpcThd == other.MpcThd &&
		p.HeavyDelThd == other.HeavyDelThd &&
		p.UpdateVoteThd == other.UpdateVoteThd &&
		p.UpdateProposalThd == other.UpdateProposalThd &&
		p.UpdateProposalTTL == other.UpdateProposalTTL &&
		p.SoftforkRule == other.SoftforkRule &&
		p.TxFeeSummand == other.TxFeeSummand &&
		bigEqual(p.TxFeeMultiplierNano, other.TxFeeMultiplierNano) &&
		p.UnlockStakeEpoch == other.UnlockStakeEpoch
}

// ApplyUpdate returns the parameters with a proposal's
// ProtocolParametersUpdate applied (PPU.apply): every field the update
// carries replaces the adopted value.
//
// An on-chain fee policy is decoded as TxSizeLinear, which reads both
// coefficients as Nano and rounds the summand to the nearest lovelace, ties
// to even, where genesis truncates it.
func (p *ByronProtocolParameters) ApplyUpdate(
	mod byron.ByronUpdateProposalBlockVersionMod,
) (*ByronProtocolParameters, error) {
	ret := p.Clone()
	if len(mod.ScriptVersion) > 0 {
		ret.ScriptVersion = mod.ScriptVersion[0]
	}
	for _, field := range []struct {
		dst    **big.Int
		values []*big.Int
	}{
		{&ret.SlotDuration, mod.SlotDuration},
		{&ret.MaxBlockSize, mod.MaxBlockSize},
		{&ret.MaxHeaderSize, mod.MaxHeaderSize},
		{&ret.MaxTxSize, mod.MaxTxSize},
		{&ret.MaxProposalSize, mod.MaxProposalSize},
	} {
		if len(field.values) > 0 && field.values[0] != nil {
			*field.dst = new(big.Int).Set(field.values[0])
		}
	}
	for _, field := range []struct {
		dst    *uint64
		values []byron.ByronLovelacePortion
	}{
		{&ret.MpcThd, mod.MpcThd},
		{&ret.HeavyDelThd, mod.HeavyDelThd},
		{&ret.UpdateVoteThd, mod.UpdateVoteThd},
		{&ret.UpdateProposalThd, mod.UpdateProposalThd},
	} {
		if len(field.values) > 0 {
			*field.dst = uint64(field.values[0])
		}
	}
	if len(mod.UpdateImplicit) > 0 {
		ret.UpdateProposalTTL = mod.UpdateImplicit[0]
	}
	if len(mod.SoftForkRule) > 0 {
		rule := mod.SoftForkRule[0]
		ret.SoftforkRule = ByronSoftforkRule{
			InitThd:      uint64(rule.InitThreshold),
			MinThd:       uint64(rule.MinThreshold),
			ThdDecrement: uint64(rule.ThresholdDecrement),
		}
	}
	if len(mod.TxFeePolicy) > 0 {
		policy := mod.TxFeePolicy[0]
		if policy.SummandNano == nil || policy.MultiplierNano == nil {
			return nil, errors.New("byron fee policy update is incomplete")
		}
		summand := byronRoundNanoHalfEven(policy.SummandNano)
		if err := byronCheckLovelace("fee policy summand", summand); err != nil {
			return nil, err
		}
		ret.TxFeeSummand = summand.Uint64()
		ret.TxFeeMultiplierNano = new(big.Int).Set(policy.MultiplierNano)
	}
	if len(mod.UnlockStakeEpoch) > 0 {
		ret.UnlockStakeEpoch = mod.UnlockStakeEpoch[0]
	}
	return ret, nil
}

// byronRoundNanoHalfEven is Haskell's round on a Nano: the nearest integer to
// nano / 10^9, ties to even.
func byronRoundNanoHalfEven(nano *big.Int) *big.Int {
	scale := big.NewInt(byronFeePolicyScale)
	quotient, remainder := new(big.Int).DivMod(nano, scale, new(big.Int))
	twice := new(big.Int).Lsh(remainder, 1)
	switch twice.Cmp(scale) {
	case 1:
		quotient.Add(quotient, big.NewInt(1))
	case 0:
		if quotient.Bit(0) == 1 {
			quotient.Add(quotient, big.NewInt(1))
		}
	}
	return quotient
}

// MinFee returns summand + ceiling(multiplier * size) in lovelace
// (calculateTxSizeLinear), which must stay within the Lovelace domain.
func (p *ByronProtocolParameters) MinFee(size uint64) (*big.Int, error) {
	multiplier := p.TxFeeMultiplierNano
	if multiplier == nil {
		multiplier = new(big.Int)
	}
	scale := big.NewInt(byronFeePolicyScale)
	perSize := new(big.Int).Mul(multiplier, new(big.Int).SetUint64(size))
	// Ceiling division rounds toward positive infinity for either sign.
	quotient, remainder := new(big.Int).DivMod(perSize, scale, new(big.Int))
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if err := byronCheckLovelace("minimum fee", quotient); err != nil {
		return nil, err
	}
	required := quotient.Add(
		quotient,
		new(big.Int).SetUint64(p.TxFeeSummand),
	)
	if err := byronCheckLovelace("minimum fee", required); err != nil {
		return nil, err
	}
	return required, nil
}

// UpdateAdoptionThreshold is upAdptThd: floor(srMinThd * numGenKeys), the
// number of genesis keys that must confirm or endorse an update.
func (p *ByronProtocolParameters) UpdateAdoptionThreshold(
	numGenKeys int,
) int {
	threshold := new(big.Int).Mul(
		new(big.Int).SetUint64(p.SoftforkRule.MinThd),
		big.NewInt(int64(numGenKeys)),
	)
	threshold.Quo(threshold, big.NewInt(byronLovelacePortionDenominator))
	return int(threshold.Int64())
}
