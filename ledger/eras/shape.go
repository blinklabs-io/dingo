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
	"math/big"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
)

// StabilityWindowForEra returns the stability window (in slots) for the given
// era. For Byron, this is 2k; for Shelley and later eras, 3k/f (rounded up).
//
// Returns an error if the required genesis data is unavailable or malformed.
// This is the pure, LedgerState-free version of the stability-window
// computation used by the HFC Shape builder.
func StabilityWindowForEra(cfg *cardano.CardanoNodeConfig, eraId uint) (uint64, error) {
	if cfg == nil {
		return 0, errors.New("eras: nil CardanoNodeConfig")
	}
	if eraId == ByronEraDesc.Id {
		byronGenesis := cfg.ByronGenesis()
		if byronGenesis == nil {
			return 0, errors.New("eras: Byron genesis unavailable")
		}
		k := byronGenesis.ProtocolConsts.K
		if k <= 0 {
			return 0, fmt.Errorf("eras: invalid Byron security parameter k=%d", k)
		}
		return uint64(k) * 2, nil // #nosec G115
	}
	shelleyGenesis := cfg.ShelleyGenesis()
	if shelleyGenesis == nil {
		return 0, errors.New("eras: Shelley genesis unavailable")
	}
	k := shelleyGenesis.SecurityParam
	if k <= 0 {
		return 0, fmt.Errorf("eras: invalid Shelley security parameter k=%d", k)
	}
	activeSlotsCoeff := shelleyGenesis.ActiveSlotsCoeff.Rat
	if activeSlotsCoeff == nil {
		return 0, errors.New("eras: Shelley ActiveSlotsCoeff is nil")
	}
	if activeSlotsCoeff.Num().Sign() <= 0 {
		return 0, fmt.Errorf(
			"eras: ActiveSlotsCoeff must be positive, got %s",
			activeSlotsCoeff.String(),
		)
	}
	if activeSlotsCoeff.Cmp(big.NewRat(1, 1)) > 0 {
		return 0, fmt.Errorf(
			"eras: ActiveSlotsCoeff must be <= 1, got %s",
			activeSlotsCoeff.String(),
		)
	}
	// ceil(3 * k / f) where f = num/denom → ceil(3 * k * denom / num)
	numerator := new(big.Int).SetUint64(uint64(k))
	numerator.Mul(numerator, big.NewInt(3))
	numerator.Mul(numerator, activeSlotsCoeff.Denom())
	denominator := new(big.Int).Set(activeSlotsCoeff.Num())
	window, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() != 0 {
		window.Add(window, big.NewInt(1))
	}
	if window.Sign() <= 0 {
		return 0, fmt.Errorf(
			"eras: stability window non-positive (k=%d, f=%s)",
			k, activeSlotsCoeff.String(),
		)
	}
	if !window.IsUint64() {
		return 0, fmt.Errorf(
			"eras: stability window overflows uint64 (k=%d, f=%s)",
			k, activeSlotsCoeff.String(),
		)
	}
	return window.Uint64(), nil
}

// BuildEraParams assembles a hardfork.EraParams value for the given era,
// using the node configuration for the era-specific epoch length, slot length,
// and safe-zone (stability-window) values.
func BuildEraParams(cfg *cardano.CardanoNodeConfig, era EraDesc) (hardfork.EraParams, error) {
	if cfg == nil {
		return hardfork.EraParams{}, errors.New("eras: nil CardanoNodeConfig")
	}
	if era.EpochLengthFunc == nil {
		return hardfork.EraParams{}, fmt.Errorf(
			"eras: era %q has no EpochLengthFunc",
			era.Name,
		)
	}
	slotLenMs, epochLen, err := era.EpochLengthFunc(cfg)
	if err != nil {
		return hardfork.EraParams{}, fmt.Errorf(
			"eras: era %q EpochLengthFunc: %w",
			era.Name, err,
		)
	}
	safeZone, err := StabilityWindowForEra(cfg, era.Id)
	if err != nil {
		return hardfork.EraParams{}, fmt.Errorf(
			"eras: era %q stability window: %w",
			era.Name, err,
		)
	}
	return hardfork.EraParams{
		EpochSize: uint64(epochLen),
		// slotLenMs is protocol-bounded (milliseconds per slot, 1–20s range).
		// #nosec G115
		SlotLength:    time.Duration(slotLenMs) * time.Millisecond,
		SafeZoneSlots: safeZone,
		// GenesisWindow is not yet tracked per era in dingo; defaulting to
		// the safe-zone value mirrors the Haskell default for Shelley.
		GenesisWindow: safeZone,
	}, nil
}

// BuildShape constructs a hardfork.Shape from dingo's static era table.
// The shape's SystemStart comes from the Shelley genesis; each entry's
// EraParams are built via BuildEraParams; the protocol-version range
// is read directly from EraDesc.MinMajorVersion / MaxMajorVersion.
func BuildShape(cfg *cardano.CardanoNodeConfig) (hardfork.Shape, error) {
	if cfg == nil {
		return hardfork.Shape{}, errors.New("eras: nil CardanoNodeConfig")
	}
	shelleyGenesis := cfg.ShelleyGenesis()
	if shelleyGenesis == nil {
		return hardfork.Shape{}, errors.New("eras: Shelley genesis unavailable (required for SystemStart)")
	}

	entries := make([]hardfork.ShapeEntry, 0, len(Eras))
	for _, era := range Eras {
		params, err := BuildEraParams(cfg, era)
		if err != nil {
			return hardfork.Shape{}, err
		}
		entries = append(entries, hardfork.ShapeEntry{
			EraID:           era.Id,
			EraName:         era.Name,
			MinMajorVersion: era.MinMajorVersion,
			MaxMajorVersion: era.MaxMajorVersion,
			Params:          params,
		})
	}

	// Resolve each entry's NextEraTrigger. Must happen after the entries are
	// built because AtVersion defaults to the *next* entry's MinMajorVersion.
	// TestXHardForkAtEpoch is keyed on the lowercase successor era name and
	// only honoured when ExperimentalHardForksEnabled is set.
	for i := range entries {
		if i == len(entries)-1 {
			entries[i].NextEraTrigger = hardfork.NewTriggerNotDuringThisExecution()
			continue
		}
		nextEra := entries[i+1]
		if epoch, ok := cfg.HardForkEpoch(strings.ToLower(nextEra.EraName)); ok {
			entries[i].NextEraTrigger = hardfork.NewTriggerAtEpoch(epoch)
			continue
		}
		entries[i].NextEraTrigger = hardfork.NewTriggerAtVersion(nextEra.MinMajorVersion)
	}

	return hardfork.Shape{
		SystemStart: shelleyGenesis.SystemStart,
		Eras:        entries,
	}, nil
}
