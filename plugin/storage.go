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

package plugin

import (
	"errors"
	"fmt"
	"slices"
)

// StorageTier is an ordered storage capability selected by the operator.
// Higher tiers include every lower tier. New tiers append to this ordering.
type StorageTier uint8

const (
	// StorageTierNone marks a descriptor that is not a storage consumer.
	StorageTierNone StorageTier = iota
	// StorageTierCore provides consensus and current-chain storage.
	StorageTierCore
	// StorageTierAPI adds complete API and historical indexes.
	StorageTierAPI
)

// Valid reports whether tier is selectable by the current binary.
func (t StorageTier) Valid() bool {
	return t >= StorageTierCore && t <= StorageTierAPI
}

// Includes reports whether t satisfies required.
func (t StorageTier) Includes(required StorageTier) bool {
	return t.Valid() && required.Valid() && t >= required
}

// String returns the operator-facing tier name.
func (t StorageTier) String() string {
	switch t {
	case StorageTierNone:
		return "none"
	case StorageTierCore:
		return "core"
	case StorageTierAPI:
		return "api"
	default:
		return fmt.Sprintf("storage-tier-%d", t)
	}
}

// StorageFeature names data a storage consumer needs.
type StorageFeature string

const (
	StorageFeatureBark             StorageFeature = "bark"
	StorageFeatureBlockfrost       StorageFeature = "blockfrost"
	StorageFeatureCIP26            StorageFeature = "cip26"
	StorageFeatureKupo             StorageFeature = "kupo"
	StorageFeatureMCP              StorageFeature = "mcp"
	StorageFeatureMesh             StorageFeature = "mesh"
	StorageFeatureMidnight         StorageFeature = "midnight"
	StorageFeatureOffchainMetadata StorageFeature = "offchain-metadata"
	StorageFeatureUtxorpc          StorageFeature = "utxorpc"
)

// StorageRequirements declares a consumer's minimum tier and named features.
// A zero Tier means the provider is not a storage consumer.
type StorageRequirements struct {
	Tier     StorageTier
	Features []StorageFeature
}

func (r StorageRequirements) validate() error {
	if r.Tier == StorageTierNone {
		if len(r.Features) != 0 {
			return errors.New("storage features require a storage tier")
		}
		return nil
	}
	if !r.Tier.Valid() {
		return fmt.Errorf("unknown storage tier %d", r.Tier)
	}
	if len(r.Features) == 0 {
		return fmt.Errorf(
			"storage tier %q requires at least one feature",
			r.Tier,
		)
	}
	seen := make(map[StorageFeature]struct{}, len(r.Features))
	for _, feature := range r.Features {
		if feature == "" {
			return errors.New("storage feature is empty")
		}
		if _, ok := seen[feature]; ok {
			return fmt.Errorf("storage feature %q is duplicated", feature)
		}
		seen[feature] = struct{}{}
	}
	return nil
}

func (r StorageRequirements) clone() StorageRequirements {
	r.Features = slices.Clone(r.Features)
	return r
}
