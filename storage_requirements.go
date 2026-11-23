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

package dingo

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/blinklabs-io/dingo/plugin"
)

type storageConsumer struct {
	name         string
	requirements plugin.StorageRequirements
	enabled      bool
}

var builtinStorageConsumers = []storageConsumer{
	{name: "Bark API", requirements: plugin.StorageRequirements{
		Tier:     plugin.StorageTierCore,
		Features: []plugin.StorageFeature{plugin.StorageFeatureBark},
	}},
	{name: "Midnight indexer", requirements: plugin.StorageRequirements{
		Tier:     plugin.StorageTierAPI,
		Features: []plugin.StorageFeature{plugin.StorageFeatureMidnight},
	}},
	{name: "Midnight server", requirements: plugin.StorageRequirements{
		Tier:     plugin.StorageTierAPI,
		Features: []plugin.StorageFeature{plugin.StorageFeatureMidnight},
	}},
	{name: "CIP-26 registry", requirements: plugin.StorageRequirements{
		Tier:     plugin.StorageTierAPI,
		Features: []plugin.StorageFeature{plugin.StorageFeatureCIP26},
	}},
	{
		name: "off-chain metadata fetcher",
		requirements: plugin.StorageRequirements{
			Tier: plugin.StorageTierAPI,
			Features: []plugin.StorageFeature{
				plugin.StorageFeatureOffchainMetadata,
			},
		},
	},
}

func storageTierForMode(mode StorageMode) plugin.StorageTier {
	switch mode {
	case StorageModeCore:
		return plugin.StorageTierCore
	case StorageModeAPI:
		return plugin.StorageTierAPI
	default:
		return plugin.StorageTierNone
	}
}

func (n *Node) storageFeatureEnabled(feature plugin.StorageFeature) bool {
	return slices.Contains(n.config.storageFeatures, feature)
}

func (n *Node) storagePlan() ([]plugin.StorageFeature, error) {
	selectedTier := storageTierForMode(n.apiStorageMode())
	consumers, err := n.enabledStorageConsumers()
	if err != nil {
		return nil, err
	}

	allFeatures := make(map[plugin.StorageFeature]struct{})
	for _, descriptor := range n.pluginHost.Providers() {
		for _, feature := range descriptor.Storage.Features {
			allFeatures[feature] = struct{}{}
		}
	}
	for _, consumer := range builtinStorageConsumers {
		for _, feature := range consumer.requirements.Features {
			allFeatures[feature] = struct{}{}
		}
	}

	enabledFeatures := make(map[plugin.StorageFeature]struct{})
	var incompatible []string
	for _, consumer := range consumers {
		if !consumer.enabled {
			continue
		}
		if !selectedTier.Includes(consumer.requirements.Tier) {
			incompatible = append(incompatible, fmt.Sprintf(
				"%s requires %q", consumer.name,
				consumer.requirements.Tier.String(),
			))
			continue
		}
		for _, feature := range consumer.requirements.Features {
			enabledFeatures[feature] = struct{}{}
		}
	}
	if len(incompatible) > 0 {
		sort.Strings(incompatible)
		return nil, fmt.Errorf(
			"storage mode %q is incompatible with enabled consumers: %s",
			n.apiStorageMode(), strings.Join(incompatible, "; "),
		)
	}
	if selectedTier == plugin.StorageTierAPI {
		enabledFeatures = allFeatures
	}

	features := make([]plugin.StorageFeature, 0, len(enabledFeatures))
	for feature := range enabledFeatures {
		features = append(features, feature)
	}
	slices.Sort(features)
	return features, nil
}

func (n *Node) enabledStorageConsumers() ([]storageConsumer, error) {
	consumers := slices.Clone(builtinStorageConsumers)
	consumers[0].enabled = n.config.barkPort > 0
	consumers[1].enabled = n.config.midnight.Enabled
	consumers[2].enabled = n.config.midnight.ServerEnabled &&
		n.config.midnight.Port > 0
	consumers[3].enabled = n.config.tokenRegistry.Enabled
	consumers[4].enabled = n.apiStorageMode().IsAPI()

	apiCapabilities := []plugin.Capability{
		plugin.CapabilityAPIBlockfrost,
		plugin.CapabilityAPIKupo,
		plugin.CapabilityAPIMesh,
		plugin.CapabilityAPIUtxorpc,
		plugin.CapabilityAPIMcp,
	}
	for _, capability := range apiCapabilities {
		selection, port, err := n.apiPluginSelection(capability)
		if err != nil {
			return nil, err
		}
		descriptor, ok := n.pluginHost.ProviderDescriptor(
			capability, selection.Provider,
		)
		if !ok {
			return nil, fmt.Errorf(
				"plugin descriptor is missing for %s/%s",
				capability.Name(), selection.Provider,
			)
		}
		enabled := port > 0
		if descriptor.Storage.Tier == plugin.StorageTierAPI &&
			!n.apiStorageMode().IsAPI() &&
			selection.Provider == "builtin" &&
			port == defaultAPIPort(capability) {
			// API-tier built-ins with non-zero defaults remain dormant in
			// core mode. A changed port or provider is an enabled selection.
			enabled = false
		}
		consumers = append(consumers, storageConsumer{
			name:         capability.Name() + "/" + selection.Provider,
			requirements: descriptor.Storage, enabled: enabled,
		})
	}

	if n.config.cfg != nil {
		for name, selection := range n.config.cfg.Plugins.Extra {
			capability, ok := n.pluginHost.Contract(name)
			if !ok {
				continue
			}
			descriptor, ok := n.pluginHost.ProviderDescriptor(
				capability, selection.Provider,
			)
			if !ok || descriptor.Storage.Tier == plugin.StorageTierNone {
				continue
			}
			consumers = append(consumers, storageConsumer{
				name:         name + "/" + selection.Provider,
				requirements: descriptor.Storage, enabled: true,
			})
		}
	}
	return consumers, nil
}
