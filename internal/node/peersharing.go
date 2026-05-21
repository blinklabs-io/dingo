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

package node

import "log/slog"

// resolvePeerSharing decides the effective PeerSharing setting from the
// Dingo-native value (CLI/env/YAML, may be nil), the BlockProducer flag,
// and the cardano-node config.json value (may be nil). On a block producer
// PeerSharing defaults to off and only the Dingo-native flag can turn it
// on; cardano-node config.json is ignored. Off block-producers, the
// cardano-node value acts as a fallback default when the Dingo-native
// value is unconfigured.
//
// The reason this lives at the node-startup boundary rather than inside the
// ouroboros wrapper or the gouroboros library: PeerSharing leaks topology
// from a forging node and is incompatible with relays that do not speak
// NtN >= 13 (they reset connections with UnknownMiniProtocol on
// MiniProtocolNum 10). The decision must be made before any outbound NtN
// VersionData is constructed.
func resolvePeerSharing(
	dingoNative *bool,
	blockProducer bool,
	cardanoNode *bool,
	logger *slog.Logger,
) bool {
	if dingoNative != nil {
		if *dingoNative && blockProducer && logger != nil {
			logger.Warn(
				"PeerSharing enabled on a block producer; this leaks topology and is incompatible with relays running NtN < 13. Disable unless you know what you're doing.",
				"component", "node",
			)
		}
		return *dingoNative
	}
	if blockProducer {
		if logger != nil {
			logger.Info(
				"PeerSharing disabled by default on block producer; set --peer-sharing=true to override",
				"component", "node",
			)
		}
		return false
	}
	if cardanoNode != nil {
		if logger != nil {
			logger.Info(
				"using cardano-node config.json PeerSharing as fallback default",
				"component", "node",
				"value", *cardanoNode,
			)
		}
		return *cardanoNode
	}
	return false
}
