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

package cardano

import "embed"

// EmbeddedConfigFS contains the embedded Cardano configuration files
// for all supported networks (preview, preprod, mainnet, devnet, musashi,
// prime-testnet). This includes the network configuration and all genesis
// files required for network operation when no external config files are
// available.
//
//go:embed preview mainnet preprod devnet musashi prime-testnet
var EmbeddedConfigFS embed.FS

// embeddedConfigNames are the file names a network directory may use for its
// cardano-node configuration, tried in order.
//
// Most networks ship `config.json`. prime-testnet ships `configuration.yaml`,
// because config/cardano/ is a verbatim copy of docker-cardano-configs
// (enforced by bin/config-parity.sh) and that is the name upstream uses. The
// loader parses both through the same YAML decoder, so the only thing the
// difference costs is the file name.
var embeddedConfigNames = []string{"config.json", "configuration.yaml"}

// EmbeddedConfigPath returns the embedded configuration file for network.
//
// Callers deriving a default path must use this rather than appending
// "/config.json": a network whose upstream config is named otherwise is then
// simply unreachable from the embedded filesystem, and the failure surfaces as
// "no embedded config available" with nothing naming the file name as the
// cause.
//
// A network with no embedded directory at all gets the conventional
// "<network>/config.json", so the error a caller reports names the file it
// would have expected rather than the last name this happened to try.
func EmbeddedConfigPath(network string) string {
	for _, name := range embeddedConfigNames {
		candidate := network + "/" + name
		f, err := EmbeddedConfigFS.Open(candidate)
		if err != nil {
			continue
		}
		//nolint:errcheck // nothing was read; a close error has no meaning here
		_ = f.Close()
		return candidate
	}
	return network + "/" + embeddedConfigNames[0]
}
