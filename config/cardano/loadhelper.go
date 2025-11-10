// Copyright 2025 Blink Labs Software
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

import (
	"embed"
	"fmt"
	"os"
)

// LoadCardanoNodeConfigWithFallback tries to load config from file, then falls back to embed FS for preview network.
func LoadCardanoNodeConfigWithFallback(
	cfgPath, network string,
	embedFS embed.FS,
) (*CardanoNodeConfig, error) {
	_, err := os.Stat(cfgPath)
	if err == nil {
		return NewCardanoNodeConfigFromFile(cfgPath)
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf(
			"failed to check config file %q: %w",
			cfgPath,
			err,
		)
	}

	// File doesn't exist, try embedded config for preview network
	if network == "preview" {
		return NewCardanoNodeConfigFromEmbedFS(embedFS, cfgPath)
	}
	return nil, fmt.Errorf(
		"config file %q not found and no embedded config available for network %q",
		cfgPath,
		network,
	)
}
