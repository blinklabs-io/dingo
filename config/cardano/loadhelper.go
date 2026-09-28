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
	"strings"
)

// LoadCardanoNodeConfigWithFallback tries to load config from file,
// then falls back to the embedded FS for supported networks.
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

	// File doesn't exist, try embedded config. embed.FS always uses slash
	// paths, while callers may have built cfgPath with filepath.Join.
	// ReplaceAll rather than filepath.ToSlash so a backslash-separated path
	// is normalized on every platform (ToSlash is the identity on Unix).
	embedPath := strings.ReplaceAll(cfgPath, "\\", "/")
	cfg, embedErr := NewCardanoNodeConfigFromEmbedFS(embedFS, embedPath)
	if embedErr != nil {
		return nil, fmt.Errorf(
			"config file %q not found and no embedded config available for network %q: %w",
			cfgPath,
			network,
			embedErr,
		)
	}
	return cfg, nil
}

// AlonzoLovelacePerUtxoWord returns Alonzo genesis' lovelacePerUTxOWord, or
// zero when it is unavailable. It is nil-safe on every hop -- a nil config, a
// config whose Alonzo genesis never loaded, and a path that resolves to
// neither a file nor an embedded network config all return zero, which
// database.Config documents as "not supplied".
//
// It exists because every database.Config construction site must carry this
// value: database.New repairs a pre-gouroboros-v0.205.7 Alonzo
// protocol-parameter row in place from it, and a site that omits it makes the
// same database unrepairable and demands a resync. Sites that already hold a
// loaded config pass it as c; the rest pass nil and let this load one.
func AlonzoLovelacePerUtxoWord(
	c *CardanoNodeConfig,
	cfgPath, network string,
) uint64 {
	if c == nil {
		if network == "" && cfgPath == "" {
			return 0
		}
		if cfgPath == "" {
			cfgPath = EmbeddedConfigPath(network)
		}
		loaded, err := LoadCardanoNodeConfigWithFallback(
			cfgPath,
			network,
			EmbeddedConfigFS,
		)
		if err != nil || loaded == nil {
			return 0
		}
		c = loaded
	}
	genesis := c.AlonzoGenesis()
	if genesis == nil {
		return 0
	}
	return genesis.LovelacePerUtxoWord
}
