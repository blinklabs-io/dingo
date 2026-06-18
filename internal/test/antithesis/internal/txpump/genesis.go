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

package txpump

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/blinklabs-io/gouroboros/ledger/common"
)

// GenesisUTxO represents a single pre-funded UTxO from the genesis
// configuration, as produced by the testnet-generation-tool.
type GenesisUTxO struct {
	TxHash string `json:"txHash"`
	Index  uint32 `json:"index"`
	Amount uint64 `json:"amount"`
}

type shelleyGenesisFile struct {
	InitialFunds map[string]uint64 `json:"initialFunds"`
}

// LoadGenesisUTxOs reads pre-funded UTxOs from a JSON file or directory
// produced by the testnet-generation-tool configurator.
//
// If path is a directory, all .json files in it are read (non-JSON files such
// as key files are skipped). If path is a file, it is read directly. The
// expected JSON format is either an array of objects with txHash, index, and
// amount fields or a Shelley genesis file containing initialFunds.
func LoadGenesisUTxOs(path string) ([]UTxO, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat genesis UTxO path %s: %w", path, err)
	}

	var files []string
	if info.IsDir() {
		entries, dirErr := os.ReadDir(path)
		if dirErr != nil {
			return nil, fmt.Errorf("read genesis UTxO dir %s: %w", path, dirErr)
		}
		for _, e := range entries {
			if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
				files = append(files, filepath.Join(path, e.Name()))
			}
		}
	} else {
		files = []string{path}
	}

	var utxos []UTxO
	for _, f := range files {
		loaded, loadErr := loadGenesisFile(f)
		if loadErr != nil {
			return nil, fmt.Errorf("load %s: %w", f, loadErr)
		}
		utxos = append(utxos, loaded...)
	}

	if len(utxos) == 0 {
		return nil, fmt.Errorf("no genesis UTxOs found in %s", path)
	}

	return utxos, nil
}

func loadGenesisFile(path string) ([]UTxO, error) {
	data, err := os.ReadFile(path) //nolint:gosec // trusted config path
	if err != nil {
		return nil, err
	}

	var raw []GenesisUTxO
	rawErr := json.Unmarshal(data, &raw)
	if rawErr == nil {
		utxos := make([]UTxO, len(raw))
		for i, r := range raw {
			utxos[i] = UTxO(r)
		}
		return utxos, nil
	}

	var shelley shelleyGenesisFile
	if err := json.Unmarshal(data, &shelley); err != nil {
		return nil, fmt.Errorf("unmarshal UTxO list: %w", rawErr)
	}
	if len(shelley.InitialFunds) == 0 {
		return nil, fmt.Errorf(
			"unmarshal UTxO list: %w; no Shelley initialFunds",
			rawErr,
		)
	}
	return utxosFromShelleyInitialFunds(shelley.InitialFunds)
}

func utxosFromShelleyInitialFunds(
	initialFunds map[string]uint64,
) ([]UTxO, error) {
	addresses := make([]string, 0, len(initialFunds))
	for address := range initialFunds {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)

	utxos := make([]UTxO, 0, len(addresses))
	for _, address := range addresses {
		addrBytes, err := hex.DecodeString(address)
		if err != nil {
			return nil, fmt.Errorf(
				"Shelley initialFunds address %q: %w",
				address, err,
			)
		}
		txHash := common.Blake2b256Hash(addrBytes)
		utxos = append(utxos, UTxO{
			TxHash: txHash.String(),
			Index:  0,
			Amount: initialFunds[address],
		})
	}
	return utxos, nil
}
