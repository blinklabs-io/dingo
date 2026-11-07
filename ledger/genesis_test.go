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

package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"

	"github.com/blinklabs-io/dingo/config/cardano"
)

type completeTestShelleyGenesisLoader struct {
	cfg *cardano.CardanoNodeConfig
}

func completeTestShelleyGenesis(
	cfg *cardano.CardanoNodeConfig,
) completeTestShelleyGenesisLoader {
	return completeTestShelleyGenesisLoader{cfg: cfg}
}

func (l completeTestShelleyGenesisLoader) LoadShelleyGenesisFromReader(
	r io.Reader,
) error {
	base, err := cardano.EmbeddedConfigFS.ReadFile("preview/shelley-genesis.json")
	if err != nil {
		return fmt.Errorf("read complete Shelley genesis fixture: %w", err)
	}
	overrides, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read Shelley genesis overrides: %w", err)
	}
	var genesis map[string]json.RawMessage
	if err := json.Unmarshal(base, &genesis); err != nil {
		return fmt.Errorf("decode complete Shelley genesis fixture: %w", err)
	}
	genesis["genDelegs"] = json.RawMessage(`{}`)
	genesis["updateQuorum"] = json.RawMessage(`0`)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(overrides, &fields); err != nil {
		return fmt.Errorf("decode Shelley genesis overrides: %w", err)
	}
	if protocolOverrides, ok := fields["protocolParams"]; ok {
		var protocolParams map[string]json.RawMessage
		if err := json.Unmarshal(genesis["protocolParams"], &protocolParams); err != nil {
			return fmt.Errorf("decode complete protocol parameters: %w", err)
		}
		var protocolFields map[string]json.RawMessage
		if err := json.Unmarshal(protocolOverrides, &protocolFields); err != nil {
			return fmt.Errorf("decode protocol parameter overrides: %w", err)
		}
		maps.Copy(protocolParams, protocolFields)
		fields["protocolParams"], err = json.Marshal(protocolParams)
		if err != nil {
			return fmt.Errorf("encode complete protocol parameters: %w", err)
		}
	}
	maps.Copy(genesis, fields)
	merged, err := json.Marshal(genesis)
	if err != nil {
		return fmt.Errorf("encode complete Shelley genesis fixture: %w", err)
	}
	return l.cfg.LoadShelleyGenesisFromReader(bytes.NewReader(merged))
}
