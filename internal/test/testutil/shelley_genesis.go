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

package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/stretchr/testify/require"
)

// CompleteShelleyGenesisLoader loads Shelley genesis overrides on top of the
// embedded preview fixture.
type CompleteShelleyGenesisLoader struct {
	cfg *cardano.CardanoNodeConfig
}

// CompleteShelleyGenesis returns a loader that lets tests supply only the
// genesis fields relevant to the behavior they exercise while still passing
// production genesis validation.
func CompleteShelleyGenesis(
	cfg *cardano.CardanoNodeConfig,
) CompleteShelleyGenesisLoader {
	return CompleteShelleyGenesisLoader{cfg: cfg}
}

// LoadShelleyGenesisFromReader merges overrides with the complete embedded
// fixture and loads the result into the configured node configuration.
func (l CompleteShelleyGenesisLoader) LoadShelleyGenesisFromReader(
	overrides io.Reader,
) error {
	merged, err := completeShelleyGenesis(overrides)
	if err != nil {
		return err
	}
	return l.cfg.LoadShelleyGenesisFromReader(bytes.NewReader(merged))
}

// CompleteShelleyGenesisJSON returns a complete Shelley genesis document with
// the supplied top-level and protocol-parameter overrides.
func CompleteShelleyGenesisJSON(
	t testing.TB,
	overrides map[string]any,
	protocolParamOverrides map[string]any,
) string {
	t.Helper()
	fields := make(map[string]json.RawMessage, len(overrides)+1)
	for key, value := range overrides {
		encoded, err := json.Marshal(value)
		require.NoError(t, err)
		fields[key] = encoded
	}
	if len(protocolParamOverrides) > 0 {
		encoded, err := json.Marshal(protocolParamOverrides)
		require.NoError(t, err)
		fields["protocolParams"] = encoded
	}
	raw, err := json.Marshal(fields)
	require.NoError(t, err)
	merged, err := completeShelleyGenesis(bytes.NewReader(raw))
	require.NoError(t, err)
	return string(merged)
}

func completeShelleyGenesis(overrides io.Reader) ([]byte, error) {
	base, err := cardano.EmbeddedConfigFS.ReadFile("preview/shelley-genesis.json")
	if err != nil {
		return nil, fmt.Errorf("read complete Shelley genesis fixture: %w", err)
	}
	var genesis map[string]json.RawMessage
	if err := json.Unmarshal(base, &genesis); err != nil {
		return nil, fmt.Errorf("decode complete Shelley genesis fixture: %w", err)
	}
	genesis["genDelegs"] = json.RawMessage(`{}`)
	genesis["updateQuorum"] = json.RawMessage(`0`)

	rawOverrides, err := io.ReadAll(overrides)
	if err != nil {
		return nil, fmt.Errorf("read Shelley genesis overrides: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(rawOverrides, &fields); err != nil {
		return nil, fmt.Errorf("decode Shelley genesis overrides: %w", err)
	}
	if protocolOverrides, ok := fields["protocolParams"]; ok {
		var protocolParams map[string]json.RawMessage
		if err := json.Unmarshal(genesis["protocolParams"], &protocolParams); err != nil {
			return nil, fmt.Errorf("decode complete protocol parameters: %w", err)
		}
		var protocolFields map[string]json.RawMessage
		if err := json.Unmarshal(protocolOverrides, &protocolFields); err != nil {
			return nil, fmt.Errorf("decode protocol parameter overrides: %w", err)
		}
		maps.Copy(protocolParams, protocolFields)
		fields["protocolParams"], err = json.Marshal(protocolParams)
		if err != nil {
			return nil, fmt.Errorf("encode complete protocol parameters: %w", err)
		}
	}
	maps.Copy(genesis, fields)
	merged, err := json.Marshal(genesis)
	if err != nil {
		return nil, fmt.Errorf("encode complete Shelley genesis fixture: %w", err)
	}
	return merged, nil
}
