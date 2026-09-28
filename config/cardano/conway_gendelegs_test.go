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

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// conwayGenesisWithGenDelegs returns the Conway test genesis with a top-level
// genDelegs member added, as the cardano-ledger Conway genesis schema permits.
func conwayGenesisWithGenDelegs(t *testing.T, genDelegs string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testDataDir, "conway-genesis.json"))
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc))
	doc["genDelegs"] = json.RawMessage(genDelegs)
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	return out
}

func TestConwayGenesisAcceptsGenDelegs(t *testing.T) {
	t.Parallel()
	const populated = `{"3bb57c00a978cd85bb1b3e35135900852e1df4424316cf96569d2892":` +
		`{"delegate":"e70c3720e3356b01c81c74b64f471c24fc0e3fc353ebbaff07026832",` +
		`"vrf":"6bd4866213697d3cb456a9cd8fd84bff98b1449dd7fcb091ad48f77da5a86968"}}`
	for name, genDelegs := range map[string]string{
		"empty":     `{}`,
		"populated": populated,
	} {
		t.Run(name+"/reader", func(t *testing.T) {
			t.Parallel()
			var c CardanoNodeConfig
			err := c.LoadConwayGenesisFromReader(
				bytes.NewReader(conwayGenesisWithGenDelegs(t, genDelegs)),
			)
			require.NoError(t, err)
			require.NotNil(t, c.ConwayGenesis())
		})
		t.Run(name+"/file", func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "conway.json")
			genesis := conwayGenesisWithGenDelegs(t, genDelegs)
			require.NoError(t, os.WriteFile(path, genesis, 0o600))
			c := CardanoNodeConfig{ConwayGenesisFile: path}
			require.NoError(t, c.loadGenesisConfigs())
			require.NotNil(t, c.ConwayGenesis())
		})
	}
}

// TestConwayGenesisAcceptsLegacyCommitteeQuorum covers the Vector Testnet
// committee shape, which carries a legacy "quorum" member beside "threshold".
func TestConwayGenesisAcceptsLegacyCommitteeQuorum(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join(testDataDir, "conway-genesis.json"))
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc))
	doc["committee"] = json.RawMessage(
		`{"members":{},"quorum":0,"threshold":0}`,
	)
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	var c CardanoNodeConfig
	require.NoError(t, c.LoadConwayGenesisFromReader(bytes.NewReader(out)))
	require.NotNil(t, c.ConwayGenesis())
}

func TestConwayGenesisStillRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join(testDataDir, "conway-genesis.json"))
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc))
	doc["notAConwayField"] = json.RawMessage(`1`)
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	var c CardanoNodeConfig
	require.Error(t, c.LoadConwayGenesisFromReader(bytes.NewReader(out)))

	doc["committee"] = json.RawMessage(
		`{"members":{},"threshold":0,"notACommitteeField":1}`,
	)
	delete(doc, "notAConwayField")
	out, err = json.Marshal(doc)
	require.NoError(t, err)
	require.Error(t, c.LoadConwayGenesisFromReader(bytes.NewReader(out)))
}

// TestConwayGenesisRejectsQuorumWithoutThreshold pins that the legacy quorum
// member is tolerated only beside threshold. cardano-ledger requires threshold,
// so a committee carrying quorum alone must not decode to a nil threshold.
func TestConwayGenesisRejectsQuorumWithoutThreshold(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join(testDataDir, "conway-genesis.json"))
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc))
	doc["committee"] = json.RawMessage(`{"members":{},"quorum":0.5}`)
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	var c CardanoNodeConfig
	require.Error(t, c.LoadConwayGenesisFromReader(bytes.NewReader(out)))
}

// TestConwayGenesisRejectsQuorumWithNullThreshold pins that a null threshold
// does not count as present: stripping quorum beside it would decode to a nil
// threshold, which cardano-ledger rejects.
func TestConwayGenesisRejectsQuorumWithNullThreshold(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join(testDataDir, "conway-genesis.json"))
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc))
	doc["committee"] = json.RawMessage(
		`{"members":{},"quorum":0.5,"threshold":null}`,
	)
	out, err := json.Marshal(doc)
	require.NoError(t, err)
	var c CardanoNodeConfig
	require.Error(t, c.LoadConwayGenesisFromReader(bytes.NewReader(out)))
}

// TestConwayGenesisRejectsDuplicateMembersWhenNormalizing pins that removing
// genDelegs or quorum does not collapse duplicate members, which would discard
// an earlier occurrence before the strict decoder could reject it.
func TestConwayGenesisRejectsDuplicateMembersWhenNormalizing(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Join(testDataDir, "conway-genesis.json"))
	require.NoError(t, err)
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc))
	for name, tc := range map[string]struct {
		committee string
		extra     string
	}{
		// The earlier committee carries an unknown member; collapsing to the
		// later occurrence would hide it from the strict decoder.
		"top-level": {
			committee: `{"members":{},"threshold":0,"notACommitteeField":1}`,
			extra: `,"genDelegs":{}` +
				`,"committee":{"members":{},"threshold":0}`,
		},
		"committee": {
			committee: `{"members":{},"quorum":0,` +
				`"threshold":{"notARational":1},"threshold":0}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			d := maps.Clone(doc)
			d["committee"] = json.RawMessage(tc.committee)
			out, err := json.Marshal(d)
			require.NoError(t, err)
			out = append(out[:len(out)-1], tc.extra+"}"...)
			var c CardanoNodeConfig
			require.Error(t, c.LoadConwayGenesisFromReader(bytes.NewReader(out)))
		})
	}
}

// TestLoadConwayGenesisFromReaderStopsAfterValue pins that the reader loader
// returns once it has read one complete JSON value, without waiting for EOF.
func TestLoadConwayGenesisFromReaderStopsAfterValue(t *testing.T) {
	t.Parallel()
	genesis := conwayGenesisWithGenDelegs(t, `{}`)
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })
	go func() { _, _ = pw.Write(genesis) }()
	done := make(chan error, 1)
	go func() {
		var c CardanoNodeConfig
		done <- c.LoadConwayGenesisFromReader(pr)
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("LoadConwayGenesisFromReader blocked waiting for EOF")
	}
}
