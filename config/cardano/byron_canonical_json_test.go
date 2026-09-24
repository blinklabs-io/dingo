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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeByronGenesisConfig writes a minimal cardano-node config.json plus a
// Byron genesis file with the given body into a temp directory, and returns a
// CardanoNodeConfig with only ByronGenesisFile set (no declared hash, so
// validateGenesisHash skips comparison and just returns whatever this test's
// genesis hashes to).
func writeByronGenesisConfig(t *testing.T, byronGenesisJSON string) *CardanoNodeConfig {
	t.Helper()
	defaults, err := EmbeddedConfigFS.ReadFile("mainnet/byron-genesis.json")
	require.NoError(t, err)
	completed, err := mergeByronGenesisDefaults(
		[]byte(byronGenesisJSON), defaults,
	)
	require.NoError(t, err)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "byron-genesis.json"),
		completed,
		0o644,
	))
	cfg, err := NewCardanoNodeConfigFromReader(
		bytes.NewReader([]byte(`{"ByronGenesisFile": "byron-genesis.json"}`)),
	)
	require.NoError(t, err)
	cfg.path = dir
	return cfg
}

type byronJSONEntry struct {
	key   string
	value json.RawMessage
}

func mergeByronGenesisDefaults(input, defaults []byte) ([]byte, error) {
	inputEntries, err := decodeByronJSONObject(input)
	if err != nil {
		return nil, err
	}
	defaultEntries, err := decodeByronJSONObject(defaults)
	if err != nil {
		return nil, err
	}
	inputKeys := make(map[string]struct{}, len(inputEntries))
	for _, entry := range inputEntries {
		inputKeys[entry.key] = struct{}{}
	}
	merged := make([]byronJSONEntry, 0, len(inputEntries)+len(defaultEntries))
	for _, entry := range defaultEntries {
		if _, supplied := inputKeys[entry.key]; !supplied {
			merged = append(merged, entry)
		}
	}
	seen := make(map[string]struct{}, len(inputEntries))
	for idx, entry := range inputEntries {
		if _, alreadySeen := seen[entry.key]; !alreadySeen {
			seen[entry.key] = struct{}{}
			for _, defaultEntry := range defaultEntries {
				if defaultEntry.key != entry.key || !isJSONObject(entry.value) ||
					!isJSONObject(defaultEntry.value) {
					continue
				}
				entry.value, err = mergeByronGenesisDefaults(
					entry.value,
					defaultEntry.value,
				)
				if err != nil {
					return nil, err
				}
				inputEntries[idx].value = entry.value
				break
			}
		}
		merged = append(merged, inputEntries[idx])
	}
	var result bytes.Buffer
	result.WriteByte('{')
	for idx, entry := range merged {
		if idx > 0 {
			result.WriteByte(',')
		}
		key, err := json.Marshal(entry.key)
		if err != nil {
			return nil, err
		}
		result.Write(key)
		result.WriteByte(':')
		result.Write(entry.value)
	}
	result.WriteByte('}')
	return result.Bytes(), nil
}

func decodeByronJSONObject(raw []byte) ([]byronJSONEntry, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if opening != json.Delim('{') {
		return nil, fmt.Errorf("expected JSON object, got %v", opening)
	}
	entries := make([]byronJSONEntry, 0)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("expected JSON object key, got %T", keyToken)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		entries = append(entries, byronJSONEntry{key: key, value: value})
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return entries, nil
}

func isJSONObject(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '{'
}

// TestLoadGenesisConfigsByronDuplicateKeys covers dingo#4424: the reference
// Byron genesis parser preserves every duplicate object member in the
// canonical bytes used for the genesis hash, but resolves duplicate keys by
// first occurrence when populating configuration fields. Before the fix,
// canonicalizeByronGenesisJSON round-tripped through encoding/json's
// map[string]any, which discards every duplicate but the last for both the
// hash and the decoded struct.
func TestLoadGenesisConfigsByronDuplicateKeys(t *testing.T) {
	t.Parallel()

	t.Run("duplicate protocolMagic in a nested object", func(t *testing.T) {
		t.Parallel()

		cfg := writeByronGenesisConfig(t, `{
  "protocolConsts": {"protocolMagic": 42, "protocolMagic": 43, "k": 2160}
}`)
		require.NoError(t, cfg.loadGenesisConfigs())

		g := cfg.ByronGenesis()
		require.NotNil(t, g)
		assert.Equal(
			t,
			42,
			g.ProtocolConsts.ProtocolMagic,
			"first occurrence must win for the decoded field",
		)

		canonical, err := canonicalizeByronGenesisJSON(
			[]byte(`{"protocolConsts": {"protocolMagic": 42, "protocolMagic": 43, "k": 2160}}`),
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			2,
			strings.Count(string(canonical), `"protocolMagic":`),
			"both duplicate entries must survive in the hashed canonical bytes: %s",
			canonical,
		)
	})

	t.Run("duplicate k at the top level of protocolConsts", func(t *testing.T) {
		t.Parallel()

		cfg := writeByronGenesisConfig(t, `{
  "protocolConsts": {"k": 2160, "k": 1, "protocolMagic": 42}
}`)
		require.NoError(t, cfg.loadGenesisConfigs())

		g := cfg.ByronGenesis()
		require.NotNil(t, g)
		assert.Equal(t, 2160, g.ProtocolConsts.K)
	})

	t.Run("duplicate top-level object key", func(t *testing.T) {
		t.Parallel()

		cfg := writeByronGenesisConfig(t, `{
  "startTime": 1,
  "startTime": 2,
  "protocolConsts": {"k": 2160}
}`)
		require.NoError(t, cfg.loadGenesisConfigs())

		g := cfg.ByronGenesis()
		require.NotNil(t, g)
		assert.Equal(t, 1, g.StartTime, "first occurrence must win at the top level too")

		canonical, err := canonicalizeByronGenesisJSON(
			[]byte(`{"startTime": 1, "startTime": 2}`),
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			2,
			strings.Count(string(canonical), `"startTime":`),
			"both top-level duplicates must survive hashing: %s",
			canonical,
		)
	})
}

// TestCanonicalizeByronGenesisJSONRejectsNonReferenceEscapes covers
// dingo#4425. The Byron reference's canonical-JSON grammar accepts only the
// \" and \\ string escapes; encoding/json (and general JSON) also accept
// \uXXXX, \/, \n, \r, \t, \b and \f, and previously canonicalizeByronGenesisJSON
// silently normalized those before hashing, so it could accept and hash a
// document the reference would refuse to parse at all.
func TestCanonicalizeByronGenesisJSONRejectsNonReferenceEscapes(t *testing.T) {
	t.Parallel()

	t.Run("plain protocolMagic key succeeds", func(t *testing.T) {
		t.Parallel()

		_, err := canonicalizeByronGenesisJSON(
			[]byte(`{"protocolMagic": 42}`),
		)
		require.NoError(t, err)
	})

	t.Run("unicode-escaped key is rejected", func(t *testing.T) {
		t.Parallel()

		// Built by concatenation, not a single raw-string literal: writing a
		// literal backslash-u escape inline is fragile to transcode through
		// tooling, so the escape is assembled explicitly byte by byte here.
		body := []byte(`{"protocolM`)
		body = append(body, '\\', 'u', '0', '0', '6', '1')
		body = append(body, []byte(`gic": 42}`)...)

		_, err := canonicalizeByronGenesisJSON(body)
		require.Error(t, err)
	})

	t.Run("escaped forward slash in a string value is rejected", func(t *testing.T) {
		t.Parallel()

		_, err := canonicalizeByronGenesisJSON(
			[]byte(`{"foo": "abc\/def"}`),
		)
		require.Error(t, err)
	})

	t.Run("literal forward slash in a string value is accepted", func(t *testing.T) {
		t.Parallel()

		_, err := canonicalizeByronGenesisJSON(
			[]byte(`{"foo": "abc/def"}`),
		)
		require.NoError(t, err)
	})

	for _, escape := range []string{`\n`, `\r`, `\t`, `\b`, `\f`} {
		t.Run("rejects "+escape, func(t *testing.T) {
			t.Parallel()

			_, err := canonicalizeByronGenesisJSON(
				[]byte(`{"foo": "a` + escape + `b"}`),
			)
			require.Error(t, err)
		})
	}

	t.Run("via the full config load path", func(t *testing.T) {
		t.Parallel()

		// The escaped key unescapes to the legitimate field name
		// "protocolMagic" and protocolConsts.k is set to a valid value, so
		// the only possible failure is the escape grammar itself -- not an
		// unknown-field error from gouroboros's DisallowUnknownFields, and
		// not the unrelated "security parameter must be positive" check in
		// validateSecurityParameters.
		body := []byte(`{"protocolConsts": {"k": 2160, "`)
		body = append(body, 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l', 'M')
		body = append(body, '\\', 'u', '0', '0', '6', '1')
		body = append(body, []byte(`gic": 42}}`)...)

		dir := t.TempDir()
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, "byron-genesis.json"),
			body,
			0o644,
		))
		cfg, err := NewCardanoNodeConfigFromReader(
			bytes.NewReader([]byte(`{"ByronGenesisFile": "byron-genesis.json"}`)),
		)
		require.NoError(t, err)
		cfg.path = dir

		err = cfg.loadGenesisConfigs()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "byron genesis JSON")
		assert.NotContains(t, err.Error(), "unknown field")
		assert.NotContains(t, err.Error(), "must be positive")
	})
}

// TestCanonicalizeByronGenesisJSONAcceptsReferenceSupportedEscapes covers the
// positive half of dingo#4425's acceptance criteria: \" and \\ are the two
// escapes the Byron reference does support, and must keep working -- both
// for parsing (the document is still accepted) and for the value itself
// (the escaped character survives into the canonical hash bytes correctly
// re-escaped, not doubled or dropped).
func TestCanonicalizeByronGenesisJSONAcceptsReferenceSupportedEscapes(t *testing.T) {
	t.Parallel()

	t.Run("escaped quote", func(t *testing.T) {
		t.Parallel()

		canonical, err := canonicalizeByronGenesisJSON(
			[]byte(`{"foo": "a\"b"}`),
		)
		require.NoError(t, err)
		assert.Contains(t, string(canonical), `"a\"b"`)
	})

	t.Run("escaped backslash", func(t *testing.T) {
		t.Parallel()

		canonical, err := canonicalizeByronGenesisJSON(
			[]byte(`{"foo": "a\\b"}`),
		)
		require.NoError(t, err)
		assert.Contains(t, string(canonical), `"a\\b"`)
	})

	t.Run("both, via the full config load path", func(t *testing.T) {
		t.Parallel()

		cfg := writeByronGenesisConfig(t, `{
  "protocolConsts": {"k": 2160},
  "avvmDistr": {"a\"b\\c": "1"}
}`)
		require.NoError(t, cfg.loadGenesisConfigs())
	})
}

// TestCanonicalizeByronGenesisJSONKeyOrderFlexibility covers the
// "preserve ... object-key-order flexibility" acceptance criterion: the
// reference does not require an object's members to already be sorted on
// input, only that they get sorted (stably, so duplicates are unaffected)
// when producing the canonical hash bytes. Two documents whose (non-
// duplicate) keys appear in a different order must hash identically.
func TestCanonicalizeByronGenesisJSONKeyOrderFlexibility(t *testing.T) {
	t.Parallel()

	inOrder, err := canonicalizeByronGenesisJSON(
		[]byte(`{"protocolConsts": {"k": 2160, "protocolMagic": 42}}`),
	)
	require.NoError(t, err)

	reordered, err := canonicalizeByronGenesisJSON(
		[]byte(`{"protocolConsts": {"protocolMagic": 42, "k": 2160}}`),
	)
	require.NoError(t, err)

	assert.Equal(
		t,
		string(inOrder),
		string(reordered),
		"key order in the input must not affect the canonical hash bytes",
	)
}

// TestLoadGenesisConfigsRejectsNegativeByronSlotDuration covers dingo#4427:
// gouroboros's Byron genesis SlotDuration field is a signed int decoded from
// a JSON string, so a genesis can carry slotDuration "-1". Before the fix,
// nothing rejected that at genesis load time, and it reached a bare
// uint(...) conversion in the Byron era-shape calculation
// (ledger/eras/byron.go), wrapping to a very large unsigned duration instead
// of failing where the bad value was introduced.
func TestLoadGenesisConfigsRejectsNegativeByronSlotDuration(t *testing.T) {
	t.Parallel()

	cfg := writeByronGenesisConfig(t, `{
  "blockVersionData": {"slotDuration": "-1"}
}`)
	err := cfg.loadGenesisConfigs()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "slotDuration")
	assert.Contains(t, err.Error(), "negative")
}

// TestLoadGenesisConfigsAcceptsNonNegativeByronSlotDuration is the companion
// positive case, proving the new check does not reject ordinary genesis
// documents.
func TestLoadGenesisConfigsAcceptsNonNegativeByronSlotDuration(t *testing.T) {
	t.Parallel()

	cfg := writeByronGenesisConfig(t, `{
  "blockVersionData": {"slotDuration": "20000"},
  "protocolConsts": {"k": 2160}
}`)
	require.NoError(t, cfg.loadGenesisConfigs())
	g := cfg.ByronGenesis()
	require.NotNil(t, g)
	assert.Equal(t, 20000, g.BlockVersionData.SlotDuration)
}
