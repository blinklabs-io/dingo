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

// Package apiconfig defines TLS configuration shared by built-in API providers.
package apiconfig

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"

	"gopkg.in/yaml.v3"
)

// TLSMode discriminates a (resolved or override) TLS policy's behavior.
type TLSMode string

const (
	// TLSModeDisabled turns TLS off, even overriding a broader scope that
	// enables it. It is also the effective default when no scope sets
	// tls.mode at all, so upgrading a deployment that never configured TLS
	// for a given provider changes nothing.
	TLSModeDisabled TLSMode = "disabled"
	// TLSModeServer serves TLS using CertFilePath/KeyFilePath.
	TLSModeServer TLSMode = "server"
)

// TLSPolicy is the YAML shape of both the top-level `api.tls` defaults and
// every `plugins.api.<name>.config.tls` override. A nil field means "not
// set at this scope"; MergeTLS lets a narrower scope's explicit field win
// over a broader scope's, independently per field.
type TLSPolicy struct {
	// Mode is TLSModeDisabled or TLSModeServer. Nil resolves to
	// TLSModeDisabled unless a broader scope sets it. The envconfig tags
	// here take effect only when this type is embedded in a struct
	// envconfig.Process actually walks (internal/config.APIConfig, for
	// the top-level api.tls policy); a provider's own
	// plugins.api.<name>.config.tls decodes through YAML only (see
	// plugin.Register's strict decode) and ignores them.
	Mode         *string `yaml:"mode,omitempty"         envconfig:"DINGO_API_TLS_MODE"`
	CertFilePath *string `yaml:"certFilePath,omitempty" envconfig:"DINGO_API_TLS_CERT_FILE_PATH"`
	KeyFilePath  *string `yaml:"keyFilePath,omitempty"  envconfig:"DINGO_API_TLS_KEY_FILE_PATH"`
}

// EffectiveTLS is a fully resolved and validated TLS policy: the concrete
// answer a listener needs to decide whether, and how, to serve TLS.
type EffectiveTLS struct {
	Enabled      bool
	CertFilePath string
	KeyFilePath  string
}

func stringVal(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func firstSet(override, base *string) *string {
	if override != nil {
		return override
	}
	return base
}

// MergeTLS resolves override's fields against base, field by field:
// whatever override explicitly sets (including an explicit "disabled"
// Mode) wins outright; any field override leaves nil falls back to base.
// The result does not depend on how base or override were themselves
// constructed (e.g. any map iteration order upstream), because merging
// happens per named struct field rather than by replacing the whole
// policy or walking a generic map.
func MergeTLS(base, override TLSPolicy) TLSPolicy {
	return TLSPolicy{
		Mode:         firstSet(override.Mode, base.Mode),
		CertFilePath: firstSet(override.CertFilePath, base.CertFilePath),
		KeyFilePath:  firstSet(override.KeyFilePath, base.KeyFilePath),
	}
}

// Resolve validates p and returns the concrete TLS behavior it selects.
// path identifies p's position in the configuration tree (e.g.
// "plugins.api.blockfrost.config.tls") purely to make a returned error
// actionable; it does not affect resolution. Resolve does not read
// CertFilePath/KeyFilePath from disk -- that happens at listener startup,
// matching the existing UTxO RPC precedent of surfacing a missing/invalid
// certificate file as a listener startup error rather than a config
// validation error.
func (p TLSPolicy) Resolve(path string) (EffectiveTLS, error) {
	mode := TLSModeDisabled
	if m := stringVal(p.Mode); m != "" {
		mode = TLSMode(m)
	}
	switch mode {
	case TLSModeDisabled:
		return EffectiveTLS{}, nil
	case TLSModeServer:
		cert := stringVal(p.CertFilePath)
		key := stringVal(p.KeyFilePath)
		if (cert == "") != (key == "") {
			return EffectiveTLS{}, fmt.Errorf(
				"%s: certFilePath and keyFilePath must both be set (only one is set)",
				path,
			)
		}
		if cert == "" {
			return EffectiveTLS{}, fmt.Errorf(
				"%s: certFilePath and keyFilePath are required when mode is %q",
				path, TLSModeServer,
			)
		}
		return EffectiveTLS{
			Enabled:      true,
			CertFilePath: cert,
			KeyFilePath:  key,
		}, nil
	default:
		return EffectiveTLS{}, fmt.Errorf(
			"%s: invalid mode %q (must be %q or %q)",
			path, mode, TLSModeDisabled, TLSModeServer,
		)
	}
}

// decodeSection extracts raw[key] (if present) into a T using the same
// strict-ish YAML decoding provider configs already use elsewhere, so a
// caller working purely in map[string]any (a plugin.Selection.Config) gets
// the identical field shapes a typed ProviderConfig would.
func decodeSection[T any](raw map[string]any, key string) (T, error) {
	var dst T
	if raw == nil {
		return dst, nil
	}
	value, ok := raw[key]
	if !ok {
		return dst, nil
	}
	data, err := yaml.Marshal(value)
	if err != nil {
		return dst, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&dst); err != nil && !errors.Is(err, io.EOF) {
		return dst, err
	}
	return dst, nil
}

// DecodeTLSPolicy extracts raw["tls"] into a TLSPolicy, or the zero value
// if raw has no "tls" key.
func DecodeTLSPolicy(raw map[string]any) (TLSPolicy, error) {
	return decodeSection[TLSPolicy](raw, "tls")
}

func isZeroTLS(p TLSPolicy) bool {
	return p.Mode == nil && p.CertFilePath == nil && p.KeyFilePath == nil
}

func setSection(
	raw map[string]any,
	key string,
	policy any,
) (map[string]any, error) {
	data, err := yaml.Marshal(policy)
	if err != nil {
		return nil, err
	}
	var value map[string]any
	if err := yaml.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	clone := make(map[string]any, len(raw)+1)
	maps.Copy(clone, raw)
	clone[key] = value
	return clone, nil
}

// MergeProviderConfig folds a shared top-level `api.tls` default and a
// lower-priority TLS base into raw's own "tls" section, field by field, and
// returns a new map; raw itself is never mutated. Precedence from lowest to
// highest is legacyTLS, apiTLS, then raw's own "tls" section. Every other key
// in raw (e.g. "port")
// passes through unchanged.
//
// This keeps the merge deterministic and independent of map iteration
// order: each named field resolves through MergeTLS rather than by replacing
// raw's whole "tls" value or iterating it as a
// generic map.
//
// If the merge result is entirely empty, the tls key is left out entirely rather
// than written as an empty mapping -- identical to raw never having had
// that key. This matters for providers whose config type has no "tls" field:
// as long as no operator-visible TLS policy actually applies to it, its config
// still decodes exactly as it did before this merge step existed. Once a
// real TLS policy applies (top level, legacy compat, or the provider's own
// config), the section is written and a provider whose config type can't
// decode it fails -- correctly, since that provider cannot honor the
// policy the operator configured.
func MergeProviderConfig(
	raw map[string]any,
	legacyTLS, apiTLS TLSPolicy,
) (map[string]any, error) {
	providerTLS, err := DecodeTLSPolicy(raw)
	if err != nil {
		return nil, fmt.Errorf("decode tls: %w", err)
	}
	mergedTLS := MergeTLS(MergeTLS(legacyTLS, apiTLS), providerTLS)
	result := raw
	if !isZeroTLS(mergedTLS) {
		result, err = setSection(result, "tls", mergedTLS)
		if err != nil {
			return nil, fmt.Errorf("encode merged tls: %w", err)
		}
	}
	return result, nil
}
