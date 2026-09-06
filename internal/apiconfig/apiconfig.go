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

// Package apiconfig defines the TLS and authentication configuration
// surface shared by every built-in API provider (Blockfrost, Mesh,
// UTxORPC): a top-level `api.tls`/`api.auth` default policy that each
// provider's own `plugins.api.<name>.config.tls`/`config.auth` can
// override field by field. See ARCHITECTURE.md's "API security" section
// for the overall design and dingo#2996/#2998 for the issues this
// implements.
//
// TLSPolicy and AuthPolicy are the YAML-decodable, tri-state
// representations used at both the top-level `api:` scope and the
// provider-config scope: every field is a pointer so "not set at this
// scope" (nil, inherit from a broader scope or fall back to the disabled
// default) is distinguishable from an explicit, zero-value-looking
// setting such as `mode: disabled`. Merge folds two scopes together field
// by field (MergeTLS, MergeAuth); Resolve validates a fully merged policy
// and turns it into the concrete EffectiveTLS/EffectiveAuth a listener
// actually acts on.
package apiconfig

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// AuthMode discriminates a (resolved or override) authentication policy's
// behavior.
type AuthMode string

const (
	// AuthModeDisabled accepts every request without checking credentials.
	// It is also the effective default when no scope sets auth.mode at
	// all, so upgrading a deployment that never configured authentication
	// changes nothing -- existing reverse-proxy/no-auth deployments keep
	// working unmodified.
	AuthModeDisabled AuthMode = "disabled"
	// AuthModeToken requires a shared-secret bearer credential; see Token/
	// TokenFilePath on AuthPolicy.
	AuthModeToken AuthMode = "token"
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

// AuthPolicy is TLSPolicy's authentication counterpart.
type AuthPolicy struct {
	// Mode is AuthModeDisabled or AuthModeToken. Nil resolves to
	// AuthModeDisabled unless a broader scope sets it.
	Mode *string `yaml:"mode,omitempty"          envconfig:"DINGO_API_AUTH_MODE"`
	// Token is the shared secret presented credentials are compared
	// against, inline in configuration. Mutually exclusive with
	// TokenFilePath. Never logged -- see LogValue. Deliberately has no
	// environment variable binding of its own (unlike every other field
	// here): an inline secret is still settable via YAML, but not via an
	// environment variable, to avoid encouraging a pattern where the
	// secret ends up duplicated into process-inspection-visible places
	// such as a container's env dump. Use TokenFilePath for anything but
	// local testing.
	//
	// The `ignored:"true"` tag is load-bearing, not decorative: envconfig
	// auto-derives an environment variable name for every exported struct
	// field it walks even without an explicit `envconfig` tag (here,
	// CARDANO_API_AUTH_TOKEN), so omitting envconfig alone does not
	// actually suppress a binding -- only `ignored:"true"` does.
	Token *string `yaml:"token,omitempty"                                                    ignored:"true"`
	// TokenFilePath names a file whose trimmed contents are the shared
	// secret, read at listener startup (not at Resolve time, matching
	// TLSPolicy's own deferral of certificate loading to listener
	// startup). Mutually exclusive with Token.
	TokenFilePath *string `yaml:"tokenFilePath,omitempty" envconfig:"DINGO_API_AUTH_TOKEN_FILE_PATH"`
}

// EffectiveTLS is a fully resolved and validated TLS policy: the concrete
// answer a listener needs to decide whether, and how, to serve TLS.
type EffectiveTLS struct {
	Enabled      bool
	CertFilePath string
	KeyFilePath  string
}

// EffectiveAuth is a fully resolved and validated authentication policy.
type EffectiveAuth struct {
	Enabled bool
	// Token is the inline shared secret, if configured directly. Empty
	// when TokenFilePath is set instead.
	Token string
	// TokenFilePath is the shared-secret file path, if configured that
	// way. Empty when Token is set directly.
	TokenFilePath string
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

// ListenHost resolves the bind address one API listener uses: its own
// plugins.api.<name>.config.host when that is set, otherwise the shared
// apiBindAddr default composition passes down. Every provider resolves it
// the same way here, so a per-provider override and the shared default
// cannot disagree between the validation that checks for a port conflict
// and the listener that actually binds. See ARCHITECTURE.md's "API
// security" section.
func ListenHost(configured, fallback string) string {
	if configured != "" {
		return configured
	}
	return fallback
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

// MergeAuth is MergeTLS's authentication counterpart, except that Token and
// TokenFilePath are merged as a single credential-source unit rather than
// independently per field like Mode: Resolve rejects a policy with both
// fields set (they are mutually exclusive), so if override explicitly sets
// either one, it is switching the credential source and override's values
// for both fields replace base's wholesale -- base's Token/TokenFilePath
// are not carried forward to mix with override's. Only when override sets
// neither field does base's credential source (Token and TokenFilePath
// together) pass through unchanged.
func MergeAuth(base, override AuthPolicy) AuthPolicy {
	merged := AuthPolicy{
		Mode: firstSet(override.Mode, base.Mode),
	}
	if override.Token != nil || override.TokenFilePath != nil {
		merged.Token = override.Token
		merged.TokenFilePath = override.TokenFilePath
	} else {
		merged.Token = base.Token
		merged.TokenFilePath = base.TokenFilePath
	}
	return merged
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

// Resolve is TLSPolicy.Resolve's authentication counterpart. It does not
// read TokenFilePath from disk -- that happens at listener startup.
func (p AuthPolicy) Resolve(path string) (EffectiveAuth, error) {
	mode := AuthModeDisabled
	if m := stringVal(p.Mode); m != "" {
		mode = AuthMode(m)
	}
	switch mode {
	case AuthModeDisabled:
		return EffectiveAuth{}, nil
	case AuthModeToken:
		token := stringVal(p.Token)
		tokenFile := stringVal(p.TokenFilePath)
		if token != "" && tokenFile != "" {
			return EffectiveAuth{}, fmt.Errorf(
				"%s: token and tokenFilePath are mutually exclusive",
				path,
			)
		}
		if token == "" && tokenFile == "" {
			return EffectiveAuth{}, fmt.Errorf(
				"%s: token or tokenFilePath is required when mode is %q",
				path, AuthModeToken,
			)
		}
		return EffectiveAuth{
			Enabled:       true,
			Token:         token,
			TokenFilePath: tokenFile,
		}, nil
	default:
		return EffectiveAuth{}, fmt.Errorf(
			"%s: invalid mode %q (must be %q or %q)",
			path, mode, AuthModeDisabled, AuthModeToken,
		)
	}
}

// LogValue redacts Token so a structured slog call never reveals the
// shared secret. TokenFilePath is a filesystem path, not a secret, and is
// logged as-is.
func (p AuthPolicy) LogValue() slog.Value {
	var attrs []slog.Attr
	if p.Mode != nil {
		attrs = append(attrs, slog.String("mode", *p.Mode))
	}
	if p.Token != nil {
		attrs = append(attrs, slog.String("token", "***redacted***"))
	}
	if p.TokenFilePath != nil {
		attrs = append(attrs, slog.String("tokenFilePath", *p.TokenFilePath))
	}
	return slog.GroupValue(attrs...)
}

// LogValue redacts Token so a structured slog call never reveals the
// shared secret.
func (a EffectiveAuth) LogValue() slog.Value {
	attrs := []slog.Attr{slog.Bool("enabled", a.Enabled)}
	if a.Token != "" {
		attrs = append(attrs, slog.String("token", "***redacted***"))
	}
	if a.TokenFilePath != "" {
		attrs = append(attrs, slog.String("tokenFilePath", a.TokenFilePath))
	}
	return slog.GroupValue(attrs...)
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

// DecodeAuthPolicy extracts raw["auth"] into an AuthPolicy, or the zero
// value if raw has no "auth" key.
func DecodeAuthPolicy(raw map[string]any) (AuthPolicy, error) {
	return decodeSection[AuthPolicy](raw, "auth")
}

func isZeroTLS(p TLSPolicy) bool {
	return p.Mode == nil && p.CertFilePath == nil && p.KeyFilePath == nil
}

func isZeroAuth(p AuthPolicy) bool {
	return p.Mode == nil && p.Token == nil && p.TokenFilePath == nil
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

// MergeProviderConfig folds a capability's shared top-level `api.tls`/
// `api.auth` defaults -- and, for legacy-compatibility callers only, an
// additional lower-priority TLS base -- into raw's own "tls"/"auth"
// sections, field by field, and returns a new map; raw itself is never
// mutated. Precedence from lowest to highest: legacyTLS, then apiTLS/
// apiAuth (the shared api.tls/api.auth policy), then whatever raw's own
// "tls"/"auth" sections already set. Every other key in raw (e.g. "port")
// passes through unchanged.
//
// This keeps the merge deterministic and independent of map iteration
// order: each named field resolves through MergeTLS/MergeAuth rather than
// by replacing raw's whole "tls"/"auth" value or iterating it as a
// generic map.
//
// If the merge result is entirely empty (nothing set at any scope) for a
// section, that section's key is left out of the result entirely rather
// than written as an empty mapping -- identical to raw never having had
// that key. This matters for a provider registered under one of these
// capabilities whose own config type has no "tls"/"auth" field at all
// (e.g. a third-party or test provider that predates -- or simply does
// not support -- dingo#2996's shared security surface): as long as no
// operator-visible TLS/auth policy actually applies to it, its config
// still decodes exactly as it did before this merge step existed. Once a
// real policy applies (top level, legacy compat, or the provider's own
// config), the section is written and a provider whose config type can't
// decode it fails -- correctly, since that provider cannot honor the
// policy the operator configured.
func MergeProviderConfig(
	raw map[string]any,
	legacyTLS, apiTLS TLSPolicy,
	apiAuth AuthPolicy,
) (map[string]any, error) {
	providerTLS, err := DecodeTLSPolicy(raw)
	if err != nil {
		return nil, fmt.Errorf("decode tls: %w", err)
	}
	providerAuth, err := DecodeAuthPolicy(raw)
	if err != nil {
		return nil, fmt.Errorf("decode auth: %w", err)
	}
	mergedTLS := MergeTLS(MergeTLS(legacyTLS, apiTLS), providerTLS)
	mergedAuth := MergeAuth(apiAuth, providerAuth)
	result := raw
	if !isZeroTLS(mergedTLS) {
		result, err = setSection(result, "tls", mergedTLS)
		if err != nil {
			return nil, fmt.Errorf("encode merged tls: %w", err)
		}
	}
	if !isZeroAuth(mergedAuth) {
		result, err = setSection(result, "auth", mergedAuth)
		if err != nil {
			return nil, fmt.Errorf("encode merged auth: %w", err)
		}
	}
	return result, nil
}
