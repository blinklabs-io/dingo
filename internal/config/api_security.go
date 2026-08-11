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

package config

import (
	"fmt"

	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// APIConfig is the top-level `api:` configuration section (dingo #2998). It
// supplies shared TLS and authentication defaults to every selected
// built-in API provider (Blockfrost, Mesh, UTxO RPC); a provider overrides
// any field it sets explicitly under its own
// plugins.api.<capability>.config.tls/auth. See ResolveAPISecurity for the
// exact field-by-field precedence and EffectiveAPIPolicy for the legacy
// root-field compatibility fallback.
type APIConfig struct {
	TLS  APITLSPolicy  `yaml:"tls"`
	Auth APIAuthPolicy `yaml:"auth"`
}

// APITLSPolicy is the shared default server-TLS policy for API listeners.
// Mode "server" enables TLS, requiring both CertFilePath and KeyFilePath
// (after a provider's own overrides are applied); "off" and "" both mean
// disabled. This is unrelated to bark's own mutual-TLS client verification
// (TlsClientCAFilePath), which is a separate surface.
type APITLSPolicy struct {
	Mode         string `yaml:"mode"         envconfig:"DINGO_API_TLS_MODE"`
	CertFilePath string `yaml:"certFilePath" envconfig:"DINGO_API_TLS_CERT_FILE_PATH"`
	KeyFilePath  string `yaml:"keyFilePath"  envconfig:"DINGO_API_TLS_KEY_FILE_PATH"`
}

// APIAuthPolicy is the shared default authentication policy for API
// listeners. Mode "token" requires clients to present a credential (an
// "Authorization: Bearer <token>" header, or, as a Blockfrost-compatible
// alias, a "project_id: <token>" header) matching the contents of
// TokenFilePath; see internal/apiauth. "none" and "" both mean no
// in-process authentication.
type APIAuthPolicy struct {
	Mode          string `yaml:"mode"          envconfig:"DINGO_API_AUTH_MODE"`
	TokenFilePath string `yaml:"tokenFilePath" envconfig:"DINGO_API_AUTH_TOKEN_FILE_PATH"`
}

// EffectiveAPIPolicy returns the top-level API security policy with the
// legacy root-level TLS compatibility fallback applied: TlsCertFilePath/
// TlsKeyFilePath (which predate this section and also still configure bark
// and Midnight) continue to enable TLS for every API provider when api.tls
// itself is entirely unset, so upgrading an existing deployment does not
// silently drop its API TLS listener. Setting any of api.tls.mode,
// api.tls.certFilePath, or api.tls.keyFilePath opts fully into the new
// section; the root fields then have no effect on the API policy (they
// still apply to bark/Midnight, which do not participate in this
// section). Composition should call this instead of reading c.API
// directly, and should prefer api.tls in new configuration over the root
// fields.
func (c *Config) EffectiveAPIPolicy() APIConfig {
	policy := c.API
	if policy.TLS.Mode == "" && policy.TLS.CertFilePath == "" &&
		policy.TLS.KeyFilePath == "" &&
		(c.TlsCertFilePath != "" || c.TlsKeyFilePath != "") {
		policy.TLS = APITLSPolicy{
			Mode:         "server",
			CertFilePath: c.TlsCertFilePath,
			KeyFilePath:  c.TlsKeyFilePath,
		}
	}
	return policy
}

// ResolvedAPISecurity is the fully merged, per-provider effective TLS and
// authentication policy: a concrete, instance-owned settings snapshot with
// no further resolution left for a provider to do. TLSCertFilePath/
// TLSKeyFilePath are only ever non-empty when TLSMode is "server", and
// AuthTokenFilePath is only ever non-empty when AuthMode is "token" --
// callers do not need to re-check the mode before using the other fields.
type ResolvedAPISecurity struct {
	TLSMode           string
	TLSCertFilePath   string
	TLSKeyFilePath    string
	AuthMode          string
	AuthTokenFilePath string
}

// ResolveAPISecurity computes the effective TLS/auth policy for one API
// provider selection. Resolution is field-by-field and deterministic,
// independent of map iteration order: each field is chosen by checking, in
// order, (1) whether the provider's own
// plugins.api.<capability>.config.tls/auth mapping explicitly sets that
// field to a non-empty value, (2) whether apiPolicy (see
// EffectiveAPIPolicy) sets it, then (3) a fixed default ("off"/"none", ""
// for paths) equivalent to today's pre-#2998 behavior. An explicit
// empty-string provider field is treated the same as an absent one -- only
// a real value (including the disabling sentinels "off"/"none") counts as
// an override -- so a provider can affirmatively disable an inherited
// policy but cannot ambiguously blank one out.
func ResolveAPISecurity(
	apiPolicy APIConfig,
	selection hostplugin.Selection,
) ResolvedAPISecurity {
	providerTLS, _ := selection.Config["tls"].(map[string]any)
	providerAuth, _ := selection.Config["auth"].(map[string]any)
	sec := ResolvedAPISecurity{
		TLSMode: resolveAPIPolicyField(
			providerTLS, "mode", apiPolicy.TLS.Mode, "off",
		),
		TLSCertFilePath: resolveAPIPolicyField(
			providerTLS, "certFilePath", apiPolicy.TLS.CertFilePath, "",
		),
		TLSKeyFilePath: resolveAPIPolicyField(
			providerTLS, "keyFilePath", apiPolicy.TLS.KeyFilePath, "",
		),
		AuthMode: resolveAPIPolicyField(
			providerAuth, "mode", apiPolicy.Auth.Mode, "none",
		),
		AuthTokenFilePath: resolveAPIPolicyField(
			providerAuth, "tokenFilePath", apiPolicy.Auth.TokenFilePath, "",
		),
	}
	// Normalize the resolved struct to its final answer so callers never
	// need mode-conditional logic of their own: a provider whose effective
	// mode is not "server"/"token" has no business reading leftover
	// path fields from a policy layer it isn't using.
	if sec.TLSMode != "server" {
		sec.TLSCertFilePath = ""
		sec.TLSKeyFilePath = ""
	}
	if sec.AuthMode != "token" {
		sec.AuthTokenFilePath = ""
	}
	return sec
}

// resolveAPIPolicyField resolves a single field's effective value given a
// provider's own raw config sub-map (nil if the provider does not have
// that block at all), the top-level default, and the built-in fallback.
func resolveAPIPolicyField(
	providerFields map[string]any,
	key, topLevel, fallback string,
) string {
	if providerFields != nil {
		if raw, ok := providerFields[key]; ok {
			if s, ok := raw.(string); ok && s != "" {
				return s
			}
		}
	}
	if topLevel != "" {
		return topLevel
	}
	return fallback
}

// ValidateAPISecurity checks one resolved effective API security policy,
// returning every problem found. path identifies the full configuration
// location the operator should look at (e.g.
// "plugins.api.blockfrost.config" or "api").
func ValidateAPISecurity(path string, sec ResolvedAPISecurity) []error {
	var errs []error
	switch sec.TLSMode {
	case "", "off", "server":
	default:
		errs = append(errs, fmt.Errorf(
			"invalid %s.tls.mode %q: must be \"off\" or \"server\"",
			path, sec.TLSMode,
		))
	}
	if sec.TLSMode == "server" &&
		(sec.TLSCertFilePath == "" || sec.TLSKeyFilePath == "") {
		errs = append(errs, fmt.Errorf(
			"%s.tls: mode is \"server\" but certFilePath and keyFilePath "+
				"must both be set (got certFilePath=%q, keyFilePath=%q)",
			path, sec.TLSCertFilePath, sec.TLSKeyFilePath,
		))
	}
	switch sec.AuthMode {
	case "", "none", "token":
	default:
		errs = append(errs, fmt.Errorf(
			"invalid %s.auth.mode %q: must be \"none\" or \"token\"",
			path, sec.AuthMode,
		))
	}
	if sec.AuthMode == "token" && sec.AuthTokenFilePath == "" {
		errs = append(errs, fmt.Errorf(
			"%s.auth: mode is \"token\" but tokenFilePath is not set",
			path,
		))
	}
	return errs
}
