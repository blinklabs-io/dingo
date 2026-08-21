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
	"bytes"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// configLeafFieldPaths returns the dotted Go field path of every exported
// leaf (non-struct) field reachable from Config, which is exactly the set
// configLogClasses has to classify.
func configLeafFieldPaths(t *testing.T) []string {
	t.Helper()

	var paths []string
	var walk func(rt reflect.Type, prefix string)
	walk = func(rt reflect.Type, prefix string) {
		for field := range rt.Fields() {
			if !field.IsExported() {
				continue
			}
			ft := field.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			path := prefix + field.Name
			if ft.Kind() == reflect.Struct {
				walk(ft, path+".")
				continue
			}
			paths = append(paths, path)
		}
	}
	walk(reflect.TypeFor[Config](), "")
	slices.Sort(paths)
	return paths
}

// TestConfigLogClassesCoverEveryConfigField is the fail-safe gate on the
// redacted representation. An unclassified field resolves to logSecret, so
// adding one cannot leak a secret -- but it would silently reduce a benign
// new field to ***redacted*** in every operator's debug log. Failing here
// forces an explicit classification decision instead.
func TestConfigLogClassesCoverEveryConfigField(t *testing.T) {
	t.Parallel()

	classes := configLogClasses()
	paths := configLeafFieldPaths(t)
	for _, path := range paths {
		if _, ok := classes[path]; !ok {
			t.Errorf(
				"Config field %s has no logClass: add it to one of "+
					"the logPlain/logSecret/logURI/logProviderConfig "+
					"field lists in redact.go",
				path,
			)
		}
	}
	// A stale entry means a field was renamed or removed and its
	// classification no longer applies to anything.
	for path := range classes {
		if !slices.Contains(paths, path) {
			t.Errorf(
				"redact.go classifies %s, which is not a Config field",
				path,
			)
		}
	}
}

// TestConfigLogClassesAreUnambiguous catches a field path listed under two
// different classes, where the effective class would depend on map
// iteration order.
func TestConfigLogClassesAreUnambiguous(t *testing.T) {
	t.Parallel()

	seen := make(map[string]int)
	for _, paths := range [][]string{
		logPlainConfigFields,
		logSecretConfigFields,
		logURIConfigFields,
		logProviderConfigFields,
	} {
		for _, path := range paths {
			seen[path]++
		}
	}
	for path, count := range seen {
		if count > 1 {
			t.Errorf("Config field %s is classified %d times", path, count)
		}
	}
}

// sentinelSecrets are the distinctive values planted in the configuration
// below. None of them may appear in a rendered log line.
var sentinelSecrets = []string{
	"SENTINEL-API-AUTH-TOKEN",
	"SENTINEL-KOIOS-API-KEY",
	"SENTINEL-BARK-PASSWORD",
	"SENTINEL-BARK-HOST-PASSWORD",
	"SENTINEL-MITHRIL-KEY",
	"SENTINEL-S3-SECRET-KEY",
	"SENTINEL-TOKEN-REGISTRY-PASSWORD",
	"SENTINEL-IPFS-PASSWORD",
	"SENTINEL-PG-PASSWORD",
	"SENTINEL-DSN-PASSWORD",
	"SENTINEL-DSN-KEYWORD-PASSWORD",
	"SENTINEL-UNKNOWN-PROVIDER-KEY",
	"SENTINEL-BLOCKFROST-TOKEN",
	"SENTINEL-NESTED-PROVIDER-SECRET",
	"SENTINEL-MEMPOOL-LIST-SECRET",
	// An AWS access key ID is half of a credential pair, and
	// "accessKeyId" is exactly the spelling a left-anchored
	// access[-_]?key pattern cannot reach past the "Id" suffix.
	"AKIAEXAMPLE",
	"SENTINEL-NESTED-AUTH-SECRET",
}

// sentinelSecretConfig plants a sentinel in every secret-bearing field and
// provider-map key, alongside non-secret values that must survive.
func sentinelSecretConfig() *Config {
	authToken := "SENTINEL-API-AUTH-TOKEN"
	certPath := "/etc/dingo/api.crt"
	return &Config{
		Network:      "preview",
		DatabasePath: "/var/lib/dingo",
		StorageMode:  "api",
		Logging:      LoggingConfig{Format: "json", Level: "debug"},
		API: APIConfig{
			TLS:  apiconfig.TLSPolicy{CertFilePath: &certPath},
			Auth: apiconfig.AuthPolicy{Token: &authToken},
		},
		KoiosParity: KoiosParityConfig{
			Enabled: true,
			APIKey:  "SENTINEL-KOIOS-API-KEY",
		},
		BarkBaseUrl: "https://bark:SENTINEL-BARK-PASSWORD@bark.example/api",
		BarkBlockDownloadHosts: []string{
			"https://blocks:SENTINEL-BARK-HOST-PASSWORD@blocks.example",
			"https://plain.example",
		},
		Mithril: MithrilConfig{
			AggregatorURL: "https://aggregator.example/aggregator" +
				"?apiKey=SENTINEL-MITHRIL-KEY&network=preview",
		},
		TokenRegistry: TokenRegistryConfig{
			SourceURL: "https://reg:SENTINEL-TOKEN-REGISTRY-PASSWORD" +
				"@registry.example/registry.tar.gz",
		},
		OffchainMetadata: OffchainMetadataConfig{
			IPFSGatewayURL: "https://ipfs:SENTINEL-IPFS-PASSWORD" +
				"@ipfs.example/ipfs/",
		},
		DatabaseLifecycle: DatabaseLifecycleConfig{
			SnapshotCloudDestination: "s3://snapshots/dingo" +
				"?accessKeyId=AKIAEXAMPLE" +
				"&secretKey=SENTINEL-S3-SECRET-KEY",
		},
		ShelleyVRFKey: "/etc/dingo/vrf.skey",
		Plugins: PluginsConfig{
			Storage: StoragePluginsConfig{
				Blob: hostplugin.Selection{
					Provider: "badger",
					Config:   map[string]any{"dataDir": "/var/lib/dingo/blob"},
				},
				Metadata: hostplugin.Selection{
					Provider: "postgres",
					Config: map[string]any{
						"host":     "db.example",
						"port":     5432,
						"user":     "dingo",
						"database": "dingo",
						"sslMode":  "require",
						"password": "SENTINEL-PG-PASSWORD",
						"dsn": "postgres://dingo:SENTINEL-DSN-PASSWORD" +
							"@db.example:5432/dingo?sslmode=require",
						"futureKey": "SENTINEL-UNKNOWN-PROVIDER-KEY",
					},
				},
			},
			Mempool: hostplugin.Selection{
				Provider: "dag",
				Config: map[string]any{
					"capacity": 1024,
					"futureList": []any{
						"SENTINEL-MEMPOOL-LIST-SECRET",
					},
				},
			},
			API: APIPluginsConfig{
				Blockfrost: hostplugin.Selection{
					Provider: "blockfrost",
					Config: map[string]any{
						"port": 3000,
						"auth": map[string]any{
							"mode": "token",
							"token": map[string]any{
								"host":  "SENTINEL-NESTED-AUTH-SECRET",
								"value": "SENTINEL-BLOCKFROST-TOKEN",
							},
						},
						"futureSection": map[string]any{
							"deeper": map[string]any{
								"anything": "SENTINEL-NESTED-" +
									"PROVIDER-SECRET",
							},
						},
					},
				},
				Mesh: hostplugin.Selection{
					Provider: "mesh",
					Config: map[string]any{
						"dsn": "mysql://mesh:" +
							"SENTINEL-DSN-KEYWORD-PASSWORD@tcp" +
							"(mesh.example:3306)/mesh",
					},
				},
			},
		},
	}
}

// TestConfigLogValueRedactsSentinelSecrets renders the configuration
// through both real slog handlers and asserts that no sentinel secret
// survives, and that non-secret values still do.
func TestConfigLogValueRedactsSentinelSecrets(t *testing.T) {
	t.Parallel()

	cfg := sentinelSecretConfig()
	handlers := map[string]func(*bytes.Buffer) slog.Handler{
		"text": func(b *bytes.Buffer) slog.Handler {
			return slog.NewTextHandler(b, nil)
		},
		"json": func(b *bytes.Buffer) slog.Handler {
			return slog.NewJSONHandler(b, nil)
		},
	}
	for name, newHandler := range handlers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			slog.New(newHandler(&buf)).Info("config", "config", cfg)
			rendered := buf.String()
			for _, sentinel := range sentinelSecrets {
				if strings.Contains(rendered, sentinel) {
					t.Errorf("rendered log leaks %s", sentinel)
				}
			}
			// Negative case: redaction must not blank the record.
			for _, want := range []string{
				"preview",
				"/var/lib/dingo",
				"/etc/dingo/api.crt",
				"/etc/dingo/vrf.skey",
				"badger",
				"postgres",
				"db.example",
				"5432",
				"dingo",
				"require",
				"aggregator.example",
				"registry.example",
				"blocks.example",
				"plain.example",
				"snapshots/dingo",
				"mesh.example:3306",
				"1024",
			} {
				if !strings.Contains(rendered, want) {
					t.Errorf(
						"rendered log dropped non-secret %q:\n%s",
						want,
						rendered,
					)
				}
			}
			if !strings.Contains(rendered, redactedPlaceholder) {
				t.Errorf("rendered log has no redaction marker:\n%s",
					rendered)
			}
		})
	}
}

// TestConfigLogValueNil covers a nil receiver, which slog resolves the same
// way as any other LogValuer.
func TestConfigLogValueNil(t *testing.T) {
	t.Parallel()

	var cfg *Config
	if got := cfg.LogValue().String(); got != "<nil>" {
		t.Errorf("nil Config LogValue = %q, want <nil>", got)
	}
}

func TestRedactURICredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "postgres url keeps host database and sslmode",
			in: "postgres://dingo:hunter2@db.example:5432/dingo" +
				"?sslmode=require",
			want: "postgres://dingo:" + redactedPlaceholder +
				"@db.example:5432/dingo?sslmode=require",
		},
		{
			name: "mysql dsn without scheme",
			in:   "dingo:hunter2@tcp(db.example:3306)/dingo",
			want: "dingo:" + redactedPlaceholder +
				"@tcp(db.example:3306)/dingo",
		},
		{
			name: "keyword form dsn",
			in:   "host=db.example user=dingo password=hunter2 dbname=dingo",
			want: "host=db.example user=dingo password=" +
				redactedPlaceholder + " dbname=dingo",
		},
		{
			name: "credential query parameter",
			in:   "https://aggregator.example/x?apiKey=abc123&network=preview",
			want: "https://aggregator.example/x?apiKey=" +
				redactedPlaceholder + "&network=preview",
		},
		{
			name: "quoted keyword dsn password containing whitespace",
			in: "host=db.example password='hunter 2 three' " +
				"dbname=dingo",
			want: "host=db.example password=" + redactedPlaceholder +
				" dbname=dingo",
		},
		{
			name: "double quoted keyword dsn password",
			in:   `host=db.example password="hunter 2" dbname=dingo`,
			want: "host=db.example password=" + redactedPlaceholder +
				" dbname=dingo",
		},
		{
			name: "keyword dsn with whitespace around equals",
			in:   "host=db.example password = hunter2 dbname=dingo",
			want: "host=db.example password = " +
				redactedPlaceholder + " dbname=dingo",
		},
		{
			name: "empty keyword value does not consume the next pair",
			in:   "password= dbname=dingo",
			want: "password= dbname=dingo",
		},
		{
			name: "prefixed credential query parameters",
			in: "https://idp.example/token?client_secret=hunter2" +
				"&api_token=abc123&network=preview",
			want: "https://idp.example/token?client_secret=" +
				redactedPlaceholder + "&api_token=" +
				redactedPlaceholder + "&network=preview",
		},
		{
			name: "suffixed credential query parameter",
			in: "s3://snapshots/dingo?accessKeyId=AKIAEXAMPLE" +
				"&secretKey=hunter2&prefix=dingo",
			want: "s3://snapshots/dingo?accessKeyId=" +
				redactedPlaceholder + "&secretKey=" +
				redactedPlaceholder + "&prefix=dingo",
		},
		{
			name: "kebab case credential header parameter",
			in:   "https://api.example/v1?x-api-key=abc123&page=2",
			want: "https://api.example/v1?x-api-key=" +
				redactedPlaceholder + "&page=2",
		},
		{
			name: "semicolon separated query parameters",
			in:   "https://api.example/v1?apiToken=abc123;page=2",
			want: "https://api.example/v1?apiToken=" +
				redactedPlaceholder + ";page=2",
		},
		{
			name: "a parameter naming a file is a path not a credential",
			in:   "https://api.example/v1?tokenFilePath=/etc/dingo/t",
			want: "https://api.example/v1?tokenFilePath=/etc/dingo/t",
		},
		{
			name: "fragment is not scanned as a query parameter",
			in:   "https://api.example/v1?page=2#apiKey=abc123",
			want: "https://api.example/v1?page=2#apiKey=abc123",
		},
		{
			name: "no credentials is unchanged",
			in:   "https://aggregator.example/aggregator",
			want: "https://aggregator.example/aggregator",
		},
		{
			name: "username without password is not a credential",
			in:   "https://dingo@bark.example/api",
			want: "https://dingo@bark.example/api",
		},
		{
			name: "ipv6 host without userinfo is unchanged",
			in:   "http://[::1]:3000/metrics",
			want: "http://[::1]:3000/metrics",
		},
		{
			name: "bucket uri is unchanged",
			in:   "s3://snapshots/dingo",
			want: "s3://snapshots/dingo",
		},
		{
			name: "at sign in path is not userinfo",
			in:   "https://registry.example/a@b/registry.tar.gz",
			want: "https://registry.example/a@b/registry.tar.gz",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := redactURICredentials(tc.in); got != tc.want {
				t.Errorf(
					"redactURICredentials(%q) = %q, want %q",
					tc.in,
					got,
					tc.want,
				)
			}
		})
	}
}

// TestProviderConfigUnknownKeyIsRedacted pins the fail-safe default for a
// provider configuration map: a key nobody has classified is redacted, at
// any nesting depth, rather than logged on the chance that it is benign.
func TestProviderConfigUnknownKeyIsRedacted(t *testing.T) {
	t.Parallel()

	value := providerConfigValue(reflect.ValueOf(map[string]any{
		"host":       "db.example",
		"futureKey":  "unclassified-secret",
		"futureTree": map[string]any{"deeper": "unclassified-nested-secret"},
	}))
	rendered := value.String()
	for _, unwanted := range []string{
		"unclassified-secret",
		"unclassified-nested-secret",
	} {
		if strings.Contains(rendered, unwanted) {
			t.Errorf("provider config leaks %q: %s", unwanted, rendered)
		}
	}
	if !strings.Contains(rendered, "db.example") {
		t.Errorf("provider config dropped classified host: %s", rendered)
	}
}

// credentialKeyNameSpellings is the table that stops the next round of
// missed spellings. Every entry is one way an operator, a provider, or a
// cloud SDK spells the same handful of credential terms; the classifier
// has to reach the same verdict for all of them, and a reviewer can read
// the term set in redact.go directly against this list.
var credentialKeyNameSpellings = map[string]bool{
	// camelCase
	"apiKey":          true,
	"apiToken":        true,
	"accessKeyId":     true,
	"secretAccessKey": true,
	"authToken":       true,
	"refreshToken":    true,
	"sessionToken":    true,
	"privateKey":      true,
	"clientSecret":    true,
	"sasToken":        true,
	"accountKey":      true,
	"signingKey":      true,
	"subscriptionKey": true,
	// snake_case
	"api_key":               true,
	"api_token":             true,
	"client_secret":         true,
	"access_key_id":         true,
	"aws_secret_access_key": true,
	"auth_token":            true,
	// SCREAMING_SNAKE_CASE
	"API_KEY":               true,
	"CLIENT_SECRET":         true,
	"AWS_SECRET_ACCESS_KEY": true,
	// kebab-case
	"api-key":       true,
	"x-api-key":     true,
	"client-secret": true,
	"access-key-id": true,
	// dotted
	"auth.token": true,
	// PascalCase and acronym runs
	"SharedAccessSignature": true,
	"APIKey":                true,
	"SASToken":              true,
	// run together, no separator at all
	"apikey":       true,
	"apitoken":     true,
	"accesskeyid":  true,
	"clientsecret": true,
	// bare terms
	"password":  true,
	"passwd":    true,
	"pwd":       true,
	"secret":    true,
	"token":     true,
	"sig":       true,
	"signature": true,
	// prefixed and suffixed around the credential term
	"dingoApiToken": true,
	"apiKeyValue":   true,
	// A key naming a location holds a path, which an operator needs.
	"tokenFilePath":  false,
	"tokenFile":      false,
	"secretFile":     false,
	"passwordFile":   false,
	"signingKeyFile": false,
	"keyFilePath":    false,
	"certFilePath":   false,
	"dataDir":        false,
	// A key word with no credential qualifier is not a credential.
	"publicKeys":    false,
	"shelleyVrfKey": false,
	"keyName":       false,
	// Benign names that embed a credential term as a substring must not
	// classify on it.
	"monkey":      false,
	"keyspace":    false,
	"passthrough": false,
	"bypass":      false,
	// Real provider keys that carry no secret.
	"host":             false,
	"port":             false,
	"user":             false,
	"database":         false,
	"sslMode":          false,
	"timeZone":         false,
	"dsn":              false,
	"endpoint":         false,
	"url":              false,
	"bucket":           false,
	"region":           false,
	"prefix":           false,
	"timeout":          false,
	"mode":             false,
	"maxConnections":   false,
	"poolMaxOpenConns": false,
	"capacity":         false,
	"network":          false,
	"":                 false,
}

// TestIsCredentialKeyName runs the credential classifier over every
// spelling in credentialKeyNameSpellings.
func TestIsCredentialKeyName(t *testing.T) {
	t.Parallel()

	for name, want := range credentialKeyNameSpellings {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := isCredentialKeyName(name); got != want {
				t.Errorf(
					"isCredentialKeyName(%q) = %t, want %t (words %q)",
					name,
					got,
					want,
					keyNameWords(name),
				)
			}
		})
	}
}

// TestKeyNameWords pins the decomposition the classifier decides on, so a
// failing classification can be read as either a wrong split or a wrong
// term set rather than one opaque verdict.
func TestKeyNameWords(t *testing.T) {
	t.Parallel()

	tests := map[string][]string{
		"accessKeyId":           {"access", "key", "id"},
		"access_key_id":         {"access", "key", "id"},
		"ACCESS-KEY-ID":         {"access", "key", "id"},
		"accesskeyid":           {"access", "key", "id"},
		"APIKey":                {"api", "key"},
		"IPFSGatewayURL":        {"ipfs", "gateway", "url"},
		"SharedAccessSignature": {"shared", "access", "signature"},
		"sha256Key":             {"sha256", "key"},
		"client_secret":         {"client", "secret"},
		"auth.token":            {"auth", "token"},
		// No vocabulary segmentation covers these, so they stay whole
		// instead of matching on an embedded term.
		"monkey":   {"monkey"},
		"keyspace": {"keyspace"},
		"sslmode":  {"sslmode"},
		"datadir":  {"datadir"},
	}
	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := keyNameWords(name); !slices.Equal(got, want) {
				t.Errorf("keyNameWords(%q) = %q, want %q", name, got, want)
			}
		})
	}
}

// TestProviderConfigKeyTableAgreesWithClassifier holds the hand-written
// provider key table and the credential classifier to one answer.
// providerConfigKeyClass consults the classifier first, so a key listed
// plain or URI that the classifier reads as a credential would be
// silently over-redacted at runtime, and a key listed secret that the
// classifier reads as benign would mean the term set has a hole.
func TestProviderConfigKeyTableAgreesWithClassifier(t *testing.T) {
	t.Parallel()

	for _, key := range slices.Concat(
		providerConfigPlainKeys,
		providerConfigURIKeys,
	) {
		if isCredentialKeyName(key) {
			t.Errorf(
				"provider key %q is classified renderable but reads as "+
					"a credential (words %q)",
				key,
				keyNameWords(key),
			)
		}
	}
	for _, key := range providerConfigSecretKeys {
		if !isCredentialKeyName(key) {
			t.Errorf(
				"provider key %q is classified secret but does not read "+
					"as a credential (words %q)",
				key,
				keyNameWords(key),
			)
		}
	}
}

// credentialShapedPlainConfigFields are the Config field paths that read
// as credential-shaped but are deliberately rendered. Both name an
// on-chain Midnight policy ID or asset name for an authorization token --
// public ledger data, not the token. The Config field paths are
// exhaustively enumerated and gated by
// TestConfigLogClassesCoverEveryConfigField, so that path keeps its
// explicit human decision instead of deferring to a name-shape heuristic;
// this list is what makes each such decision reviewable.
var credentialShapedPlainConfigFields = []string{
	"Midnight.AuthTokenAssetName",
	"Midnight.AuthTokenPolicyID",
}

// TestConfigFieldClassesAgreeWithClassifier runs the credential
// classifier over the leaf name of every classified Config field path. A
// new secret-bearing field whose name reads as a credential but that was
// classified plain fails here, which is the audit the provider-key path
// gets at runtime.
func TestConfigFieldClassesAgreeWithClassifier(t *testing.T) {
	t.Parallel()

	var exercised []string
	for _, path := range slices.Concat(
		logPlainConfigFields,
		logURIConfigFields,
		logProviderConfigFields,
	) {
		leaf := path
		if i := strings.LastIndex(path, "."); i >= 0 {
			leaf = path[i+1:]
		}
		if !isCredentialKeyName(leaf) {
			continue
		}
		if slices.Contains(credentialShapedPlainConfigFields, path) {
			exercised = append(exercised, path)
			continue
		}
		t.Errorf(
			"Config field %s is classified renderable but its name "+
				"reads as a credential (words %q): classify it as a "+
				"secret, or add it to "+
				"credentialShapedPlainConfigFields with a reason",
			path,
			keyNameWords(leaf),
		)
	}
	for _, path := range credentialShapedPlainConfigFields {
		if !slices.Contains(exercised, path) {
			t.Errorf(
				"credentialShapedPlainConfigFields lists %s, which is "+
					"no longer a credential-shaped renderable field",
				path,
			)
		}
	}
	for _, path := range logSecretConfigFields {
		leaf := path
		if i := strings.LastIndex(path, "."); i >= 0 {
			leaf = path[i+1:]
		}
		if !isCredentialKeyName(leaf) {
			t.Errorf(
				"Config field %s is classified secret but its name does "+
					"not read as a credential (words %q)",
				path,
				keyNameWords(leaf),
			)
		}
	}
}

// TestProviderConfigSecretSubtreeIsRedactedWhole pins that a
// secret-classified or unclassified provider key covers everything under
// it. Recursing into the subtree would reclassify its inner keys by their
// own names, and an inner key that happens to be classified plain --
// "host", "mode" -- would then render part of a secret-bearing value.
func TestProviderConfigSecretSubtreeIsRedactedWhole(t *testing.T) {
	t.Parallel()

	value := providerConfigValue(reflect.ValueOf(map[string]any{
		"host": "db.example",
		"token": map[string]any{
			"host": "nested-under-secret-key",
			"mode": "nested-under-secret-key-mode",
		},
		"clientSecret": map[string]any{
			"mode": "nested-under-credential-shaped-key",
		},
		"futureTree": map[string]any{
			"host": "nested-under-unclassified-key",
		},
	}))
	rendered := value.String()
	for _, unwanted := range []string{
		"nested-under-secret-key",
		"nested-under-secret-key-mode",
		"nested-under-credential-shaped-key",
		"nested-under-unclassified-key",
	} {
		if strings.Contains(rendered, unwanted) {
			t.Errorf("provider config leaks %q: %s", unwanted, rendered)
		}
	}
	if !strings.Contains(rendered, "db.example") {
		t.Errorf("provider config dropped classified host: %s", rendered)
	}
}

// TestProviderConfigNestedSectionsAreWalked is the counterweight to
// redacting a secret subtree whole: the API providers nest their tls and
// auth policies, so those sections have to stay renderable containers or
// the whole policy disappears from a startup log.
func TestProviderConfigNestedSectionsAreWalked(t *testing.T) {
	t.Parallel()

	value := providerConfigValue(reflect.ValueOf(map[string]any{
		"auth": map[string]any{
			"mode":          "token",
			"tokenFilePath": "/etc/dingo/token",
			"token":         "nested-auth-token",
		},
		"tls": map[string]any{
			"mode":         "manual",
			"certFilePath": "/etc/dingo/api.crt",
		},
	}))
	rendered := value.String()
	if strings.Contains(rendered, "nested-auth-token") {
		t.Errorf("provider config leaks nested auth token: %s", rendered)
	}
	for _, want := range []string{
		"token",
		"/etc/dingo/token",
		"manual",
		"/etc/dingo/api.crt",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf(
				"provider config dropped nested policy %q: %s",
				want,
				rendered,
			)
		}
	}
}
